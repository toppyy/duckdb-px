#define DUCKDB_EXTENSION_MAIN
#include "px_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <algorithm>
#include <mutex>

namespace duckdb {

struct PxReader;

//! A filter on the first variable that the optimizer has pushed down into the
//! scan. The filter is not stored as an expression: it is resolved into the
//! indexes of the CODES of the variable that can satisfy it. Those are the very
//! same indexes that the dictionary vectors of the reader use, so an
//! observation can be matched with a single integer comparison instead of a
//! string comparison.
//! The pushed down filter is only used to skip over the observations that can
//! not match. DuckDB still applies the filter to the rows that the scan
//! returns, so getting a filter wrong is always safe: it can only make the
//! scan read more rows than strictly necessary.
struct PxCodeFilter {

  //! Whether the optimizer pushed a filter on the first variable down at all
  bool active = false;
  //! Sorted, unique list of the code indexes of the first variable that can
  //! match the filter. Can be empty, then no observation matches the filter.
  vector<idx_t> code_indexes;

  bool Matches(idx_t code_index) const {
    return std::binary_search(code_indexes.begin(), code_indexes.end(),
                              code_index);
  }

  //! The observations of a code are stored as one block and the codes are read
  //! in the order of the CODES, so a code that the reader has moved past can
  //! never be seen again and does not have to be checked anymore.
  void RemovePassedCodes(idx_t code_index) {
    code_indexes.erase(code_indexes.begin(),
                       std::lower_bound(code_indexes.begin(),
                                        code_indexes.end(), code_index));
  }

  //! Returns false when no observation can match the filter anymore
  bool CanMatch() const { return !code_indexes.empty(); }

  //! Both filters are applied to an observation, so only the codes that both
  //! of them can match can be read.
  void Intersect(const PxCodeFilter &other) {
    vector<idx_t> intersection;
    std::set_intersection(code_indexes.begin(), code_indexes.end(),
                          other.code_indexes.begin(), other.code_indexes.end(),
                          std::back_inserter(intersection));
    code_indexes = std::move(intersection);
  }
};

struct PxUnionData {

  string file_name;
  vector<string> names;
  vector<LogicalType> types;
  unique_ptr<PxReader> reader;

  const string &GetFileName() { return file_name; }
};

struct PxReader {

  using UNION_READER_DATA = unique_ptr<PxUnionData>;

  AllocatedData allocated_data;
  LogicalType duckdb_type;
  vector<LogicalType> return_types;
  vector<unique_ptr<Vector>> read_vecs;
  vector<string> names;
  string filename;
  PxFile pxfile;
  size_t data_offset;
  size_t data_size;
  size_t observations_read;
  const char *data;
  std::mutex read_lock;

  std::string value_type;

  StringView GetNextValue() {
    if (data_offset >= data_size) {
      return StringView(nullptr, 0);
    }
    data_offset = SkipWhiteSpace(data, data_offset, data_size);
    if (data_offset >= data_size) {
      return StringView(nullptr, 0);
    }
    if (data[data_offset] == ';') {
      data_offset++;
      data_offset = SkipWhiteSpace(data, data_offset, data_size);
      return StringView(nullptr, 0);
    }
    size_t start = data_offset;
    while (data_offset < data_size && !IsWhiteSpace(data[data_offset]) &&
           data[data_offset] != ';') {
      data_offset++;
    }
    StringView rtrn(data + start, data_offset - start);
    data_offset = SkipWhiteSpace(data, data_offset, data_size);
    if (data_offset < data_size && data[data_offset] == ';') {
      data_offset++;
    }
    return rtrn;
  }

  void AssignValue(size_t variable, size_t out_idx, StringView val) {
    if (value_type == "float") {
      AssignFloatValue(variable, out_idx, val);
      return;
    }

    AssignIntegerValue(variable, out_idx, val);
  }

  void AssignFloatValue(size_t variable, size_t out_idx, StringView val) {
    float fval = ParseFloat(val);
    FlatVector::GetData<float>(*read_vecs[variable])[out_idx] = fval;
  }

  void AssignIntegerValue(size_t variable, size_t out_idx, StringView val) {
    int32_t ival = ParseInt32(val);
    FlatVector::GetData<int32_t>(*read_vecs[variable])[out_idx] = ival;
  }

  //! Move the reader to the next observation without materializing it
  void SkipObservation() {
    // The value of an observation that is skipped is never needed
    GetNextValue();
    for (size_t col_idx = 0; col_idx < pxfile.variable_count; col_idx++) {
      pxfile.GetVariable(col_idx).NextCodeIndexSequential();
    }
    observations_read++;
  }

  void Read(DataChunk &output, const PxCodeFilter &code_filter) {
    std::lock_guard<std::mutex> guard(read_lock);
    if (observations_read >= pxfile.observations) {
      return;
    }

    // Reset sequential counters on first read
    if (observations_read == 0) {
      for (size_t i = 0; i < pxfile.variable_count; i++) {
        pxfile.GetVariable(i).ResetSequentialCounter();
      }
    }

    // A filter that is pushed down is only used to skip over the observations
    // that can not match it. The codes that the reader moves past are dropped
    // from it while scanning, the pushed down filter of the query itself is
    // left alone.
    PxCodeFilter remaining = code_filter;
    idx_t current_code =
        remaining.active ? pxfile.GetVariable(0).GetCurrentCodeIndex() : 0;

    // There are actually variables + 1 vectors in the output
    // pxfile.variable_count only counts for variables excl. "value"
    // which is always present
    column_t variables = pxfile.variable_count;
    idx_t out_idx = 0;

    while (observations_read < pxfile.observations) {

      if (remaining.active) {
        auto code_index = pxfile.GetVariable(0).GetCurrentCodeIndex();
        if (code_index != current_code) {
          // The reader has moved on to the next code of the first variable
          remaining.RemovePassedCodes(code_index);
          current_code = code_index;
        }
        if (!remaining.CanMatch()) {
          // Every code that could have matched the filter has been read, the
          // rest of the file can not contain a match anymore
          observations_read = pxfile.observations;
          break;
        }
        if (!remaining.Matches(code_index)) {
          // The observations of the first variable are stored as one block of
          // observations per code, so the whole block is skipped instead of
          // being materialized and thrown away by DuckDB afterwards.
          SkipObservation();
          continue;
        }
      }

      for (size_t col_idx = 0; col_idx <= variables; col_idx++) {

        if (col_idx == variables) {
          StringView val = GetNextValue();
          if (!IsNumeric(val)) {
            FlatVector::Validity(*read_vecs[variables]).SetInvalid(out_idx);
            continue;
          };
          FlatVector::Validity(*read_vecs[variables]).SetValid(out_idx);
          AssignValue(variables, out_idx, val);
          continue;
        }

        size_t selectedIndex =
            pxfile.GetVariable(col_idx).NextCodeIndexSequential();

        auto &sel_vector = DictionaryVector::SelVector(*read_vecs[col_idx]);
        sel_vector[out_idx] = selectedIndex;
      }

      out_idx++;
      observations_read++;
      if (out_idx == STANDARD_VECTOR_SIZE) {
        break;
      }
    }

    for (size_t i = 0; i <= variables; i++) {
      output.data[i].Reference(*read_vecs[i]);
    }

    output.SetCardinality(out_idx);
  }

  const string &GetFileName() { return filename; }

  const vector<string> &GetNames() { return names; }

  const vector<LogicalType> &GetTypes() { return return_types; }

  PxReader(ClientContext &context, const string filename_p)
      : pxfile(), data_offset(0), data_size(0), data(nullptr), read_vecs(),
        return_types(), names(), observations_read(0), value_type("float") {
    filename = filename_p;
    auto &fs = FileSystem::GetFileSystem(context);
    if (!fs.FileExists(filename)) {
      throw InvalidInputException("PX-file %s not found", filename);
    }

    auto file = fs.OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
    auto fsize = file->GetFileSize();
    if (fsize == 0) {
      throw BinderException("PX-file %s is empty", filename);
    }
    try {
      allocated_data = Allocator::Get(context).Allocate(fsize);
    } catch (const Exception &ex) {
      throw BinderException(
          "Failed to allocate memory for PX-file %s (%llu bytes): %s", filename,
          (unsigned long long)fsize, ex.what());
    }
    idx_t n_read = 0;
    try {
      n_read = file->Read(allocated_data.get(), allocated_data.GetSize());
    } catch (const Exception &ex) {
      throw InvalidInputException("Failed to read PX-file %s: %s", filename,
                                  ex.what());
    }
    if (n_read != (idx_t)fsize) {
      throw InvalidInputException(
          "Failed to read PX-file %s (read %llu of %llu bytes)", filename,
          (unsigned long long)n_read, (unsigned long long)fsize);
    }

    /* Parse column types */
    data_size = fsize;
    data = const_char_ptr_cast(allocated_data.get());

    data_offset = pxfile.ParseMetadata(data, data_offset, data_size);

    int decimals = pxfile.GetDecimals();

    // Get variable metadata from parsed px-file

    for (size_t i = 0; i < pxfile.variable_count; i++) {

      Variable &var = pxfile.GetVariable(i);

      if (var.ValueCount() != 0 && var.CodeCount() != var.ValueCount()) {
        throw BinderException(
            "Number of VALUES and CODES do not match for variable '%s'!",
            var.GetName().c_str());
      }
      if (var.CodeCount() == 0) {
        throw BinderException("Variable '%s' has no CODES",
                              var.GetName().c_str());
      }
      if (var.CodeCount() > STANDARD_VECTOR_SIZE) {
        throw BinderException("Variable '%s' has too many codes %zu > %d",
                              var.GetName().c_str(), var.CodeCount(),
                              STANDARD_VECTOR_SIZE);
      }

      read_vecs.push_back(make_uniq<Vector>(LogicalType::VARCHAR));

      // Build the dictionary
      size_t idx = read_vecs.size() - 1;
      size_t out_idx = 0;
      for (auto &code : var.GetCodes()) {
        FlatVector::GetData<string_t>(*read_vecs[idx])[out_idx] =
            StringVector::AddString(*read_vecs[idx], code);
        out_idx++;
      }

      // Turn it into a dictionary vectory
      SelectionVector sel_vect;
      sel_vect.Initialize(STANDARD_VECTOR_SIZE);
      read_vecs[idx]->Dictionary(var.CodeCount(), sel_vect,
                                 STANDARD_VECTOR_SIZE);

      D_ASSERT(read_vecs[idx]->GetVectorType() ==
               VectorType::DICTIONARY_VECTOR);

      return_types.push_back(LogicalType::VARCHAR);
      names.push_back(var.GetName());
    }

    if (pxfile.variable_count > 0) {
      size_t repetition_factor = 1;
      for (size_t i = pxfile.variable_count; i-- > 0;) {
        auto &var = pxfile.GetVariable(i);
        var.SetRepetitionFactor(repetition_factor);
        if (var.CodeCount() == 0) {
          throw BinderException("Variable '%s' has zero codes",
                                var.GetName().c_str());
        }
        if (repetition_factor > SIZE_MAX / var.CodeCount()) {
          throw BinderException("Too many observations, product overflow");
        }
        repetition_factor *= var.CodeCount();
      }
    }

    // Variable(s) for values
    names.push_back("value");
    if (decimals > 0) {
      value_type = "float";
      read_vecs.push_back(make_uniq<Vector>(LogicalType::FLOAT));
      return_types.push_back(LogicalType::FLOAT);
      return;
    }

    value_type = "int";
    read_vecs.push_back(make_uniq<Vector>(LogicalType::INTEGER));
    return_types.push_back(LogicalType::INTEGER);
  };
};

struct PxBindData : FunctionData {

  string file;
  vector<string> names;
  vector<LogicalType> types;
  shared_ptr<PxReader> reader;
  //! Filter on the first variable that the optimizer pushed down into the scan
  PxCodeFilter code_filter;

  void Initialize(shared_ptr<PxReader> p_reader) {
    reader = std::move(p_reader);
  }

  void Initialize(ClientContext &, shared_ptr<PxReader> reader) {
    Initialize(reader);
  }

  bool Equals(const FunctionData &other_p) const override {
    D_ASSERT(false);
    auto &other = other_p.Cast<PxBindData>();
    return reader == other.reader && file == other.file;
  }

  unique_ptr<FunctionData> Copy() const override {
    D_ASSERT(false);
    auto copy = make_uniq<PxBindData>();
    copy->file = file;
    copy->names = names;
    copy->types = types;
    copy->reader = reader;
    copy->code_filter = code_filter;
    return std::move(copy);
  }
};

static unique_ptr<FunctionData>
PxBindFunction(ClientContext &context, TableFunctionBindInput &input,
               vector<LogicalType> &return_types, vector<string> &names) {
  auto &filename = input.inputs[0];
  auto result = make_uniq<PxBindData>();

  for (auto &kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    auto loption = StringUtil::Lower(kv.first);
    throw InternalException("Unrecognized option %s", loption.c_str());
  }

  result->reader = make_shared_ptr<PxReader>(context, filename.ToString());

  return_types = result->reader->return_types;
  names = result->reader->names;

  result->types = return_types;
  result->names = names;

  return std::move(result);
};

struct PxGlobalState : GlobalTableFunctionState {
  mutex lock;

  shared_ptr<PxReader> reader;
  vector<column_t> column_ids;
  optional_ptr<TableFilterSet> filters;
  PxCodeFilter code_filter;
};

static void PxTableFunction(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<PxBindData>();
  auto &global_state = data.global_state->Cast<PxGlobalState>();

  do {
    output.Reset();
    global_state.reader->Read(output, global_state.code_filter);

    if (output.size() > 0) {
      return;
    }

    break;

  } while (true);
};

unique_ptr<GlobalTableFunctionState>
PxGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  auto global_state_result = make_uniq<PxGlobalState>();
  auto &global_state = *global_state_result;
  auto &bind_data = input.bind_data->Cast<PxBindData>();

  global_state.column_ids = input.column_ids;
  global_state.filters = input.filters;
  global_state.code_filter = bind_data.code_filter;

  D_ASSERT(bind_data.reader != NULL);
  global_state.reader = bind_data.reader;

  return std::move(global_state_result);
};

//! The set of values that a filter restricts a single column of the scan to
struct PxFilterValues {
  //! Index of the column of the scan that is restricted
  idx_t column_index = 0;
  //! The values that the column is restricted to
  vector<Value> values;
};

//! Resolve a column reference of a filter into the index of the column of the
//! scan. Returns false when the expression does not reference a column of this
//! scan at all.
static bool TryGetScanColumn(LogicalGet &get, Expression &expr,
                             idx_t &column_index) {
  if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  auto &ref = expr.Cast<BoundColumnRefExpression>();
  if (ref.depth != 0 || ref.binding.table_index != get.table_index) {
    return false;
  }
  auto &column_ids = get.GetColumnIds();
  if (ref.binding.column_index >= column_ids.size()) {
    return false;
  }
  auto &column_id = column_ids[ref.binding.column_index];
  if (!column_id.HasPrimaryIndex() || column_id.IsVirtualColumn()) {
    return false;
  }
  column_index = column_id.GetPrimaryIndex();
  return true;
}

//! Try to extract the values that a filter restricts a column of the scan to.
//! Only expressions of the form
//!   <column> = <constant>
//!   <column> IN (<constant>, ...)
//!   <column> = <constant> OR <column> = <constant> ...
//! are understood. Returns false when nothing can be derived from the
//! expression, in which case the expression does not restrict the scan at all.
static bool TryExtractFilterValues(LogicalGet &get, Expression &expr,
                                   PxFilterValues &result) {
  switch (expr.GetExpressionClass()) {
  case ExpressionClass::BOUND_COMPARISON: {
    auto &comparison = expr.Cast<BoundComparisonExpression>();
    if (comparison.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
      return false;
    }
    Expression *column = nullptr;
    Expression *constant = nullptr;
    if (comparison.left->GetExpressionClass() ==
        ExpressionClass::BOUND_COLUMN_REF) {
      column = comparison.left.get();
      constant = comparison.right.get();
    } else if (comparison.right->GetExpressionClass() ==
               ExpressionClass::BOUND_COLUMN_REF) {
      column = comparison.right.get();
      constant = comparison.left.get();
    }
    if (!column || constant->GetExpressionClass() !=
                      ExpressionClass::BOUND_CONSTANT) {
      return false;
    }
    if (!TryGetScanColumn(get, *column, result.column_index)) {
      return false;
    }
    result.values.push_back(constant->Cast<BoundConstantExpression>().value);
    return true;
  }
  case ExpressionClass::BOUND_OPERATOR: {
    auto &in_operator = expr.Cast<BoundOperatorExpression>();
    if (in_operator.GetExpressionType() != ExpressionType::COMPARE_IN ||
        in_operator.children.size() < 2) {
      return false;
    }
    if (!TryGetScanColumn(get, *in_operator.children[0], result.column_index)) {
      return false;
    }
    for (idx_t i = 1; i < in_operator.children.size(); i++) {
      auto &child = *in_operator.children[i];
      if (child.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        return false;
      }
      result.values.push_back(child.Cast<BoundConstantExpression>().value);
    }
    return true;
  }
  case ExpressionClass::BOUND_CONJUNCTION: {
    auto &conjunction = expr.Cast<BoundConjunctionExpression>();
    if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
      return false;
    }
    PxFilterValues left;
    PxFilterValues right;
    if (!TryExtractFilterValues(get, *conjunction.children[0], left) ||
        !TryExtractFilterValues(get, *conjunction.children[1], right) ||
        left.column_index != right.column_index) {
      return false;
    }
    // An observation matches the disjunction if it matches either side, so the
    // codes of both sides can match
    result = std::move(left);
    result.values.insert(result.values.end(), right.values.begin(),
                         right.values.end());
    return true;
  }
  default:
    return false;
  }
}

//! Convert a constant of a filter into the string that is compared against the
//! CODES of a variable
static bool TryGetConstantString(ClientContext &context, const Value &value,
                                 string &result) {
  if (value.IsNull()) {
    return false;
  }
  if (value.type() == LogicalType::VARCHAR) {
    result = StringValue::Get(value);
    return true;
  }
  Value casted;
  if (!value.TryCastAs(context, LogicalType::VARCHAR, casted, nullptr)) {
    return false;
  }
  result = StringValue::Get(casted);
  return true;
}

//! Resolve the indexes of the CODES of a variable that can match the values
static vector<idx_t> ResolveCodeIndexes(ClientContext &context,
                                        Variable &variable,
                                        const vector<Value> &values) {
  auto &codes = variable.GetCodes();
  vector<idx_t> code_indexes;
  for (auto &value : values) {
    string code;
    if (!TryGetConstantString(context, value, code)) {
      continue;
    }
    for (idx_t code_idx = 0; code_idx < codes.size(); code_idx++) {
      if (codes[code_idx] == code) {
        code_indexes.push_back(code_idx);
      }
    }
  }
  std::sort(code_indexes.begin(), code_indexes.end());
  code_indexes.erase(std::unique(code_indexes.begin(), code_indexes.end()),
                     code_indexes.end());
  return code_indexes;
}

string PxDescribePushdown(const PxFilterValues &filters) {

  string columns = to_string(filters.column_index);
  string codes;

  for (idx_t i = 0; i < filters.values.size(); i++) {
    if (i > 0) {
      codes += ", ";
    }
    if (filters.values[i].type() == LogicalType::VARCHAR) {
      codes += StringValue::Get(filters.values[i]);
    }
  }

  return StringUtil::Format("filtered values of column(s) %s: %s",
                            columns.c_str(), codes.c_str());
}



//! Called by the optimizer to let the scan look at the filters that are pushed
//! into it. The filters are left in place: DuckDB applies them to the rows that
//! the scan returns anyway, so all that is gained here is that the scan does
//! not have to materialize the observations that can not match.
static void PxPushdownComplexFilter(ClientContext &context, LogicalGet &get,
                                    FunctionData *bind_data_p,
                                    vector<unique_ptr<Expression>> &filters) {
  auto &bind_data = bind_data_p->Cast<PxBindData>();
  auto &pxfile = bind_data.reader->pxfile;
  if (pxfile.variable_count == 0) {
    return;
  }

  for (auto &filter : filters) {
    PxFilterValues filter_values;
    if (!TryExtractFilterValues(get, *filter, filter_values)) {
      continue;
    }
    // The observations of the first variable are stored as one block of
    // observations per code, which makes it the cheapest variable to skip
    // over. Any other column, the "value" column included, is left alone.
    // TODO: resolve and push down filters for the other variables as well
    if (filter_values.column_index != 0) {
      continue;
    }
    PxCodeFilter code_filter;
    code_filter.active = true;
    code_filter.code_indexes = ResolveCodeIndexes(
        context, pxfile.GetVariable(0), filter_values.values);
    if (bind_data.code_filter.active) {
      // Every filter of the query is applied to an observation, so when the
      // first variable has more than one filter, only the codes that all of
      // them can match are read
      bind_data.code_filter.Intersect(code_filter);
    } else {
      bind_data.code_filter = std::move(code_filter);
    }

    get.extra_info.file_filters =
        PxDescribePushdown(filter_values);
  }

}

struct PxMetadataEntry {
  string variable;
  string code;
  string value;
  bool has_value;
  idx_t position;
};

struct PxMetadataBindData : FunctionData {
  string file;
  vector<PxMetadataEntry> entries;
  vector<string> names;
  vector<LogicalType> types;

  bool Equals(const FunctionData &other_p) const override {
    auto &other = other_p.Cast<PxMetadataBindData>();
    return file == other.file;
  }
  unique_ptr<FunctionData> Copy() const override {
    auto copy = make_uniq<PxMetadataBindData>();
    copy->file = file;
    copy->entries = entries;
    copy->names = names;
    copy->types = types;
    return std::move(copy);
  }
};

struct PxMetadataGlobalState : GlobalTableFunctionState {
  idx_t offset = 0;
};

static unique_ptr<FunctionData>
PxMetadataBindFunction(ClientContext &context, TableFunctionBindInput &input,
                       vector<LogicalType> &return_types,
                       vector<string> &names) {
  auto &filename = input.inputs[0];
  if (filename.IsNull()) {
    throw BinderException("Cannot use NULL as file name for read_px_metadata");
  }
  for (auto &kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    auto loption = StringUtil::Lower(kv.first);
    throw InternalException("Unrecognized option %s", loption.c_str());
  }

  string fname = filename.ToString();
  auto &fs = FileSystem::GetFileSystem(context);
  if (!fs.FileExists(fname)) {
    throw InvalidInputException("PX-file %s not found", fname);
  }
  auto file = fs.OpenFile(fname, FileOpenFlags::FILE_FLAGS_READ);
  auto fsize = file->GetFileSize();
  if (fsize == 0) {
    throw BinderException("PX-file %s is empty", fname);
  }
  AllocatedData allocated_data;
  try {
    allocated_data = Allocator::Get(context).Allocate(fsize);
  } catch (const Exception &ex) {
    throw BinderException(
        "Failed to allocate memory for PX-file %s (%llu bytes): %s", fname,
        (unsigned long long)fsize, ex.what());
  }
  idx_t n_read = 0;
  try {
    n_read = file->Read(allocated_data.get(), allocated_data.GetSize());
  } catch (const Exception &ex) {
    throw InvalidInputException("Failed to read PX-file %s: %s", fname,
                                ex.what());
  }
  if (n_read != (idx_t)fsize) {
    throw InvalidInputException(
        "Failed to read PX-file %s (read %llu of %llu bytes)", fname,
        (unsigned long long)n_read, (unsigned long long)fsize);
  }
  const char *data = const_char_ptr_cast(allocated_data.get());
  PxFile pxfile;
  pxfile.ParseMetadata(data, 0, fsize);

  auto result = make_uniq<PxMetadataBindData>();
  result->file = fname;

  for (size_t i = 0; i < pxfile.variable_count; i++) {
    Variable &var = pxfile.GetVariable(i);
    size_t cc = var.CodeCount();
    size_t vc = var.ValueCount();
    for (size_t j = 0; j < cc; j++) {
      PxMetadataEntry e;
      e.variable = var.GetName();
      e.code = var.GetCodes()[j];
      e.position = j;
      if (j < vc) {
        e.value = var.GetValues()[j];
        e.has_value = true;
      } else {
        e.has_value = false;
      }
      result->entries.push_back(std::move(e));
    }
  }

  // Define output schema: variable, code, value, code_index
  names = {"variable", "code", "value"};
  return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                  LogicalType::VARCHAR};
  result->names = names;
  result->types = return_types;
  return std::move(result);
}

static unique_ptr<GlobalTableFunctionState>
PxMetadataGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  auto gstate = make_uniq<PxMetadataGlobalState>();
  gstate->offset = 0;
  return std::move(gstate);
}

static void PxMetadataFunction(ClientContext &context, TableFunctionInput &data,
                               DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<PxMetadataBindData>();
  auto &gstate = data.global_state->Cast<PxMetadataGlobalState>();

  if (gstate.offset >= bind_data.entries.size()) {
    output.SetCardinality(0);
    return;
  }
  idx_t remaining = bind_data.entries.size() - gstate.offset;
  idx_t count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
  for (idx_t i = 0; i < count; i++) {
    auto &e = bind_data.entries[gstate.offset + i];
    FlatVector::GetData<string_t>(output.data[0])[i] =
        StringVector::AddString(output.data[0], e.variable);
    FlatVector::GetData<string_t>(output.data[1])[i] =
        StringVector::AddString(output.data[1], e.code);
    if (e.has_value) {
      FlatVector::GetData<string_t>(output.data[2])[i] =
          StringVector::AddString(output.data[2], e.value);
      FlatVector::Validity(output.data[2]).SetValid(i);
    } else {
      FlatVector::Validity(output.data[2]).SetInvalid(i);
    }
  }
  output.SetCardinality(count);
  gstate.offset += count;
}

static void LoadInternal(ExtensionLoader &loader) {

  // Register table function
  TableFunction px_table_function("read_px", {LogicalType::VARCHAR},
                                  PxTableFunction, PxBindFunction,
                                  PxGlobalInit);
  // Filter pushdown is not declared as supported: the scan only uses the
  // filters to skip over observations that can not match, so DuckDB keeps
  // applying the filters to the rows that the scan returns
  px_table_function.pushdown_complex_filter = PxPushdownComplexFilter;
  CreateTableFunctionInfo info(px_table_function);
  FunctionDescription desc;
  desc.parameter_names = {"file"};
  desc.description =
      "Reads a PX (PC-Axis) file and returns its contents as a table with "
      "STUB/HEADING variables as VARCHAR columns and a 'value' column (INTEGER "
      "if DECIMALS=0, otherwise FLOAT).";
  desc.examples = {
      "SELECT * FROM read_px('test/data/statfin_vaerak_pxt_11rc.px');"};
  desc.categories = {"Scan"};
  info.descriptions.push_back(std::move(desc));
  info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));

  TableFunction px_metadata_function("read_px_metadata", {LogicalType::VARCHAR},
                                     PxMetadataFunction, PxMetadataBindFunction,
                                     PxMetadataGlobalInit);
  CreateTableFunctionInfo meta_info(px_metadata_function);
  FunctionDescription meta_desc;
  meta_desc.parameter_names = {"file"};
  meta_desc.description =
      "Reads metadata from a PX (PC-Axis) file and returns the available "
      "CODE-VALUE pairs per variable without scanning the DATA section. "
      "Returns columns: 'variable' (VARCHAR), 'code' (VARCHAR), 'value' "
      "(VARCHAR, NULL if no VALUES entry exists).";
  meta_desc.examples = {
      "SELECT * FROM read_px_metadata('test/data/statfin_vaerak_pxt_11rc.px');",
      "SELECT * FROM read_px_metadata('test/data/statfin_vaerak_pxt_11rc.px') "
      "WHERE variable='Sukupuoli';"};
  meta_desc.categories = {"Scan"};
  meta_info.descriptions.push_back(std::move(meta_desc));
  meta_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  loader.RegisterFunction(std::move(meta_info));
};

void PxExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }
std::string PxExtension::Name() { return "px"; }

std::string PxExtension::Version() const {
#ifdef EXT_VERSION_PX
  return EXT_VERSION_PX;
#else
  return "";
#endif
}

}; // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(px, loader) { duckdb::LoadInternal(loader); }
}
