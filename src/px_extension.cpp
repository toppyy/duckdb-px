#define DUCKDB_EXTENSION_MAIN
#include "px_extension.hpp"
#include <mutex>

namespace duckdb {

struct PxReader;

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

  void Read(DataChunk &output, const vector<column_t> &column_ids) {
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

    // There are actually variables + 1 vectors in the output
    // pxfile.variable_count only counts for variables excl. "value"
    // which is always present
    column_t variables = pxfile.variable_count;
    idx_t out_idx = 0;

    while (true) {
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
      if (observations_read >= pxfile.observations) {
        break;
      }

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
};

static void PxTableFunction(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<PxBindData>();
  auto &global_state = data.global_state->Cast<PxGlobalState>();

  do {
    output.Reset();
    global_state.reader->Read(output, global_state.column_ids);

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

  D_ASSERT(bind_data.reader != NULL);
  global_state.reader = bind_data.reader;

  return std::move(global_state_result);
};

static void LoadInternal(ExtensionLoader &loader) {

  // Register table function
  auto px_table_function =
      TableFunction("read_px", {LogicalType::VARCHAR}, PxTableFunction,
                    PxBindFunction, PxGlobalInit);

  loader.RegisterFunction(px_table_function);
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
