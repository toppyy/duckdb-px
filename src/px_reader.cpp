#include "px_reader.hpp"

#include "px_file_source.hpp"

namespace duckdb {

StringView PxReader::GetNextValue() {
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

void PxReader::AssignValue(size_t variable, size_t out_idx, StringView val) {
  if (value_type == PxValueType::FLOAT) {
    AssignFloatValue(variable, out_idx, val);
    return;
  }

  AssignIntegerValue(variable, out_idx, val);
}

void PxReader::AssignFloatValue(size_t variable, size_t out_idx,
                                StringView val) {
  float fval = ParseFloat(val);
  FlatVector::GetData<float>(*read_vecs[variable])[out_idx] = fval;
}

void PxReader::AssignIntegerValue(size_t variable, size_t out_idx,
                                  StringView val) {
  int32_t ival = ParseInt32(val);
  FlatVector::GetData<int32_t>(*read_vecs[variable])[out_idx] = ival;
}

//! Move the reader past the value of the next observation without looking at
//! it. A value is a token of the DATA section that is separated from the
//! others by white space, a semicolon instead stands for an observation
//! without a value. Returns false when the DATA section holds no more values,
//! which happens for a file that is cut short.
bool PxReader::SkipValue() {
  data_offset = SkipWhiteSpace(data, data_offset, data_size);
  if (data_offset >= data_size) {
    return false;
  }
  if (data[data_offset] == ';') {
    data_offset++;
    return true;
  }
  while (data_offset < data_size && !IsWhiteSpace(data[data_offset]) &&
         data[data_offset] != ';') {
    data_offset++;
  }
  if (data_offset < data_size && data[data_offset] == ';') {
    data_offset++;
  }
  return true;
}

void PxReader::SkipObservations(size_t n) {
  // The observations of the file can not be read past its end
  n = MinValue(n, pxfile.observations - observations_read);
  if (n == 0) {
    return;
  }

  // The values of the skipped observations are never needed, so the DATA
  // section is scanned over without parsing the tokens that it holds
  for (size_t i = 0; i < n; i++) {
    if (!SkipValue()) {
      // The file holds fewer values than it has combinations of codes, the
      // codes still move on so that the rows after it stay correct
      break;
    }
  }

  // The observations of the file are the cartesian product of the codes of its
  // variables, so where every variable ends up after n observations follows
  // from n alone
  for (size_t col_idx = 0; col_idx < pxfile.variable_count; col_idx++) {
    pxfile.GetVariable(col_idx).SkipSequential(n);
  }
  observations_read += n;
}

void PxReader::Read(DataChunk &output, const PxCodeFilter &code_filter) {
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

  // There are actually variables + 1 vectors in the output
  // pxfile.variable_count only counts for variables excl. "value"
  // which is always present
  column_t variables = pxfile.variable_count;
  idx_t out_idx = 0;

  while (observations_read < pxfile.observations) {

    if (remaining.active) {

      if (!remaining.CanMatch()) {
        // Every code that could have matched the filter has been read, the
        // rest of the file can not contain a match anymore
        observations_read = pxfile.observations;
        break;
      }

      // The observations of a code of the first variable are stored as one
      // block, so the reader can be moved to the next block that can match
      // the filter in a single step instead of reading the blocks in between
      // and throwing them away by DuckDB afterwards
      size_t block_offset = remaining.BlockOffset();
      size_t block_end = block_offset + remaining.block_size;

      if (observations_read < block_offset) {
        SkipObservations(block_offset - observations_read);
        continue;
      }

      if (observations_read >= block_end) {
        // The block that the reader was in has been read in its entirety,
        // its code can not be read again
        remaining.RemovePassedCodes(remaining.code_indexes[0]);
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

PxReader::PxReader(ClientContext &context, const string filename)
    : pxfile(), data_offset(0), data_size(0), data(nullptr), read_vecs(),
      return_types(), names(), observations_read(0),
      value_type(PxValueType::FLOAT) {
  auto source = ReadPxFile(context, filename);
  allocated_data = std::move(source.allocated_data);
  data = const_char_ptr_cast(allocated_data.get());
  data_size = source.size;

  /* Parse column types */
  data_offset = pxfile.ParseMetadata(data, data_offset, data_size);

  // Get variable metadata from parsed px-file
  for (size_t i = 0; i < pxfile.variable_count; i++) {
    AddVariableColumn(pxfile.GetVariable(i));
  }

  SetRepetitionFactors();

  // Variable(s) for values
  AddValueColumn(pxfile.GetDecimals());
}

void PxReader::AddVariableColumn(Variable &var) {
  if (var.ValueCount() != 0 && var.CodeCount() != var.ValueCount()) {
    throw BinderException(
        "Number of VALUES and CODES do not match for variable '%s'!",
        var.GetName().c_str());
  }
  if (var.CodeCount() == 0) {
    throw BinderException("Variable '%s' has no CODES", var.GetName().c_str());
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

  // Turn it into a dictionary vectory. The selection vector is zeroed so that
  // every entry points at a code that was just written, even the entries that
  // the reader has not filled in yet.
  SelectionVector sel_vect;
  sel_vect.Initialize(STANDARD_VECTOR_SIZE);
  for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
    sel_vect.set_index(i, 0);
  }
  read_vecs[idx]->Dictionary(var.CodeCount(), sel_vect, STANDARD_VECTOR_SIZE);

  D_ASSERT(read_vecs[idx]->GetVectorType() == VectorType::DICTIONARY_VECTOR);

  return_types.push_back(LogicalType::VARCHAR);
  names.push_back(var.GetName());
}

//! Every variable is repeated once for every combination of the codes of the
//! variables that follow it, so the last variable changes fastest. The factor
//! of a variable is the product of the code counts of the variables behind it.
void PxReader::SetRepetitionFactors() {
  if (pxfile.variable_count == 0) {
    return;
  }
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

void PxReader::AddValueColumn(int decimals) {
  names.push_back("value");
  if (decimals > 0) {
    value_type = PxValueType::FLOAT;
    read_vecs.push_back(make_uniq<Vector>(LogicalType::FLOAT));
    return_types.push_back(LogicalType::FLOAT);
    return;
  }

  value_type = PxValueType::INTEGER;
  read_vecs.push_back(make_uniq<Vector>(LogicalType::INTEGER));
  return_types.push_back(LogicalType::INTEGER);
}

} // namespace duckdb
