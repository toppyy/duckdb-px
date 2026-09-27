#include "px_metadata.hpp"

#include "px_file.hpp"
#include "px_file_source.hpp"
#include "px_scan.hpp"

namespace duckdb {

unique_ptr<FunctionData>
PxMetadataBindFunction(ClientContext &context, TableFunctionBindInput &input,
                       vector<LogicalType> &return_types,
                       vector<string> &names) {
  auto &filename = input.inputs[0];
  if (filename.IsNull()) {
    throw BinderException("Cannot use NULL as file name for read_px_metadata");
  }
  CheckPxNamedParameters(input);

  string fname = filename.ToString();
  auto source = ReadPxFile(context, fname);
  PxFile pxfile;
  pxfile.ParseMetadata(source.data, 0, source.size);

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
      if (j < vc) {
        e.value = var.GetValues()[j];
        e.has_value = true;
      } else {
        e.has_value = false;
      }
      result->entries.push_back(std::move(e));
    }
  }

  // Define output schema: variable, code, value
  names = {"variable", "code", "value"};
  return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                  LogicalType::VARCHAR};
  result->names = names;
  result->types = return_types;
  return std::move(result);
}

unique_ptr<GlobalTableFunctionState>
PxMetadataGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  auto gstate = make_uniq<PxMetadataGlobalState>();
  gstate->offset = 0;
  return std::move(gstate);
}

void PxMetadataFunction(ClientContext &context, TableFunctionInput &data,
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

} // namespace duckdb
