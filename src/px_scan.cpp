#include "px_scan.hpp"

namespace duckdb {

//! Neither table function takes a named parameter, reject every one of them
void CheckPxNamedParameters(TableFunctionBindInput &input) {
  for (auto &kv : input.named_parameters) {
    if (kv.second.IsNull()) {
      throw BinderException("Cannot use NULL as function argument");
    }
    auto loption = StringUtil::Lower(kv.first);
    throw InternalException("Unrecognized option %s", loption.c_str());
  }
}

unique_ptr<FunctionData> PxBindFunction(ClientContext &context,
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types,
                                        vector<string> &names) {
  auto &filename = input.inputs[0];
  auto result = make_uniq<PxBindData>();

  CheckPxNamedParameters(input);

  if (filename.IsNull()) {
    throw BinderException("Cannot use NULL as file name for read_px");
  }
  result->reader = make_shared_ptr<PxReader>(context, filename.ToString());

  return_types = result->reader->return_types;
  names = result->reader->names;

  result->types = return_types;
  result->names = names;

  return std::move(result);
};

void PxTableFunction(ClientContext &context, TableFunctionInput &data,
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

} // namespace duckdb
