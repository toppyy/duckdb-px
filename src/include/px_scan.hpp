#pragma once

#include "px_code_filter.hpp"
#include "px_reader.hpp"

#include "duckdb.hpp"

namespace duckdb {

//! The bind data of the read_px table function
struct PxBindData : FunctionData {

  string file;
  vector<string> names;
  vector<LogicalType> types;
  shared_ptr<PxReader> reader;
  //! Filter on the first variable that the optimizer pushed down into the scan
  PxCodeFilter code_filter;

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

//! The state that a single execution of the read_px scan shares between its
//! threads
struct PxGlobalState : GlobalTableFunctionState {
  mutex lock;

  shared_ptr<PxReader> reader;
  vector<column_t> column_ids;
  optional_ptr<TableFilterSet> filters;
  PxCodeFilter code_filter;
};

//! Neither table function takes a named parameter, reject every one of them
void CheckPxNamedParameters(TableFunctionBindInput &input);

unique_ptr<FunctionData> PxBindFunction(ClientContext &context,
                                        TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types,
                                        vector<string> &names);

unique_ptr<GlobalTableFunctionState>
PxGlobalInit(ClientContext &context, TableFunctionInitInput &input);

void PxTableFunction(ClientContext &context, TableFunctionInput &data,
                     DataChunk &output);

} // namespace duckdb
