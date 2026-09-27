#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! A single CODE of a variable, together with the VALUES entry that belongs to
//! it
struct PxMetadataEntry {
  string variable;
  string code;
  string value;
  bool has_value;
  idx_t position;
};

//! The bind data of the read_px_metadata table function
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

//! The state of a single execution of the read_px_metadata scan
struct PxMetadataGlobalState : GlobalTableFunctionState {
  idx_t offset = 0;
};

unique_ptr<FunctionData>
PxMetadataBindFunction(ClientContext &context, TableFunctionBindInput &input,
                       vector<LogicalType> &return_types,
                       vector<string> &names);

unique_ptr<GlobalTableFunctionState>
PxMetadataGlobalInit(ClientContext &context, TableFunctionInitInput &input);

void PxMetadataFunction(ClientContext &context, TableFunctionInput &data,
                        DataChunk &output);

} // namespace duckdb
