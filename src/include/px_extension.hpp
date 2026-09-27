#pragma once

#include "duckdb.hpp"

//! The bundle above only forward-declares CreateTableFunctionInfo and does not
//! know FunctionDescription at all: the API that documents a function in
//! duckdb_functions() is not part of it.
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>

namespace duckdb {

class PxExtension : public Extension {
public:
  void Load(ExtensionLoader &db) override;
  std::string Name() override;
  std::string Version() const override;
};

} // namespace duckdb
