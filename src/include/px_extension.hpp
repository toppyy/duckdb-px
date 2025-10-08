#pragma once
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "px.hpp"
#include "variable.hpp"
#include "utils.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include <iostream>

namespace duckdb {

class PxExtension : public Extension {
public:
  void Load(ExtensionLoader &db) override;
  std::string Name() override;
  std::string Version() const override;
};

} // namespace duckdb
