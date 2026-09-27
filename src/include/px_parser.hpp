#pragma once

#include "px_file.hpp"

#include <cstddef>
#include <string>
#include <vector>

namespace duckdb {

/* Parser helpers */
string ISO88591toUTF8(const string &original_string);
size_t ParseList(const char *data, size_t offset, size_t data_size,
                 std::vector<string> &result, char end = ';');
size_t FindVarName(const char *data, size_t offset, size_t data_size,
                   string &varname);
PxKeyword ParseKeyword(const char *data, size_t remaining);

/* Parse specific keywords */
size_t ParseStubOrHeading(const char *data, size_t offset, size_t data_size,
                          PxFile &pxfile);
size_t ParseValues(const char *data, size_t offset, size_t data_size,
                   PxFile &pxfile);
size_t ParseCodes(const char *data, size_t offset, size_t data_size,
                  PxFile &pxfile);
size_t ParseDecimals(const char *data, size_t offset, size_t data_size,
                     int &decimals);

} // namespace duckdb
