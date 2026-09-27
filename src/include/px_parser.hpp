#pragma once

#include "px_file.hpp"

#include <string>
#include <vector>

/* Parser helpers */
std::string ISO88591toUTF8(std::string original_string);
size_t ParseList(const char *data, size_t offset, size_t data_size,
                 std::vector<std::string> &result, char end = ';');
size_t FindVarName(const char *data, size_t offset, size_t data_size,
                   std::string &varname);
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
