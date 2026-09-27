#include "px_parser.hpp"

#include "px_file.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>

size_t ParseList(const char *data, size_t offset, size_t data_size,
                 std::vector<std::string> &result, char end) {
  size_t idx = 0;
  bool quote_open = false;
  std::string element = "";
  while (true) {
    if (offset + idx >= data_size) {
      throw duckdb::BinderException(
          "Unexpected EOF while parsing list, expected '%c'", end);
    }
    char c = data[offset + idx];
    if (c == end && !quote_open) {
      break;
    }
    idx++;
    if (c == '"') {
      if (quote_open) {
        result.push_back(ISO88591toUTF8(element));
      }
      element = "";
      quote_open = !quote_open;
      continue;
    }
    if (quote_open) {
      element.push_back(c);
    }
  }
  if (quote_open) {
    throw duckdb::BinderException("Unclosed quote in list");
  }
  return idx;
}

size_t ParseList(const char *data, std::vector<std::string> &result, char end) {
  size_t len = 0;
  while (data[len] != '\0' && data[len] != end) {
    len++;
    if (len > 10000000) {
      throw duckdb::BinderException("List too large or missing terminator");
    }
  }
  if (data[len] != end) {
    throw duckdb::BinderException("Unexpected EOF while parsing list");
  }
  std::vector<std::string> tmp;
  size_t consumed = ParseList(data, 0, len + 1, tmp, end);
  result = std::move(tmp);
  return consumed;
}

size_t FindVarName(const char *data, size_t offset, size_t data_size,
                   std::string &varname) {
  size_t idx = 0;
  char c = 0;
  while (true) {
    if (offset + idx >= data_size) {
      throw duckdb::BinderException(
          "Unexpected EOF while parsing variable name");
    }
    c = data[offset + idx];
    idx++;
    if (c == '"')
      break;
  }
  std::string tmp;
  while (true) {
    if (offset + idx >= data_size) {
      throw duckdb::BinderException("Unclosed quote in variable name");
    }
    c = data[offset + idx];
    idx++;
    if (c == '"')
      break;
    tmp.push_back(c);
  }
  varname = ISO88591toUTF8(tmp);
  return idx;
}

size_t FindVarName(const char *data, std::string &varname) {
  size_t len = 0;
  while (data[len] != '\0')
    len++;
  return FindVarName(data, 0, len, varname);
}

size_t ParseStubOrHeading(const char *data, size_t offset, size_t data_size,
                          PxFile &pxfile) {
  std::vector<std::string> varnames;
  size_t inc = ParseList(data, offset, data_size, varnames);
  for (auto &name : varnames) {
    pxfile.AddVariable(name);
  }
  return inc;
}

size_t ParseStubOrHeading(const char *data, PxFile &pxfile) {
  std::vector<std::string> varnames;
  size_t inc = ParseList(data, varnames);
  for (auto &name : varnames) {
    pxfile.AddVariable(name);
  }
  return inc;
}

size_t ParseValues(const char *data, size_t offset, size_t data_size,
                   PxFile &pxfile) {
  std::string varname;
  size_t idx = FindVarName(data, offset, data_size, varname);
  size_t var_idx = 0;
  bool var_found = false;
  while (var_idx < pxfile.variable_count) {
    if (pxfile.GetVariable(var_idx).GetName() == varname) {
      var_found = true;
      break;
    }
    var_idx++;
  }
  if (!var_found)
    throw duckdb::BinderException(
        "Values specified for a variable not found in STUB/HEADING");
  idx += ParseList(data, offset + idx, data_size,
                   pxfile.GetVariableValues(var_idx));
  return idx;
}

size_t ParseValues(const char *data, PxFile &pxfile) {
  size_t len = 0;
  while (data[len] != '\0')
    len++;
  return ParseValues(data, 0, len, pxfile);
}

size_t ParseCodes(const char *data, size_t offset, size_t data_size,
                  PxFile &pxfile) {
  std::string varname;
  size_t idx = FindVarName(data, offset, data_size, varname);
  size_t var_idx = 0;
  bool var_found = false;
  while (var_idx < pxfile.variable_count) {
    if (pxfile.GetVariable(var_idx).GetName() == varname) {
      var_found = true;
      break;
    }
    var_idx++;
  }
  if (!var_found)
    throw duckdb::BinderException(
        "Codes specified for a variable not found in STUB/HEADING");
  idx += ParseList(data, offset + idx, data_size,
                   pxfile.GetVariableCodes(var_idx));
  size_t cc = pxfile.GetVariable(var_idx).CodeCount();
  if (cc == 0) {
    throw duckdb::BinderException("CODES for variable '%s' is empty",
                                  varname.c_str());
  }
  if (cc > STANDARD_VECTOR_SIZE) {
    throw duckdb::BinderException(
        "CODES for variable '%s' exceeds limit %d (got %zu)", varname.c_str(),
        STANDARD_VECTOR_SIZE, cc);
  }
  pxfile.AddVariableCodeCount(cc);
  return idx;
}

size_t ParseCodes(const char *data, PxFile &pxfile) {
  size_t len = 0;
  while (data[len] != '\0')
    len++;
  return ParseCodes(data, 0, len, pxfile);
}

size_t ParseDecimals(const char *data, size_t offset, size_t data_size,
                     int &decimals) {
  size_t idx = 9;
  std::string s_decimals;
  if (offset + 9 > data_size) {
    throw duckdb::BinderException("Unexpected EOF while parsing DECIMALS");
  }
  while (offset + idx < data_size && data[offset + idx] >= '0' &&
         data[offset + idx] <= '9') {
    s_decimals += data[offset + idx];
    idx++;
  }
  if (s_decimals.empty()) {
    throw duckdb::BinderException("Invalid DECIMALS value");
  }
  try {
    decimals = std::stoi(s_decimals);
  } catch (const std::invalid_argument &e) {
    throw duckdb::BinderException("Invalid DECIMALS value: %s",
                                  s_decimals.c_str());
  } catch (const std::out_of_range &e) {
    throw duckdb::BinderException("DECIMALS value out of range: %s",
                                  s_decimals.c_str());
  }
  return idx;
}

size_t ParseDecimals(const char *data, int &decimals) {
  size_t len = 0;
  while (data[len] != '\0')
    len++;
  return ParseDecimals(data, 0, len, decimals);
}

PxKeyword ParseKeyword(const char *data, size_t remaining) {
  if (remaining >= 5 && std::strncmp(data, "STUB=", 5) == 0) {
    return PxKeyword::STUB;
  }
  if (remaining >= 8 && std::strncmp(data, "HEADING=", 8) == 0) {
    return PxKeyword::HEADING;
  }
  if (remaining >= 7 && std::strncmp(data, "VALUES(", 7) == 0) {
    return PxKeyword::VALUES;
  }
  if (remaining >= 6 && std::strncmp(data, "CODES(", 6) == 0) {
    return PxKeyword::CODES;
  }
  if (remaining >= 5 && std::strncmp(data, "DATA=", 5) == 0) {
    return PxKeyword::DATA;
  }
  if (remaining >= 9 && std::strncmp(data, "DECIMALS=", 9) == 0) {
    return PxKeyword::DECIMALS;
  }
  return PxKeyword::UNKNOWN;
}

PxKeyword ParseKeyword(const char *data) {
  size_t len = 0;
  while (data[len] != '\0')
    len++;
  return ParseKeyword(data, len);
}

std::string ISO88591toUTF8(std::string original_string) {
  std::string rtrn;
  for (size_t i = 0; i < original_string.size(); i++) {
    switch (original_string[i]) {
    case static_cast<char>(0xe4):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xA4);
      break;
    case static_cast<char>(0xf6):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xB6);
      break;
    case static_cast<char>(0xe5):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xA5);
      break;
    case static_cast<char>(0xC4):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x84);
      break;
    case static_cast<char>(0xD6):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x96);
      break;
    case static_cast<char>(0xC5):
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x85);
      break;
    default:
      rtrn += original_string[i];
    }
  }
  return rtrn;
}
