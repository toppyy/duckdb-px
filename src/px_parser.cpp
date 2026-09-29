#include "px_parser.hpp"

#include "px_file.hpp"
#include "utils.hpp"

#include <cstring>
#include <stdexcept>

namespace duckdb {

size_t ParseList(const char *data, size_t offset, size_t data_size,
                 std::vector<string> &result, char end) {
  size_t idx = 0;
  bool quote_open = false;
  string element = "";
  while (true) {
    if (offset + idx >= data_size) {
      throw BinderException("Unexpected EOF while parsing list, expected '%c'",
                            end);
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
    throw BinderException("Unclosed quote in list");
  }
  return idx;
}

size_t FindVarName(const char *data, size_t offset, size_t data_size,
                   string &varname) {
  size_t idx = 0;
  char c = 0;
  while (true) {
    if (offset + idx >= data_size) {
      throw BinderException("Unexpected EOF while parsing variable name");
    }
    c = data[offset + idx];
    idx++;
    if (c == '"')
      break;
  }
  string tmp;
  while (true) {
    if (offset + idx >= data_size) {
      throw BinderException("Unclosed quote in variable name");
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

size_t ParseStubOrHeading(const char *data, size_t offset, size_t data_size,
                          PxFile &pxfile) {
  std::vector<string> varnames;
  size_t inc = ParseList(data, offset, data_size, varnames);
  for (auto &name : varnames) {
    pxfile.AddVariable(name);
  }
  return inc;
}

size_t ParseValues(const char *data, size_t offset, size_t data_size,
                   PxFile &pxfile) {
  string varname;
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
    throw BinderException(
        "Values specified for a variable not found in STUB/HEADING");
  if (pxfile.GetVariable(var_idx).HasValues()) {
    throw BinderException("Duplicate VALUES for variable '%s'",
                          varname.c_str());
  }
  idx += ParseList(data, offset + idx, data_size,
                   pxfile.GetVariableValues(var_idx));
  return idx;
}

size_t ParseCodes(const char *data, size_t offset, size_t data_size,
                  PxFile &pxfile) {
  string varname;
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
    throw BinderException(
        "Codes specified for a variable not found in STUB/HEADING");
  if (pxfile.GetVariable(var_idx).HasCodes()) {
    throw BinderException("Duplicate CODES for variable '%s'", varname.c_str());
  }
  idx += ParseList(data, offset + idx, data_size,
                   pxfile.GetVariableCodes(var_idx));
  size_t cc = pxfile.GetVariable(var_idx).CodeCount();
  if (cc == 0) {
    throw BinderException("CODES for variable '%s' is empty", varname.c_str());
  }
  if (cc > STANDARD_VECTOR_SIZE) {
    throw BinderException("CODES for variable '%s' exceeds limit %d (got %zu)",
                          varname.c_str(), STANDARD_VECTOR_SIZE, cc);
  }
  pxfile.AddVariableCodeCount(cc);
  return idx;
}

size_t ParseDecimals(const char *data, size_t offset, size_t data_size,
                     int &decimals) {
  size_t idx = 9;
  string s_decimals;
  if (offset + 9 > data_size) {
    throw BinderException("Unexpected EOF while parsing DECIMALS");
  }
  while (offset + idx < data_size && data[offset + idx] >= '0' &&
         data[offset + idx] <= '9') {
    s_decimals += data[offset + idx];
    idx++;
  }
  if (s_decimals.empty()) {
    throw BinderException("Invalid DECIMALS value");
  }
  try {
    decimals = std::stoi(s_decimals);
  } catch (const std::invalid_argument &e) {
    throw BinderException("Invalid DECIMALS value: %s", s_decimals.c_str());
  } catch (const std::out_of_range &e) {
    throw BinderException("DECIMALS value out of range: %s",
                          s_decimals.c_str());
  }
  return idx;
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

string ISO88591toUTF8(const string &original_string) {
  const auto *bytes =
      reinterpret_cast<const unsigned char *>(original_string.data());
  size_t size = original_string.size();

  // A PX file is officially ISO-8859-1, but plenty of the files in the wild are
  // already UTF-8. Whatever the file claims to be, DuckDB needs valid UTF-8
  // out of here: handing it raw high bytes makes it abort the query with an
  // internal error deep inside utf8proc instead of reading the file.
  // Input that already is valid UTF-8 is therefore passed through untouched.
  if (IsValidUTF8(bytes, size)) {
    return original_string;
  }

  // Everything else is read as ISO-8859-1, where every one of the 128 high
  // bytes is a character of its own and maps to U+0080 - U+00FF.
  string rtrn;
  rtrn.reserve(size);
  for (size_t i = 0; i < size; i++) {
    auto byte = bytes[i];
    if (byte < 0x80) {
      rtrn.push_back(static_cast<char>(byte));
      continue;
    }
    rtrn.push_back(static_cast<char>(0xC0 | (byte >> 6)));
    rtrn.push_back(static_cast<char>(0x80 | (byte & 0x3F)));
  }
  return rtrn;
}

} // namespace duckdb
