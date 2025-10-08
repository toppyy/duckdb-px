#include "px.hpp"

size_t ParseList(const char *data, std::vector<std::string> &result, char end) {
  // Expects a string which contains a list of quoted elements
  // Eg. "monkey","island","is","cool"

  size_t idx = 0;
  bool quote_open = false;
  char c = end;
  std::string element = "";

  while ((c = data[idx]) != end) {
    idx++;
    if (c == '"') {
      if (quote_open)
        result.push_back(ISO88591toUTF8(element));
      element = "";
      quote_open = !quote_open;
      continue;
    }
    element.push_back(c);
  }
  return idx;
}

size_t FindVarName(const char *data, std::string &varname) {
  // The variable is specified within brackets and quotes
  // for example:
  //      VALUES("somevariable")="val1","val2";
  //      CODES("somevariable")="val1","val2";

  size_t idx = 0;
  char c = 0;

  while ((c = data[idx++]) != '"') {
  };

  std::string tmp;
  while ((c = data[idx++]) != '"') {
    tmp.push_back(c);
  };

  varname = ISO88591toUTF8(tmp);

  return idx;
}

size_t ParseStubOrHeading(const char *data, PxFile &pxfile) {
  // Returns the number of variables listed in a STUB-declaration
  // for example STUB="var1","var2";

  std::vector<std::string> varnames;
  size_t inc = ParseList(data, varnames);

  for (auto name : varnames) {
    pxfile.AddVariable(name);
  }
  return inc;
}

size_t ParseValues(const char *data, PxFile &pxfile) {
  // Values is a list of values associated with a variable

  std::string varname;
  size_t idx = FindVarName(data, varname);

  size_t var_idx = 0;
  bool var_found = false;
  char c = 0;

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

  idx += ParseList(data + idx, pxfile.GetVariableValues(var_idx));

  return idx;
}

size_t ParseCodes(const char *data, PxFile &pxfile) {
  // Codes is a list of values associated with a variable

  std::string varname;
  size_t idx = FindVarName(data, varname);

  size_t var_idx = 0;
  bool var_found = false;
  char c = 0;

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

  idx += ParseList(data + idx, pxfile.GetVariableCodes(var_idx));

  pxfile.AddVariableCodeCount(pxfile.GetVariable(var_idx).CodeCount());

  return idx;
}

size_t ParseDecimals(const char *data, int &decimals) {

  size_t idx = 9;
  std::string s_decimals;

  while (data[idx] >= '0' && data[idx] < '9') {
    s_decimals += data[idx];
    idx++;
  }

  try {
    decimals = std::stoi(s_decimals);
  } catch (const std::invalid_argument &e) {
    std::cerr << "Invalid argument: " << e.what() << std::endl;
  } catch (const std::out_of_range &e) {
    std::cerr << "Out of range: " << e.what() << std::endl;
  }

  return idx;
}

PxKeyword ParseKeyword(const char *data) {

  if (std::strncmp(data, "STUB=", 5) == 0) {
    return PxKeyword::STUB;
  }

  if (std::strncmp(data, "HEADING=", 8) == 0) {
    return PxKeyword::HEADING;
  }

  if (std::strncmp(data, "VALUES(", 7) == 0) {
    return PxKeyword::VALUES;
  }

  if (std::strncmp(data, "CODES(", 6) == 0) {
    return PxKeyword::CODES;
  }

  if (std::strncmp(data, "DATA=", 5) == 0) {
    return PxKeyword::DATA;
  }

  if (std::strncmp(data, "DECIMALS=", 9) == 0) {
    return PxKeyword::DECIMALS;
  }

  return PxKeyword::UNKNOWN;
};

std::string ISO88591toUTF8(std::string original_string) {
  std::string rtrn;
  for (int i = 0; i < original_string.size(); i++) {

    switch (original_string[i]) {
    case static_cast<char>(0xe4): // ä
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xA4);
      break;
    case static_cast<char>(0xf6): // ö
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xB6);
      break;
    case static_cast<char>(0xe5): // å
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0xA5);
      break;
    case static_cast<char>(0xC4): // Ä
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x84);
      break;
    case static_cast<char>(0xD6): // Ö
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x96);
      break;
    case static_cast<char>(0xC5): // Å
      rtrn += static_cast<char>(0xC3);
      rtrn += static_cast<char>(0x85);
      break;
    default:
      rtrn += original_string[i];
    }
  }
  return rtrn;
}

PxFile::PxFile() : variable_count(0), variables(), observations(1) {
  variables.reserve(10);
}

void PxFile::AddVariable(std::string name) {
  variable_count++;
  variables.emplace_back(name);
}

int PxFile::GetDecimals() { return decimals; }

size_t PxFile::ParseMetadata(const char *data, size_t idx, size_t data_size) {

  decimals = 3;

  PxKeyword current_keyword = PxKeyword::UNKNOWN;

  do {
    while (IsWhiteSpace(data[idx])) {
      idx++;
    };

    current_keyword = ParseKeyword(data + idx);

    if (current_keyword == PxKeyword::UNKNOWN) {
      // The keyword did not match, fast-forward to next keyword
      // so that "DECIMALS" is not found within "SHOWDECIMALS", for example
      while (data[idx++] != ';') {
        if (idx >= data_size) {
          throw duckdb::BinderException("Reached EOF when parsing keywords");
        }
      };
      idx++;
      continue;
    }

    if (current_keyword == PxKeyword::DATA)
      break;

    if ((current_keyword == PxKeyword::STUB) ||
        (current_keyword == PxKeyword::HEADING)) {
      idx += ParseStubOrHeading(data + idx, *this);
      continue;
    }

    if (current_keyword == PxKeyword::DECIMALS) {
      idx += ParseDecimals(data + idx, decimals);
      continue;
    }

    if (current_keyword == PxKeyword::VALUES) {
      idx += ParseValues(data + idx, *this);
      continue;
    }

    if (current_keyword == PxKeyword::CODES) {
      idx += ParseCodes(data + idx, *this);
      continue;
    }

    idx++;

  } while (true);

  // idx points to DATA= after the loop
  // so we can skip 'DATA=' by incrementing idx
  idx += 5;
  idx = SkipWhiteSpace(data, idx, data_size);

  return idx;
}

std::string PxFile::GetValueForVariable(size_t var_idx, size_t row_idx) {
  return variables[var_idx].NextCode(row_idx);
}

void PxFile::AddVariableCodeCount(size_t code_count) {
  observations *= code_count;
}

std::vector<std::string> &PxFile::GetVariableCodes(size_t var_idx) {
  return variables[var_idx].GetCodes();
}

std::vector<std::string> &PxFile::GetVariableValues(size_t var_idx) {
  return variables[var_idx].GetValues();
}

Variable &PxFile::GetVariable(size_t var_idx) { return variables[var_idx]; }
