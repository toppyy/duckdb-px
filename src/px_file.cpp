#include "px_file.hpp"
#include "px_parser.hpp"

#include "utils.hpp"

PxFile::PxFile() : variable_count(0), variables(), observations(1) {
  variables.reserve(10);
}

void PxFile::AddVariable(std::string name) {
  if (name.empty()) {
    throw duckdb::BinderException("Variable name cannot be empty");
  }
  variable_count++;
  variables.emplace_back(name);
}

int PxFile::GetDecimals() { return decimals; }

size_t PxFile::ParseMetadata(const char *data, size_t idx, size_t data_size) {
  if (data_size == 0) {
    throw duckdb::BinderException("PX file is empty");
  }
  decimals = 3;
  PxKeyword current_keyword = PxKeyword::UNKNOWN;
  do {
    while (idx < data_size && IsWhiteSpace(data[idx])) {
      idx++;
    }
    if (idx >= data_size) {
      throw duckdb::BinderException(
          "Reached EOF when parsing keywords, missing DATA");
    }
    size_t remaining = data_size - idx;
    current_keyword = ParseKeyword(data + idx, remaining);
    if (current_keyword == PxKeyword::UNKNOWN) {
      bool found = false;
      while (idx < data_size) {
        if (data[idx] == ';') {
          found = true;
          idx++;
          break;
        }
        idx++;
      }
      if (!found) {
        throw duckdb::BinderException("Reached EOF when parsing keywords");
      }
      if (idx < data_size && data[idx] == ';') {
        idx++;
      }
      continue;
    }
    if (current_keyword == PxKeyword::DATA)
      break;
    if ((current_keyword == PxKeyword::STUB) ||
        (current_keyword == PxKeyword::HEADING)) {
      idx += ParseStubOrHeading(data, idx, data_size, *this);
      continue;
    }
    if (current_keyword == PxKeyword::DECIMALS) {
      idx += ParseDecimals(data, idx, data_size, decimals);
      continue;
    }
    if (current_keyword == PxKeyword::VALUES) {
      idx += ParseValues(data, idx, data_size, *this);
      continue;
    }
    if (current_keyword == PxKeyword::CODES) {
      idx += ParseCodes(data, idx, data_size, *this);
      continue;
    }
    idx++;
  } while (true);
  if (variable_count == 0) {
    throw duckdb::BinderException("No variables defined via STUB/HEADING");
  }
  for (size_t i = 0; i < variable_count; i++) {
    if (variables[i].CodeCount() == 0) {
      throw duckdb::BinderException("Variable '%s' has no CODES",
                                    variables[i].GetName().c_str());
    }
    if (variables[i].CodeCount() != variables[i].ValueCount() &&
        variables[i].ValueCount() != 0) {
      throw duckdb::BinderException(
          "Number of VALUES and CODES do not match for variable '%s'",
          variables[i].GetName().c_str());
    }
  }
  idx += 5;
  idx = SkipWhiteSpace(data, idx, data_size);
  return idx;
}

void PxFile::AddVariableCodeCount(size_t code_count) {
  if (code_count == 0) {
    throw duckdb::BinderException("Code count cannot be zero");
  }
  if (observations > SIZE_MAX / code_count) {
    throw duckdb::BinderException("Too many observations, product overflow");
  }
  observations *= code_count;
}

std::vector<std::string> &PxFile::GetVariableCodes(size_t var_idx) {
  if (var_idx >= variables.size()) {
    throw duckdb::InternalException("GetVariableCodes out of range");
  }
  return variables[var_idx].GetCodes();
}

std::vector<std::string> &PxFile::GetVariableValues(size_t var_idx) {
  if (var_idx >= variables.size()) {
    throw duckdb::InternalException("GetVariableValues out of range");
  }
  return variables[var_idx].GetValues();
}

Variable &PxFile::GetVariable(size_t var_idx) {
  if (var_idx >= variables.size()) {
    throw duckdb::InternalException("GetVariable out of range %zu / %zu",
                                    var_idx, variables.size());
  }
  return variables[var_idx];
}
