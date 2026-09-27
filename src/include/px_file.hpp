#pragma once

#include "variable.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

#include <string>
#include <vector>

enum class PxKeyword : uint8_t {
  UNKNOWN = 0,
  STUB = 1,
  HEADING = 2,
  VALUES = 3,
  CODES = 4,
  DATA = 5,
  DECIMALS = 6
};

//! The variables of a PX file, as they are described by its metadata: the
//! STUB/HEADING keyword declares the variables, the VALUES and CODES keywords
//! fill them, and the DATA keyword holds the observations themselves, which
//! this class does not parse.
struct PxFile {

public:
  PxFile();

  size_t variable_count;
  size_t observations;
  int decimals;

public:
  void AddVariable(std::string name);
  void AddVariableCodeCount(size_t code_count);
  //! Parse everything up to and including the DATA keyword, returns the offset
  //! of the first observation
  size_t ParseMetadata(const char *data, size_t idx, size_t data_size);
  int GetDecimals();

  std::vector<std::string> &GetVariableCodes(size_t var_idx);
  std::vector<std::string> &GetVariableValues(size_t var_idx);
  Variable &GetVariable(size_t var_idx);

private:
  std::vector<Variable> variables;
};
