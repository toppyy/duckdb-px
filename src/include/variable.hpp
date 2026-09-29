#pragma once
#include <string>
#include <vector>

namespace duckdb {

//! A variable of a PX file: the CODES and VALUES entries that the metadata of
//! the file declares for it, plus the state that walks through its codes while
//! the DATA section of the file is scanned.
struct Variable {

public:
  Variable(std::string p_name);

public:
  const std::string &GetName();
  std::vector<std::string> &GetCodes();
  std::vector<std::string> &GetValues();

  size_t CodeCount();
  size_t ValueCount();

  //! A variable may only be described once. Every CODES and VALUES keyword of
  //! the file would add to the same variable otherwise, which leaves the
  //! number of observations of the file inconsistent with its variables.
  bool HasCodes();
  bool HasValues();

  void SetRepetitionFactor(size_t p_rep_factor);

  //! Advance to the next code of the variable and return its index. The codes
  //! repeat once for every combination of the codes of the variables that
  //! follow this one, which is what the repetition factor stands for.
  size_t NextCodeIndexSequential();
  size_t GetCurrentCodeIndex();
  void ResetSequentialCounter();

private:
  std::string name;
  std::vector<std::string> codes;
  std::vector<std::string> values;
  size_t repetition_factor;

  // Sequential access state
  size_t current_code_index = 0;
  size_t count_in_current_code = 0;
};

} // namespace duckdb
