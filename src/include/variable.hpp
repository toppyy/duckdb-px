#pragma once
#include <string>
#include <vector>

struct Variable {

public:
  Variable(std::string p_name);

public:
  const std::string &GetName();
  std::vector<std::string> &GetCodes();
  std::vector<std::string> &GetValues();

  size_t CodeCount();
  size_t ValueCount();

  void SetRepetitionFactor(size_t p_rep_factor);
  std::string NextCode(size_t row_idx);
  size_t NextCodeIndex(size_t row_idx);

  // Sequential access methods - much faster, no division/modulo
  size_t NextCodeIndexSequential();
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
