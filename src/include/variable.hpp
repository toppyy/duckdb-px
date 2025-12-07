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

private:
  std::string name;
  std::vector<std::string> codes;
  std::vector<std::string> values;
  size_t repetition_factor;
};
