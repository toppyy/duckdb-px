#include "variable.hpp"

Variable::Variable(std::string p_name)
    : name(p_name), repetition_factor(0), codes(), values(){};

const std::string &Variable::GetName() { return name; };

size_t Variable::CodeCount() { return codes.size(); };
size_t Variable::ValueCount() { return values.size(); };

size_t Variable::NextCodeIndex(size_t row_idx) {
  size_t i = row_idx % (repetition_factor * codes.size());
  return i / repetition_factor;
}

std::string Variable::NextCode(size_t row_idx) {
  size_t i = row_idx % (repetition_factor * codes.size());
  return codes[i / repetition_factor];
}

void Variable::SetRepetitionFactor(size_t p_rep_factor) {
  repetition_factor = p_rep_factor;
}

std::vector<std::string> &Variable::GetCodes() { return codes; }

std::vector<std::string> &Variable::GetValues() { return values; }
