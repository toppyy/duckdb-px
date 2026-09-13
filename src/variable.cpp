#include "variable.hpp"

Variable::Variable(std::string p_name)
    : name(p_name), repetition_factor(0), codes(), values(),
      current_code_index(0), count_in_current_code(0){};

const std::string &Variable::GetName() { return name; };

size_t Variable::CodeCount() { return codes.size(); };
size_t Variable::ValueCount() { return values.size(); };

size_t Variable::NextCodeIndex(size_t row_idx) {
  if (repetition_factor == 0 || codes.empty()) {
    return 0;
  }
  size_t denom = repetition_factor * codes.size();
  if (denom == 0)
    return 0;
  size_t i = row_idx % denom;
  return i / repetition_factor;
}

std::string Variable::NextCode(size_t row_idx) {
  if (repetition_factor == 0 || codes.empty()) {
    return "";
  }
  size_t denom = repetition_factor * codes.size();
  if (denom == 0)
    return "";
  size_t i = row_idx % denom;
  return codes[i / repetition_factor];
}

void Variable::SetRepetitionFactor(size_t p_rep_factor) {
  repetition_factor = p_rep_factor;
}

std::vector<std::string> &Variable::GetCodes() { return codes; }

std::vector<std::string> &Variable::GetValues() { return values; }

size_t Variable::NextCodeIndexSequential() {
  if (codes.empty())
    return 0;
  if (repetition_factor == 0) {
    size_t idx = current_code_index;
    current_code_index = (current_code_index + 1) % codes.size();
    return idx;
  }
  size_t idx = current_code_index;
  count_in_current_code++;
  if (count_in_current_code >= repetition_factor) {
    count_in_current_code = 0;
    current_code_index++;
    if (current_code_index >= codes.size()) {
      current_code_index = 0;
    }
  }
  return idx;
}

void Variable::ResetSequentialCounter() {
  current_code_index = 0;
  count_in_current_code = 0;
}
