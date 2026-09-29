#include "variable.hpp"

namespace duckdb {

Variable::Variable(std::string p_name)
    : name(p_name), repetition_factor(0), codes(), values(),
      current_code_index(0), count_in_current_code(0){};

const std::string &Variable::GetName() { return name; };
size_t Variable::CodeCount() { return codes.size(); };

size_t Variable::ValueCount() { return values.size(); };

bool Variable::HasCodes() { return !codes.empty(); };

bool Variable::HasValues() { return !values.empty(); };

void Variable::SetRepetitionFactor(size_t p_rep_factor) {
  repetition_factor = p_rep_factor;
}

size_t Variable::GetRepetitionFactor() { return repetition_factor; }

std::vector<std::string> &Variable::GetCodes() { return codes; }

std::vector<std::string> &Variable::GetValues() { return values; }

size_t Variable::NextCodeIndexSequential() {
  if (codes.empty())
    return 0;
  // A variable without a repetition factor, which is the case until the
  // reader has set them, moves on to the next code every observation
  const size_t period = repetition_factor > 0 ? repetition_factor : 1;
  size_t idx = current_code_index;
  count_in_current_code++;
  if (count_in_current_code >= period) {
    count_in_current_code = 0;
    current_code_index++;
    if (current_code_index >= codes.size()) {
      current_code_index = 0;
    }
  }
  return idx;
}

void Variable::SkipSequential(size_t n) {
  if (codes.empty() || n == 0) {
    return;
  }
  const size_t period = repetition_factor > 0 ? repetition_factor : 1;
  // The state is a position in a cycle of period * CodeCount() observations:
  // count_in_current_code is the offset of the current code inside its period
  // and every CodeCount() periods the code of the variable starts over.
  // Reducing the advance modulo the cycle keeps the arithmetic below from
  // overflowing, however far the reader has to skip.
  const size_t cycle = period * codes.size();
  size_t advanced = (count_in_current_code % cycle) + (n % cycle);
  if (advanced >= cycle) {
    advanced -= cycle;
  }
  count_in_current_code = advanced % period;
  current_code_index = (current_code_index + advanced / period) % codes.size();
}

void Variable::ResetSequentialCounter() {
  current_code_index = 0;
  count_in_current_code = 0;
}

size_t Variable::GetCurrentCodeIndex() { return current_code_index; }

} // namespace duckdb
