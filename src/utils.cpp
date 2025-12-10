#include "utils.hpp"

bool IsWhiteSpace(char c) {
  if (c == 32)
    return true;
  if (c == '\r')
    return true;
  if (c == '\n')
    return true;
  if (c == '\t')
    return true;
  return false;
}

bool IsNumeric(std::string val) {

  char c = val[0];
  if (c == '-') {
    // Negative number?
    if (val.size() > 1) {
      c = val[1];
    }
  }

  return c >= '0' && c <= '9';
}

size_t SkipWhiteSpace(const char *data, size_t offset, size_t size) {
  while (offset < size) {
    if (!IsWhiteSpace(data[offset])) {
      break;
    }
    offset++;
  };

  return offset;
}
