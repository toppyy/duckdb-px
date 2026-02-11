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

bool IsNumeric(duckdb::StringView val) {
  if (val.empty())
    return false;

  char c = val[0];
  if (c == '-') {
    // Negative number?
    if (val.size() > 1) {
      c = val[1];
    } else {
      return false; // Just a minus sign is not numeric
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

// Manual float parsing for C++11 StringView compatibility
float ParseFloat(duckdb::StringView sv) {
  if (sv.empty())
    return 0.0f;

  const char *start = sv.data();
  const char *end = start + sv.size();
  const char *p = start;

  // Skip leading whitespace
  while (p < end && IsWhiteSpace(*p))
    p++;

  if (p >= end)
    return 0.0f;

  // Handle sign
  bool negative = false;
  if (*p == '-') {
    negative = true;
    p++;
  } else if (*p == '+') {
    p++;
  }

  if (p >= end)
    return 0.0f;

  // Parse integer part
  float result = 0.0f;
  while (p < end && *p >= '0' && *p <= '9') {
    result = result * 10.0f + (*p - '0');
    p++;
  }

  // Parse fractional part
  if (p < end && *p == '.') {
    p++;
    float fraction = 0.0f;
    float divisor = 1.0f;
    while (p < end && *p >= '0' && *p <= '9') {
      fraction = fraction * 10.0f + (*p - '0');
      divisor *= 10.0f;
      p++;
    }
    result += fraction / divisor;
  }

  // Parse exponent
  if (p < end && (*p == 'e' || *p == 'E')) {
    p++;
    bool exp_negative = false;
    if (p < end && *p == '-') {
      exp_negative = true;
      p++;
    } else if (p < end && *p == '+') {
      p++;
    }

    int32_t exponent = 0;
    while (p < end && *p >= '0' && *p <= '9') {
      exponent = exponent * 10 + (*p - '0');
      p++;
    }

    // Apply exponent
    float multiplier = 1.0f;
    for (int i = 0; i < exponent; i++) {
      multiplier *= 10.0f;
    }

    if (exp_negative) {
      result /= multiplier;
    } else {
      result *= multiplier;
    }
  }

  return negative ? -result : result;
}

// Manual int32 parsing for C++11 StringView compatibility
int32_t ParseInt32(duckdb::StringView sv) {
  if (sv.empty())
    return 0;

  const char *start = sv.data();
  const char *end = start + sv.size();
  const char *p = start;

  // Skip leading whitespace
  while (p < end && IsWhiteSpace(*p))
    p++;

  if (p >= end)
    return 0;

  // Handle sign
  bool negative = false;
  if (*p == '-') {
    negative = true;
    p++;
  } else if (*p == '+') {
    p++;
  }

  if (p >= end)
    return 0;

  // Parse integer part
  int32_t result = 0;
  while (p < end && *p >= '0' && *p <= '9') {
    result = result * 10 + (*p - '0');
    p++;
  }

  return negative ? -result : result;
}
