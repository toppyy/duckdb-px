#include "utils.hpp"

#include <cstdint>
#include <climits>

namespace duckdb {

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

bool IsNumeric(StringView val) {
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

bool IsValidUTF8(const unsigned char *bytes, size_t size) {
  size_t i = 0;
  while (i < size) {
    auto lead = bytes[i];
    if (lead < 0x80) {
      i++;
      continue;
    }

    // Number of bytes of the sequence that follows the leading byte, and the
    // smallest code point that may be encoded with that many
    size_t follow_bytes;
    uint32_t min_code_point;
    if ((lead & 0xE0) == 0xC0) {
      follow_bytes = 1;
      min_code_point = 0x80;
    } else if ((lead & 0xF0) == 0xE0) {
      follow_bytes = 2;
      min_code_point = 0x800;
    } else if ((lead & 0xF8) == 0xF0) {
      follow_bytes = 3;
      min_code_point = 0x10000;
    } else {
      // A continuation byte in leading position, or a byte that UTF-8 does not
      // use at all
      return false;
    }

    if (i + follow_bytes >= size) {
      // The sequence is cut off by the end of the string
      return false;
    }

    // Assemble the code point of the sequence, rejecting the continuation bytes
    // that are not continuation bytes
    uint32_t code_point = lead & (0x7F >> follow_bytes);
    for (size_t j = 1; j <= follow_bytes; j++) {
      auto follow = bytes[i + j];
      if ((follow & 0xC0) != 0x80) {
        return false;
      }
      code_point = (code_point << 6) | (follow & 0x3F);
    }

    if (code_point < min_code_point) {
      // Overlong encoding
      return false;
    }
    if (code_point > 0x10FFFF) {
      // Above the last code point of Unicode
      return false;
    }
    if (code_point >= 0xD800 && code_point <= 0xDFFF) {
      // UTF-16 surrogate halves do not belong in UTF-8
      return false;
    }

    i += follow_bytes + 1;
  }
  return true;
}
// ---------- whitespace lookup table ----------
static const bool WS[128] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

// Manual float parsing for C++11 StringView compatibility

float ParseFloat(StringView sv) {
  if (sv.empty())
    return 0.0f;

  const char *start = sv.data();
  const char *end = start + sv.size();
  const char *p = start;

  // Skip leading whitespace
  while (p < end && WS[(unsigned char)*p])
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
    int digits = 0;
    while (p < end && *p >= '0' && *p <= '9' && digits < 6) {
      exponent = exponent * 10 + (*p - '0');
      p++;
      digits++;
    }
    while (p < end && *p >= '0' && *p <= '9') {
      p++;
    }
    if (exponent > 38) {
      exponent = 38;
    }
    if (exponent < -38) {
      exponent = -38;
    }
    float multiplier = 1.0f;
    for (int i = 0; i < (exponent < 0 ? -exponent : exponent); i++) {
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
int32_t ParseInt32(StringView sv) {
  const char *p = sv.data();
  const char *end = p + sv.size();
  while (p < end && WS[(unsigned char)*p])
    p++;
  if (p >= end)
    return 0;
  const bool negative = (*p == '-');
  p += ((*p == '-') | (*p == '+'));
  const char *digits = p;
  uint64_t result = 0;
  while (p < end) {
    unsigned d = static_cast<unsigned>(static_cast<unsigned char>(*p) - '0');
    if (d > 9)
      break;
    result = result * 10 + d;
    p++;
  }
  if (p == digits)
    return 0;
  const uint64_t limit = negative ? 2147483648ULL : 2147483647ULL;
  if (p - digits > 19 || result > limit)
    return negative ? INT32_MIN : INT32_MAX;
  return negative ? -(int32_t)result : (int32_t)result;
}

/*
int32_t ParseInt32(StringView sv) {
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

  int64_t result = 0;
  while (p < end && *p >= '0' && *p <= '9') {
    result = result * 10 + (*p - '0');
    if (result > 3000000000LL) {
      break;
    }
    p++;
  }
  while (p < end && *p >= '0' && *p <= '9') {
    p++;
  }
  if (result > INT32_MAX)
    result = INT32_MAX;
  if (negative && result > (int64_t)INT32_MAX + 1)
    result = (int64_t)INT32_MAX + 1;
  int32_t out = (int32_t)result;
  return negative ? -out : out;
}
*/

} // namespace duckdb
