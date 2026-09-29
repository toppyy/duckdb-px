#pragma once
#include <string>
#include <cstddef>
#include <cstdint>

namespace duckdb {

// Simple C++11-compatible StringView class
class StringView {
private:
  const char *data_;
  size_t size_;

public:
  StringView() : data_(nullptr), size_(0) {}
  StringView(const char *data, size_t size) : data_(data), size_(size) {}

  const char *data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  char operator[](size_t pos) const { return data_[pos]; }
  char at(size_t pos) const { return data_[pos]; }
};

bool IsWhiteSpace(char c);
bool IsNumeric(StringView val);

//! Whether the bytes are well-formed UTF-8, i.e. whether DuckDB can safely put
//! them in a VARCHAR. Only rejects invalid byte sequences, an empty string is
//! valid.
bool IsValidUTF8(const unsigned char *bytes, size_t size);

size_t SkipWhiteSpace(const char *data, size_t offset, size_t size);

// Manual parsing functions for C++11 StringView compatibility
float ParseFloat(StringView sv);
int32_t ParseInt32(StringView sv);

} // namespace duckdb
