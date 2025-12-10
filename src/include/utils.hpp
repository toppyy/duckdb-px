#pragma once
#include <string>
#include <cstddef>

bool IsWhiteSpace(char c);
bool IsNumeric(std::string val);

size_t SkipWhiteSpace(const char *data, size_t offset, size_t size);
