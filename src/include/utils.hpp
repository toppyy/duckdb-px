#pragma once
#include <cstddef>

bool IsWhiteSpace(char c);
bool IsNumeric(char c);

size_t SkipWhiteSpace(const char *data, size_t offset, size_t size);
