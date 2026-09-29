#pragma once

#include "duckdb.hpp"

#include <algorithm>
#include <iterator>
#include <vector>

namespace duckdb {

//! A filter on the first variable that the optimizer has pushed down into the
//! scan. The filter is not stored as an expression: it is resolved into the
//! indexes of the CODES of the variable that can satisfy it. Those are the very
//! same indexes that the dictionary vectors of the reader use, so an
//! observation can be matched with a single integer comparison instead of a
//! string comparison.
//! The pushed down filter is only used to skip over the observations that can
//! not match. DuckDB still applies the filter to the rows that the scan
//! returns, so getting a filter wrong is always safe: it can only make the
//! scan read more rows than strictly necessary.
struct PxCodeFilter {

  //! Whether the optimizer pushed a filter on the first variable down at all
  bool active = false;
  //! Sorted, unique list of the code indexes of the first variable that can
  //! match the filter. Can be empty, then no observation matches the filter.
  vector<idx_t> code_indexes;
  // ! Holds the index of the first observatio/value of each block. We can 
  // ! use this to skip observations not matching the pushed down filter.
  vector<size_t> observation_offsets;
  // ! Size of the block. Product of code count of all other variables (excl. value)
  size_t block_size;

  bool Matches(idx_t code_index) const {
    return std::binary_search(code_indexes.begin(), code_indexes.end(),
                              code_index);
  }

  //! The observations of a code are stored as one block and the codes are read
  //! in the order of the CODES, so a code that the reader has moved past can
  //! never be seen again and does not have to be checked anymore.
  void RemovePassedCodes(idx_t code_index) {
    code_indexes.erase(
        code_indexes.begin(),
        std::lower_bound(code_indexes.begin(), code_indexes.end(), code_index));

    observation_offsets.erase(
        observation_offsets.begin(),
        std::lower_bound(observation_offsets.begin(), observation_offsets.end(), code_index));

  }

  //! Returns false when no observation can match the filter anymore
  bool CanMatch() const { return !code_indexes.empty(); }

  //! Both filters are applied to an observation, so only the codes that both
  //! of them can match can be read.
  void Intersect(const PxCodeFilter &other) {
    vector<idx_t> intersection;
    std::set_intersection(code_indexes.begin(), code_indexes.end(),
                          other.code_indexes.begin(), other.code_indexes.end(),
                          std::back_inserter(intersection));
    code_indexes = std::move(intersection);
  }
};

} // namespace duckdb
