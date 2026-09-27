#pragma once

#include "px_scan.hpp"

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//! The set of values that a filter restricts a single column of the scan to
struct PxFilterValues {
  //! Index of the column of the scan that is restricted
  idx_t column_index = 0;
  //! The values that the column is restricted to
  vector<Value> values;
};

//! Called by the optimizer to let the scan look at the filters that are pushed
//! into it. The filters are left in place: DuckDB applies them to the rows that
//! the scan returns anyway, so all that is gained here is that the scan does
//! not have to materialize the observations that can not match.
void PxPushdownComplexFilter(ClientContext &context, LogicalGet &get,
                             FunctionData *bind_data_p,
                             vector<unique_ptr<Expression>> &filters);

} // namespace duckdb
