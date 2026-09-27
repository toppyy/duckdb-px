#include "px_filter_pushdown.hpp"

//! duckdb.hpp only forward-declares the planner types that this file works
//! with, so the definitions have to be included here.
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//! Resolve a column reference of a filter into the index of the column of the
//! scan. Returns false when the expression does not reference a column of this
//! scan at all.
static bool TryGetScanColumn(LogicalGet &get, Expression &expr,
                             idx_t &column_index) {
  if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  auto &ref = expr.Cast<BoundColumnRefExpression>();
  if (ref.depth != 0 || ref.binding.table_index != get.table_index) {
    return false;
  }
  auto &column_ids = get.GetColumnIds();
  if (ref.binding.column_index >= column_ids.size()) {
    return false;
  }
  auto &column_id = column_ids[ref.binding.column_index];
  if (!column_id.HasPrimaryIndex() || column_id.IsVirtualColumn()) {
    return false;
  }
  column_index = column_id.GetPrimaryIndex();
  return true;
}

//! Try to extract the values that a filter restricts a column of the scan to.
//! Only expressions of the form
//!   <column> = <constant>
//!   <column> IN (<constant>, ...)
//!   <column> = <constant> OR <column> = <constant> ...
//! are understood. Returns false when nothing can be derived from the
//! expression, in which case the expression does not restrict the scan at all.
static bool TryExtractFilterValues(LogicalGet &get, Expression &expr,
                                   PxFilterValues &result) {
  switch (expr.GetExpressionClass()) {
  case ExpressionClass::BOUND_COMPARISON: {
    auto &comparison = expr.Cast<BoundComparisonExpression>();
    if (comparison.GetExpressionType() != ExpressionType::COMPARE_EQUAL) {
      return false;
    }
    Expression *column = nullptr;
    Expression *constant = nullptr;
    if (comparison.left->GetExpressionClass() ==
        ExpressionClass::BOUND_COLUMN_REF) {
      column = comparison.left.get();
      constant = comparison.right.get();
    } else if (comparison.right->GetExpressionClass() ==
               ExpressionClass::BOUND_COLUMN_REF) {
      column = comparison.right.get();
      constant = comparison.left.get();
    }
    if (!column ||
        constant->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
      return false;
    }
    if (!TryGetScanColumn(get, *column, result.column_index)) {
      return false;
    }
    result.values.push_back(constant->Cast<BoundConstantExpression>().value);
    return true;
  }
  case ExpressionClass::BOUND_OPERATOR: {
    auto &in_operator = expr.Cast<BoundOperatorExpression>();
    if (in_operator.GetExpressionType() != ExpressionType::COMPARE_IN ||
        in_operator.children.size() < 2) {
      return false;
    }
    if (!TryGetScanColumn(get, *in_operator.children[0], result.column_index)) {
      return false;
    }
    for (idx_t i = 1; i < in_operator.children.size(); i++) {
      auto &child = *in_operator.children[i];
      if (child.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
        return false;
      }
      result.values.push_back(child.Cast<BoundConstantExpression>().value);
    }
    return true;
  }
  case ExpressionClass::BOUND_CONJUNCTION: {
    auto &conjunction = expr.Cast<BoundConjunctionExpression>();
    if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_OR) {
      return false;
    }
    PxFilterValues left;
    PxFilterValues right;
    if (!TryExtractFilterValues(get, *conjunction.children[0], left) ||
        !TryExtractFilterValues(get, *conjunction.children[1], right) ||
        left.column_index != right.column_index) {
      return false;
    }
    // An observation matches the disjunction if it matches either side, so the
    // codes of both sides can match
    result = std::move(left);
    result.values.insert(result.values.end(), right.values.begin(),
                         right.values.end());
    return true;
  }
  default:
    return false;
  }
}

//! Convert a constant of a filter into the string that is compared against the
//! CODES of a variable
static bool TryGetConstantString(ClientContext &context, const Value &value,
                                 string &result) {
  if (value.IsNull()) {
    return false;
  }
  if (value.type() == LogicalType::VARCHAR) {
    result = StringValue::Get(value);
    return true;
  }
  Value casted;
  if (!value.TryCastAs(context, LogicalType::VARCHAR, casted, nullptr)) {
    return false;
  }
  result = StringValue::Get(casted);
  return true;
}

//! Resolve the indexes of the CODES of a variable that can match the values
static vector<idx_t> ResolveCodeIndexes(ClientContext &context,
                                        Variable &variable,
                                        const vector<Value> &values) {
  auto &codes = variable.GetCodes();
  vector<idx_t> code_indexes;
  for (auto &value : values) {
    string code;
    if (!TryGetConstantString(context, value, code)) {
      continue;
    }
    for (idx_t code_idx = 0; code_idx < codes.size(); code_idx++) {
      if (codes[code_idx] == code) {
        code_indexes.push_back(code_idx);
      }
    }
  }
  std::sort(code_indexes.begin(), code_indexes.end());
  code_indexes.erase(std::unique(code_indexes.begin(), code_indexes.end()),
                     code_indexes.end());
  return code_indexes;
}

//! The number of filter values that are shown in the plan at most
static constexpr idx_t PX_DESCRIBED_VALUES = 5;

//! Describe a filter that has been pushed down into the scan, in the same way
//! that DuckDB describes the filters that it pushes down itself
static string PxDescribePushdown(LogicalGet &get,
                                 const PxFilterValues &filters) {
  string column = to_string(filters.column_index);
  if (filters.column_index < get.names.size()) {
    column = get.names[filters.column_index];
  }
  if (filters.values.empty()) {
    return column;
  }

  // A single value is shown as an equality, several values as an IN list
  string result = column + " = ";
  if (filters.values.size() > 1) {
    result = column + " IN (";
  }
  for (idx_t i = 0;
       i < MinValue<idx_t>(filters.values.size(), PX_DESCRIBED_VALUES); i++) {
    if (i > 0) {
      result += ", ";
    }
    result += filters.values[i].ToSQLString();
  }
  if (filters.values.size() > PX_DESCRIBED_VALUES) {
    result += StringUtil::Format(", ... (%llu values)",
                                 (unsigned long long)filters.values.size());
  }
  if (filters.values.size() > 1) {
    result += ")";
  }
  return result;
}

void PxPushdownComplexFilter(ClientContext &context, LogicalGet &get,
                             FunctionData *bind_data_p,
                             vector<unique_ptr<Expression>> &filters) {
  auto &bind_data = bind_data_p->Cast<PxBindData>();
  auto &pxfile = bind_data.reader->pxfile;
  if (pxfile.variable_count == 0) {
    return;
  }

  // The scan can have more than one filter pushed into it, they are all
  // described in the plan
  string described;
  for (auto &filter : filters) {
    PxFilterValues filter_values;
    if (!TryExtractFilterValues(get, *filter, filter_values)) {
      continue;
    }
    // The observations of the first variable are stored as one block of
    // observations per code, which makes it the cheapest variable to skip
    // over. Any other column, the "value" column included, is left alone.
    // TODO: resolve and push down filters for the other variables as well
    if (filter_values.column_index != 0) {
      continue;
    }
    PxCodeFilter code_filter;
    code_filter.active = true;
    code_filter.code_indexes = ResolveCodeIndexes(
        context, pxfile.GetVariable(0), filter_values.values);
    if (bind_data.code_filter.active) {
      // Every filter of the query is applied to an observation, so when the
      // first variable has more than one filter, only the codes that all of
      // them can match are read
      bind_data.code_filter.Intersect(code_filter);
    } else {
      bind_data.code_filter = std::move(code_filter);
    }

    if (!described.empty()) {
      described += ", ";
    }
    described += PxDescribePushdown(get, filter_values);
  }

  // The optimizer can run more than once over the same scan, in which case the
  // filters are pushed down again and the description is replaced
  if (!described.empty()) {
    get.extra_info.file_filters = std::move(described);
  }
}

} // namespace duckdb
