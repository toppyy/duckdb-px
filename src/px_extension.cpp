#define DUCKDB_EXTENSION_MAIN
#include "px_extension.hpp"
#include "px_filter_pushdown.hpp"
#include "px_metadata.hpp"
#include "px_scan.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {

  // Register table function
  TableFunction px_table_function("read_px", {LogicalType::VARCHAR},
                                  PxTableFunction, PxBindFunction,
                                  PxGlobalInit);
  // Filter pushdown is not declared as supported: the scan only uses the
  // filters to skip over observations that can not match, so DuckDB keeps
  // applying the filters to the rows that the scan returns
  px_table_function.pushdown_complex_filter = PxPushdownComplexFilter;
  CreateTableFunctionInfo info(px_table_function);
  FunctionDescription desc;
  desc.parameter_names = {"file"};
  desc.description =
      "Reads a PX (PC-Axis) file and returns its contents as a table with "
      "STUB/HEADING variables as VARCHAR columns and a 'value' column (INTEGER "
      "if DECIMALS=0, otherwise FLOAT).";
  desc.examples = {
      "SELECT * FROM read_px('test/data/statfin_vaerak_pxt_11rc.px');"};
  desc.categories = {"Scan"};
  info.descriptions.push_back(std::move(desc));
  info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));

  TableFunction px_metadata_function("read_px_metadata", {LogicalType::VARCHAR},
                                     PxMetadataFunction, PxMetadataBindFunction,
                                     PxMetadataGlobalInit);
  CreateTableFunctionInfo meta_info(px_metadata_function);
  FunctionDescription meta_desc;
  meta_desc.parameter_names = {"file"};
  meta_desc.description =
      "Reads metadata from a PX (PC-Axis) file and returns the available "
      "CODE-VALUE pairs per variable without scanning the DATA section. "
      "Returns columns: 'variable' (VARCHAR), 'code' (VARCHAR), 'value' "
      "(VARCHAR, NULL if no VALUES entry exists).";
  meta_desc.examples = {
      "SELECT * FROM read_px_metadata('test/data/statfin_vaerak_pxt_11rc.px');",
      "SELECT * FROM read_px_metadata('test/data/statfin_vaerak_pxt_11rc.px') "
      "WHERE variable='Sukupuoli';"};
  meta_desc.categories = {"Scan"};
  meta_info.descriptions.push_back(std::move(meta_desc));
  meta_info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  loader.RegisterFunction(std::move(meta_info));
};

void PxExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }
std::string PxExtension::Name() { return "px"; }

std::string PxExtension::Version() const {
#ifdef EXT_VERSION_PX
  return EXT_VERSION_PX;
#else
  return "";
#endif
}

}; // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(px, loader) { duckdb::LoadInternal(loader); }
}
