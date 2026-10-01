PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=px
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

PERF_REFS ?=
PERF_OUT ?=

.PHONY: perf
perf:
	@python3 scripts/perf.py $(PERF_REFS) $(if $(PERF_OUT),--out $(PERF_OUT))