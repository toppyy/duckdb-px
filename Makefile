PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=px
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# The unittest binary is dynamically linked against the libduckdb shared library
# built alongside it. LD_LIBRARY_PATH takes precedence over the binary's rpath, so
# an installed libduckdb (e.g. /usr/local/lib) would otherwise be picked up and the
# tests fail with unresolved symbols. Point the tests at the build output.
test_release_internal: export LD_LIBRARY_PATH := $(PROJ_DIR)build/release/src:$(LD_LIBRARY_PATH)
test_debug_internal: export LD_LIBRARY_PATH := $(PROJ_DIR)build/debug/src:$(LD_LIBRARY_PATH)
test_reldebug_internal: export LD_LIBRARY_PATH := $(PROJ_DIR)build/reldebug/src:$(LD_LIBRARY_PATH)

PERF_REFS ?=
PERF_OUT ?=

.PHONY: perf
perf:
	@python3 scripts/perf.py $(PERF_REFS) $(if $(PERF_OUT),--out $(PERF_OUT))