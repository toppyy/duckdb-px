#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! The bytes of a PX file, read into memory. The metadata of the file still
//! has to be parsed out of them.
struct PxFileSource {
  //! Keeps the bytes alive for as long as they are read from
  AllocatedData allocated_data;
  const char *data = nullptr;
  idx_t size = 0;
};

//! Read a whole PX file into memory. Throws when the file does not exist, is
//! empty, or can not be read completely.
PxFileSource ReadPxFile(ClientContext &context, const string &filename);

} // namespace duckdb
