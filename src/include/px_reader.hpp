#pragma once

#include "px_file.hpp"
#include "px_code_filter.hpp"
#include "utils.hpp"

#include "duckdb.hpp"

#include <mutex>

namespace duckdb {

//! The reader of a single PX file. It owns the bytes of the file, the schema
//! that the metadata of the file describes and the vectors that the
//! observations are materialized into.
struct PxReader {

  AllocatedData allocated_data;
  vector<LogicalType> return_types;
  vector<unique_ptr<Vector>> read_vecs;
  vector<string> names;
  PxFile pxfile;
  size_t data_offset;
  size_t data_size;
  size_t observations_read;
  const char *data;
  std::mutex read_lock;

  std::string value_type;

  StringView GetNextValue();

  void AssignValue(size_t variable, size_t out_idx, StringView val);

  void AssignFloatValue(size_t variable, size_t out_idx, StringView val);

  void AssignIntegerValue(size_t variable, size_t out_idx, StringView val);

  //! Move the reader to the next observation without materializing it
  void SkipObservation();

  void Read(DataChunk &output, const PxCodeFilter &code_filter);

  PxReader(ClientContext &context, const string filename);
};

} // namespace duckdb
