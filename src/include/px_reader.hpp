#pragma once

#include "px_file.hpp"
#include "px_code_filter.hpp"
#include "utils.hpp"

#include "duckdb.hpp"

#include <mutex>

namespace duckdb {

//! The type of the "value" column, guessed from the DECIMALS keyword
enum class PxValueType : uint8_t { INTEGER, FLOAT };

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

  PxValueType value_type;

  StringView GetNextValue();

  void AssignValue(size_t variable, size_t out_idx, StringView val);

  void AssignFloatValue(size_t variable, size_t out_idx, StringView val);

  void AssignIntegerValue(size_t variable, size_t out_idx, StringView val);

  //! Move the reader to the next observation without materializing it
  void SkipObservation();

  //! Move the reader n observations without materializing them
  void SkipObservations(size_t n);

  void Read(DataChunk &output, const PxCodeFilter &code_filter);

  PxReader(ClientContext &context, const string filename);

  //! Add the column that holds the CODE of a variable: a dictionary of the
  //! CODES of the variable
  void AddVariableColumn(Variable &var);

  //! Tell every variable how often its CODES repeat, see below
  void SetRepetitionFactors();

  //! Add the column that holds the observations of the DATA keyword. Its type
  //! is a guess based on the DECIMALS keyword
  void AddValueColumn(int decimals);
};

} // namespace duckdb
