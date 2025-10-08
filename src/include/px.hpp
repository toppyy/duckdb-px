#include <string>
#include <vector>
#include "variable.hpp"
#include "duckdb.hpp"
#include "utils.hpp"
#include "duckdb/common/exception.hpp"


enum class PxKeyword : uint8_t {
  UNKNOWN = 0,
  STUB = 1,
  HEADING = 2,
  VALUES = 3,
  CODES = 4,
  DATA = 5,
  DECIMALS = 6
};

struct PxFile {

public:
  PxFile();

  size_t variable_count;
  size_t observations;
  int decimals;

public:
  void AddVariable(std::string name);
  void AddVariableCodeCount(size_t code_count);
  size_t ParseMetadata(const char *data, size_t idx, size_t data_size);
  int GetDecimals();

  std::string GetValueForVariable(size_t var_idx, size_t row_idx);
  std::vector<std::string> &GetVariableCodes(size_t var_idx);
  std::vector<std::string> &GetVariableValues(size_t var_idx);
  Variable &GetVariable(size_t var_idx);

private:
  std::vector<Variable> variables;
};

/* Parser helpers */
std::string ISO88591toUTF8(std::string original_string);
size_t ParseList(const char *data, std::vector<std::string> &result,
                 char end = ';');
size_t FindVarName(const char *data, std::string &varname);
PxKeyword ParseKeyword(const char *data);

/* Parse specific keywords */
size_t ParseStubOrHeading(const char *data, PxFile &pxfile);
size_t ParseValues(const char *data, PxFile &pxfile);
size_t ParseCodes(const char *data, PxFile &pxfile);
size_t ParseDecimals(const char *data, int &decimals);
