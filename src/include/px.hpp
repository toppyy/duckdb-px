
enum class PxKeyword : uint8_t  {
    UNKNOWN     = 0,
    STUB        = 1,
    HEADING     = 2,
    VALUES      = 3,
    CODES       = 4,
    DATA        = 5
};


struct Variable {

public:
    Variable(std::string p_name);

public:
    const std::string &GetName();
    std::vector<std::string>& GetCodes();
    std::vector<std::string>& GetValues();

    size_t CodeCount();
    size_t ValueCount();

    void SetRepetitionFactor(size_t p_rep_factor);
    std::string NextCode(size_t row_idx);

private:
    std::string name;
    std::vector<std::string> codes;
    std::vector<std::string> values;
    size_t repetition_factor;

};

struct PxFile {

public:    
    PxFile();

    size_t variable_count;
    size_t observations;

public:
    void AddVariable(std::string name);
    void AddVariableCodeCount(size_t code_count);

    std::string GetValueForVariable(size_t var_idx, size_t row_idx);
    std::vector<std::string>& GetVariableCodes(size_t var_idx);
    std::vector<std::string>& GetVariableValues(size_t var_idx);
    Variable& GetVariable(size_t var_idx);

private:
    std::vector<Variable> variables;

};


/* Parser helpers */
std::string ISO88591toUTF8(std::string original_string);
size_t ParseList(const char* data, std::vector<std::string> &result, char end = ';');
size_t FindVarName(const char* data, std::string &varname);
PxKeyword ParseKeyword(const char* data);

/* Parse specific keywords */
size_t ParseStubOrHeading(const char* data, PxFile &pxfile);
size_t ParseValues(const char* data, PxFile &pxfile);
size_t ParseCodes(const char* data, PxFile &pxfile);