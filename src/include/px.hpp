using std::string;
using std::vector;


enum class PxKeyword : uint8_t  {
    UNKNOWN     = 0,
    STUB        = 1,
    HEADING     = 2,
    VALUES      = 3,
    CODES       = 4,
    DATA        = 5
};


struct Variable {
    string name;
    vector<string> codes;
    vector<string> values;
    size_t repetition_factor;

    Variable(string p_name);

    const string &GetName();

    size_t CodeCount();
    size_t ValueCount();

    string NextCode(size_t row_idx);
    void SetRepetitionFactor(size_t p_rep_factor);

};

struct PxFile {
    size_t variable_count;
    size_t observations;
    vector<Variable> variables;
    
    PxFile();

    void AddVariable(string name);
    string GetValueForVariable(size_t var_idx, size_t row_idx);
    void AddVariableCodeCount(size_t code_count);

};


/* Parser helpers */
string ISO88591toUTF8(string original_string);
size_t ParseList(const char* data,vector<string> &result, char end = ';');
size_t FindVarName(const char* data, string &varname);
PxKeyword ParseKeyword(const char* data);

/* Parse specific keywords */
size_t ParseStubOrHeading(const char* data, PxFile &pxfile);
size_t ParseValues(const char* data, PxFile &pxfile);
size_t ParseCodes(const char* data, PxFile &pxfile);