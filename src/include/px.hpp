#include <string>
#include <vector>
#include <iostream>
#include "duckdb/common/exception.hpp"


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



PxKeyword ParseKeyword(const char* data) {

    if (std::strncmp(data, "STUB=", 5) == 0) {
        return PxKeyword::STUB;
    }

    if (std::strncmp(data, "HEADING=", 8) == 0) {
        return PxKeyword::HEADING;
    }

    if (std::strncmp(data, "VALUES(", 7) == 0) {
        return PxKeyword::VALUES;
    }

    if (std::strncmp(data, "CODES(", 6) == 0) {
        return PxKeyword::CODES;
    }

    if (std::strncmp(data, "DATA=", 5) == 0) {
        return PxKeyword::DATA;
    }


    return PxKeyword::UNKNOWN;

};

struct Variable {
    string name;
    vector<string> codes;
    vector<string> values;

    size_t code_iterator;

    ~Variable() {
        std::cout << "Variable Destructor called for " << name << "\n";
    };


    Variable(string p_name) :  name(p_name), code_iterator(0), codes(), values() {
        std::cout << "Created variable " << name << ", " << code_iterator << "\n";
    };



    const string &GetName() { return name; };

    size_t CodeCount() { return codes.size(); };
    size_t ValueCount() { return values.size(); };

    bool NextCode() {
        if (code_iterator < (codes.size()-1)) {
            code_iterator++;
            return true;
        }
        code_iterator = 0;
        return false;
    }

    string CurrentCode() {
        return codes[code_iterator];
    }


};

struct PxFile {
    size_t variable_count;
    vector<Variable> variables;


    PxFile() : variable_count(0), variables() {
        variables.reserve(10);
    }


    ~PxFile() {
        std::cout << "PxFile Destructor called\n";
    };

    void AddVariable(string name) {
        variable_count++;
        variables.emplace_back(name);
    }

    void IncrementVariableCode(size_t var_idx) {

        Variable& var = variables[var_idx];

        if (var_idx == 0) {
            if (var.NextCode()) {
                return;
            }
            throw duckdb::InternalException("Iterated through all codes for all variables");
        }

        if (!variables[var_idx].NextCode()) {
            IncrementVariableCode(var_idx - 1);
            return;
        }
    }

    string GetValueForVariable(size_t var_idx) {
        string rtrn = variables[var_idx].CurrentCode();
        return rtrn;
    }

};

size_t ParseList(const char* data,vector<string> &result, char end = ';') {
    // Expects a string which contains a list of quoted elements
    // Eg. "monkey","island","is","cool"

    size_t idx = 0;
    bool quote_open = false;
    char c = end;
    string element;

    while ((c = data[idx]) != end) {
        idx++;
        if (c == '"') {
            if (quote_open) result.push_back(element);
            element = "";
            quote_open = !quote_open;
            continue;
        }
        element.push_back(c);
    }
    return idx;
}



size_t ParseStubOrHeading(const char* data, PxFile &pxfile) {
    // Returns the number of variables listed in a STUB-declaration
    // for example STUB="var1","var2";

    vector<string> varnames;
    size_t inc = ParseList(data, varnames);

    for (auto name : varnames) {
        pxfile.AddVariable(name);
    }
    return inc;
}


size_t FindVarName(const char* data, string &varname) {
    // The variable is specified within brackets and quotes
    // for example:
    //      VALUES("somevariable")="val1","val2";
    //      CODES("somevariable")="val1","val2";

    size_t idx = 0;
    char c = 0;

    while ((c = data[idx++]) != '"') {};

    while ((c = data[idx++]) != '"') {
        varname.push_back(c);
    };


    return idx;

}

size_t ParseValues(const char* data, PxFile &pxfile) {
    // Values is a list of values associated with a variable

    string varname;
    size_t idx = FindVarName(data, varname);

    size_t var_idx = 0;
    bool var_found = false;
    char c = 0;

    while (var_idx < pxfile.variable_count) {
        if (pxfile.variables[var_idx].GetName() == varname) {
            var_found = true;
            break;
        }
        var_idx++;
    }

    if (!var_found) throw duckdb::BinderException("Values specified for a variable not found in STUB/HEADING");

    idx += ParseList(data + idx, pxfile.variables[var_idx].values);

    return idx;
}


size_t ParseCodes(const char* data, PxFile &pxfile) {
    // Codes is a list of values associated with a variable

    string varname;
    size_t idx = FindVarName(data, varname);

    size_t var_idx = 0;
    bool var_found = false;
    char c = 0;

    while (var_idx < pxfile.variable_count) {
        if (pxfile.variables[var_idx].GetName() == varname) {
            var_found = true;
            break;
        }
        var_idx++;
    }

    if (!var_found) throw duckdb::BinderException("Codes specified for a variable not found in STUB/HEADING");

    idx += ParseList(data + idx, pxfile.variables[var_idx].codes);

    return idx;
}
