#include <string>
#include <vector>
#include <iostream>
#include "duckdb/common/exception.hpp"
#include "duckdb.hpp"
#include "px.hpp"

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

Variable::Variable(std::string p_name) :  name(p_name), repetition_factor(0), codes(), values() {};

const std::string& Variable::GetName() { return name; };

size_t Variable::CodeCount() { return codes.size(); };
size_t Variable::ValueCount() { return values.size(); };

std::string Variable::NextCode(size_t row_idx) {
    size_t i = row_idx % (repetition_factor * codes.size());
    return codes[i / repetition_factor];
}
    
void Variable::SetRepetitionFactor(size_t p_rep_factor) {
    repetition_factor = p_rep_factor;
}

std::vector<std::string>& Variable::GetCodes() {
    return codes;
}

std::vector<std::string>& Variable::GetValues() {
    return values;
}


std::string ISO88591toUTF8(std::string original_string) {
    std::string rtrn;
    for (int i = 0; i < original_string.size(); i++) {

        switch(original_string[i]) {
                case static_cast<char>(0xe4): // ä
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0xA4);
                    break;
                case static_cast<char>(0xf6): // ö
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0xB6);
                    break;
                case static_cast<char>(0xe5): // å
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0xA5);
                    break;
                case static_cast<char>(0xC4): // Ä
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0x84);
                    break;
                case static_cast<char>(0xD6): // Ö
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0x96);
                    break;
                case static_cast<char>(0xC5): // Å
                    rtrn += static_cast<char>(0xC3);
                    rtrn += static_cast<char>(0x85);
                    break;
                default:
                   rtrn += original_string[i];
        }
    }
    return rtrn;
}

PxFile::PxFile() : variable_count(0), variables(), observations(1) {
    variables.reserve(10);
}

void PxFile::AddVariable(std::string name) {
    variable_count++;
    variables.emplace_back(name);
}

std::string PxFile::GetValueForVariable(size_t var_idx, size_t row_idx) {
    return variables[var_idx].NextCode(row_idx);
}

void PxFile::AddVariableCodeCount(size_t code_count) {
    observations *= code_count;
}

std::vector<std::string>& PxFile::GetVariableCodes(size_t var_idx) {
    return variables[var_idx].GetCodes();    
}

std::vector<std::string>& PxFile::GetVariableValues(size_t var_idx) {
    return variables[var_idx].GetValues();    
}
 
 Variable& PxFile::GetVariable(size_t var_idx) {
    return variables[var_idx];
 }



size_t ParseList(const char* data, std::vector<std::string> &result, char end) {
    // Expects a string which contains a list of quoted elements
    // Eg. "monkey","island","is","cool"

    size_t idx = 0;
    bool quote_open = false;
    char c = end;
    std::string element = "";

    while ((c = data[idx]) != end) {
        idx++;
        if (c == '"') {
            if (quote_open) result.push_back(ISO88591toUTF8(element));
            element = "";
            quote_open = !quote_open;
            continue;
        }
        element.push_back(c);
    }
    return idx;
}

size_t FindVarName(const char* data, std::string &varname) {
    // The variable is specified within brackets and quotes
    // for example:
    //      VALUES("somevariable")="val1","val2";
    //      CODES("somevariable")="val1","val2";

    size_t idx = 0;
    char c = 0;


    while ((c = data[idx++]) != '"') {};

    std::string tmp;
    while ((c = data[idx++]) != '"') {
        tmp.push_back(c);
    };

    varname = ISO88591toUTF8(tmp);

    return idx;

}

size_t ParseStubOrHeading(const char* data, PxFile &pxfile) {
    // Returns the number of variables listed in a STUB-declaration
    // for example STUB="var1","var2";

    std::vector<std::string> varnames;
    size_t inc = ParseList(data, varnames);

    for (auto name : varnames) {
        pxfile.AddVariable(name);
    }
    return inc;
}




size_t ParseValues(const char* data, PxFile &pxfile) {
    // Values is a list of values associated with a variable

    std::string varname;
    size_t idx = FindVarName(data, varname);

    size_t var_idx = 0;
    bool var_found = false;
    char c = 0;

    while (var_idx < pxfile.variable_count) {
        if (pxfile.GetVariable(var_idx).GetName() == varname) {
            var_found = true;
            break;
        }
        var_idx++;
    }

    if (!var_found) throw duckdb::BinderException("Values specified for a variable not found in STUB/HEADING");

    idx += ParseList(data + idx, pxfile.GetVariableValues(var_idx));

    return idx;
}


size_t ParseCodes(const char* data, PxFile &pxfile) {
    // Codes is a list of values associated with a variable

    std::string varname;
    size_t idx = FindVarName(data, varname);

    size_t var_idx = 0;
    bool var_found = false;
    char c = 0;

    while (var_idx < pxfile.variable_count) {
        if (pxfile.GetVariable(var_idx).GetName() == varname) {
            var_found = true;
            break;
        }
        var_idx++;
    }

    if (!var_found) throw duckdb::BinderException("Codes specified for a variable not found in STUB/HEADING");

    idx += ParseList(data + idx, pxfile.GetVariableCodes(var_idx));

    pxfile.AddVariableCodeCount(pxfile.GetVariable(var_idx).CodeCount());

    return idx;
}
