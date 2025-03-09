#define DUCKDB_EXTENSION_MAIN


#include "duckdb.hpp"
#include "duckdb/common/multi_file_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>


#include "px_extension.hpp"
#include "px.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include <iostream>

namespace duckdb {

struct PxReader;


struct PxOptions {

  explicit PxOptions() : file_options() {}

  void Serialize(Serializer &serializer) const {
    file_options.Serialize(serializer);
  }
  static PxOptions Deserialize(Deserializer &deserializer) {
    PxOptions options;
    options.file_options = MultiFileReaderOptions::Deserialize(deserializer);
    return options;
  }

  MultiFileReaderOptions file_options;
};


struct PxUnionData {

    string file_name;
    vector<string> names;
    vector<LogicalType> types;
    unique_ptr<PxReader> reader;
    PxOptions options;

    const string &GetFileName() { return file_name; }
};



struct PxReader {

    using UNION_READER_DATA = unique_ptr<PxUnionData>;

    AllocatedData allocated_data;
    LogicalType duckdb_type;
    vector<LogicalType> return_types;
    vector<unique_ptr<Vector>> read_vecs;
    vector<string> names;
    string filename;
    MultiFileReaderData reader_data;
    PxFile pxfile;
    PxOptions options;
    size_t data_offset;
    size_t data_size;
    size_t observations_read;
    const char* data;

    std::string value_type;



    bool IsWhiteSpace(char c) {
        if (c == 32)    return true;
        if (c == '\r')  return true;
        if (c == '\n')  return true;
        if (c == '\t')  return true;
        return false;
    }

    bool IsNumeric(char c) {
        return c >= '0' && c <= '9';
    }

    void SkipWhiteSpace() {
        while(data_offset < data_size) {
            if (!IsWhiteSpace(data[data_offset])) {
                break;
            }
            data_offset++;
        };
    }

    string GetNextValue() {
        string rtrn;
        while (data_offset < data_size) {
            rtrn += data[data_offset++];
            if (IsWhiteSpace(data[data_offset])) break;
        }
        SkipWhiteSpace();
        return rtrn;
    }


    void AssignValue(size_t variable, size_t out_idx, const string& val) {
        if (value_type == "float") {
            AssignFloatValue(variable, out_idx, val);
            return;
        }

        AssignIntegerValue(variable, out_idx, val);
    }

    void AssignFloatValue(size_t variable, size_t out_idx, const string& val) {
        float fval;

        try {
            fval = std::stof(val);
        } catch (const std::invalid_argument& e) {
            std::cerr << "Invalid argument: " << e.what() << std::endl;
        } catch (const std::out_of_range& e) {
            std::cerr << "Out of range: " << e.what() << std::endl;
        }
        FlatVector::GetData<float>(*read_vecs[variable])[out_idx] = fval;
    }

    void AssignIntegerValue(size_t variable, size_t out_idx, const string& val) {
        int32_t ival;        
        try {
            ival = std::stoi(val);
        } catch (const std::invalid_argument& e) {
            std::cerr << "Invalid argument: " << e.what() << std::endl;
        } catch (const std::out_of_range& e) {
            std::cerr << "Out of range: " << e.what() << std::endl;
        }
        FlatVector::GetData<int32_t>(*read_vecs[variable])[out_idx] = ival;
    }


    void Read(DataChunk &output, const vector<column_t> &column_ids) {

        if (observations_read >= pxfile.observations) {
            return;
        }

        // There are actually variables + 1 vectors in the output
        // pxfile.variable_count only counts for variables excl. "value"
        // which is always present
        column_t variables = pxfile.variable_count;
        idx_t out_idx = 0;
        string val;

        while (true) {
            for (size_t col_idx = 0; col_idx <= variables; col_idx++) {
                if (col_idx == variables) {
                    val = GetNextValue();
                    if (!IsNumeric(val[0])) { // TODO handle negative values
                        FlatVector::Validity(*read_vecs[variables]).SetInvalid(out_idx);
                        continue;
                    };
                    FlatVector::Validity(*read_vecs[variables]).SetValid(out_idx);
                    AssignValue(variables, out_idx, val);
                    continue;
                }
                FlatVector::GetData<string_t>(*read_vecs[col_idx])[out_idx] = StringVector::AddString(*read_vecs[col_idx], pxfile.GetValueForVariable(col_idx, observations_read));
            }

            out_idx++;
            observations_read++;
            if (observations_read >= pxfile.observations) {
                break;
            }

            if (out_idx == STANDARD_VECTOR_SIZE) {
                break;
            }
        }

        for (size_t i = 0; i <= variables; i++) {
            output.data[i].Reference(*read_vecs[i]);
        }

        output.SetCardinality(out_idx);

    }

    const string &GetFileName() { return filename; }

    const vector<string> &GetNames() { return names; }

    const vector<LogicalType> &GetTypes() { return return_types; }

    PxReader(ClientContext &context, const string filename_p, const PxOptions &options_p)
        : pxfile(), data_offset(0), data_size(0), data(nullptr), read_vecs(), return_types(), names(), observations_read(0), value_type("float")
    {
		filename = filename_p;
        auto &fs = FileSystem::GetFileSystem(context);
        if (!fs.FileExists(filename)) {
            throw InvalidInputException("PX-file %s not found", filename);
        }

        auto file = fs.OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
        allocated_data = Allocator::Get(context).Allocate(file->GetFileSize());
        auto n_read = file->Read(allocated_data.get(), allocated_data.GetSize());
        D_ASSERT(n_read == file->GetFileSize());

        /* Parse column types */
        data_size = file->GetFileSize();
        data = const_char_ptr_cast(allocated_data.get());

        size_t idx = 0;

        int decimals = 3;

        PxKeyword current_keyword = PxKeyword::UNKNOWN;

        do {
            while (IsWhiteSpace(data[idx])) {
                idx++;
            };

            current_keyword = ParseKeyword(data + idx);

            if (current_keyword == PxKeyword::UNKNOWN) {
                // The keyword did not match, fast-forward to next keyword
                // so that "DECIMALS" is not found within "SHOWDECIMALS", for example
                while (data[idx++] != ';') {
                    if (idx >= data_size) {
                        throw BinderException("Reached EOF when parsing keywords");
                        return;
                    }
                };
                idx++;
                continue;
            }

            if (current_keyword == PxKeyword::DATA) break;

            if (
                ( current_keyword == PxKeyword::STUB ) ||
                ( current_keyword == PxKeyword::HEADING )
            ) {
                idx += ParseStubOrHeading(data + idx, pxfile);
                continue;
            }

            if (current_keyword == PxKeyword::DECIMALS) {
                idx += ParseDecimals(data + idx, decimals);
                continue;
            }


            if (current_keyword == PxKeyword::VALUES) {
                idx += ParseValues(data + idx, pxfile);
                continue;
            }


            if (current_keyword == PxKeyword::CODES) {
                idx += ParseCodes(data + idx, pxfile);
                continue;
            }

            idx++;

        } while(true);

        // idx points to DATA= after the loop
        // so we can skip 'DATA=' by incrementing idx
        idx += 5;
        data_offset = idx;
        SkipWhiteSpace();


        // Add variables
        for (size_t i = 0; i < pxfile.variable_count; i++) {

            Variable& var = pxfile.GetVariable(i);

            if (var.CodeCount() != var.ValueCount()) {
                throw BinderException("Number of VALUES and CODES do not match!");
            }

            read_vecs.push_back( make_uniq<Vector>(LogicalType::VARCHAR) );
            return_types.push_back(LogicalType::VARCHAR);
            names.push_back(var.GetName());
        }

        size_t repetition_factor = 1, col_idx = pxfile.variable_count - 1;
        for (size_t i = 0; i < pxfile.variable_count; i++) {
            pxfile.GetVariable(col_idx).SetRepetitionFactor(repetition_factor);
            repetition_factor *= pxfile.GetVariable(col_idx).CodeCount();
            col_idx--;
        }

        // Variable(s) for values
        names.push_back("value");
        if (decimals > 0) {
            value_type = "float";
            read_vecs.push_back( make_uniq<Vector>(LogicalType::FLOAT) );
            return_types.push_back(LogicalType::FLOAT);
            return;
        }

        value_type = "int";
        read_vecs.push_back( make_uniq<Vector>(LogicalType::INTEGER) );
        return_types.push_back(LogicalType::INTEGER);
        
    }

};


struct PxBindData : FunctionData {

    string file;
    vector<string> names;
    vector<LogicalType> types;
    PxOptions options;
    shared_ptr<PxReader> reader;

    void Initialize(shared_ptr<PxReader> p_reader) {
        reader = std::move(p_reader);
        options = p_reader->options;
    }

    void Initialize(ClientContext &, shared_ptr<PxReader> reader) {
        Initialize(reader);
    }

    bool Equals(const FunctionData &other_p) const override {
        D_ASSERT(false); // FIXME
        return false;
    }

    unique_ptr<FunctionData> Copy() const override {
        D_ASSERT(false); // FIXME
        return nullptr;
    }

};



static unique_ptr<FunctionData> PxBindFunction(ClientContext &context, TableFunctionBindInput &input,
                           vector<LogicalType> &return_types, vector<string> &names
) {
    auto &filename = input.inputs[0];
    auto result = make_uniq<PxBindData>();


    for (auto &kv : input.named_parameters) {
        if (kv.second.IsNull()) {
            throw BinderException("Cannot use NULL as function argument");
        }
        auto loption = StringUtil::Lower(kv.first);
        throw InternalException("Unrecognized option %s", loption.c_str());
    }

    auto opts = PxOptions();

    result->reader = make_shared_ptr<PxReader>(context, filename.ToString(), opts);

    return_types = result->reader->return_types;
    names = result->reader->names;

    result->types = return_types;
    result->names = names;

    return std::move(result);


};


struct PxGlobalState : GlobalTableFunctionState {
  mutex lock;

  shared_ptr<PxReader> reader;
  vector<column_t> column_ids;
  optional_ptr<TableFilterSet> filters;
};



static void PxTableFunction(ClientContext &context, TableFunctionInput &data,
                              DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<PxBindData>();
	auto &global_state = data.global_state->Cast<PxGlobalState>();

	do {
	output.Reset();
	global_state.reader->Read(output, global_state.column_ids);

	if (output.size() > 0) {
		return;
	}

	break;

	} while (true);

}


unique_ptr<GlobalTableFunctionState> PxGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto global_state_result = make_uniq<PxGlobalState>();
	auto &global_state = *global_state_result;
	auto &bind_data = input.bind_data->Cast<PxBindData>();

	global_state.column_ids = input.column_ids;
	global_state.filters = input.filters;


	D_ASSERT(bind_data.reader != NULL);
	global_state.reader = bind_data.reader;

	return std::move(global_state_result);
};



static void LoadInternal(DatabaseInstance &instance) {

    // Register table function
    auto px_table_function = TableFunction("read_px", {LogicalType::VARCHAR},  PxTableFunction,
                    PxBindFunction,  PxGlobalInit);

    ExtensionUtil::RegisterFunction(instance, px_table_function);


}

void PxExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string PxExtension::Name() {
	return "px";
}

std::string PxExtension::Version() const {
#ifdef EXT_VERSION_PX
	return EXT_VERSION_PX;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void px_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::PxExtension>();
}

DUCKDB_EXTENSION_API const char *px_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif

