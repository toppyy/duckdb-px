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


    void Read(DataChunk &output, const vector<column_t> &column_ids) {


        if (observations_read >= pxfile.observations) {
            // observations_read = 0;
            return;
        }

        idx_t out_idx = 0;
        column_t variables = pxfile.variable_count, col_idx = 0;

        string val;
        float fval;

        while (true) {
            for (size_t i = 0; i <= variables; i++) {
                col_idx = i;
                if (col_idx == variables) {
                    val = GetNextValue();
                    if (!IsNumeric(val[0])) {
                        FlatVector::Validity(*read_vecs[variables]).SetInvalid(out_idx);
                        continue;
                    };
                    try {
                        fval = std::stof(val);
                    } catch (const std::invalid_argument& e) {
                        std::cerr << "Invalid argument: " << e.what() << std::endl;
                    } catch (const std::out_of_range& e) {
                        std::cerr << "Out of range: " << e.what() << std::endl;
                    }
                    FlatVector::GetData<float>(*read_vecs[variables])[out_idx] = fval;
                    continue;
                }
                FlatVector::GetData<string_t>(*read_vecs[col_idx])[out_idx] = pxfile.GetValueForVariable(col_idx);
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
        : pxfile(), data_offset(0), data_size(0), data(nullptr), read_vecs(), return_types(), names(), observations_read(0)
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

        PxKeyword current_keyword = PxKeyword::UNKNOWN;

        do {
            char c = data[idx];

            current_keyword = ParseKeyword(data + idx);

            if (current_keyword == PxKeyword::DATA) break;

            if (
                ( current_keyword == PxKeyword::STUB ) ||
                ( current_keyword == PxKeyword::HEADING )
            ) {
                idx += ParseStubOrHeading(data + idx, pxfile);
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

            Variable& var = pxfile.variables[i];

            if (var.CodeCount() != var.ValueCount()) {
                throw BinderException("Number of VALUES and CODES do not match!");
            }

            read_vecs.push_back( make_uniq<Vector>(LogicalType::VARCHAR) );

            return_types.push_back(LogicalType::VARCHAR);
            names.push_back(var.GetName());
        }

        size_t repetition_factor = 1, col_idx = pxfile.variable_count - 1;
        for (size_t i = 0; i < pxfile.variable_count; i++) {
            pxfile.variables[col_idx].SetRepetitionFactor(repetition_factor);
            repetition_factor *= pxfile.variables[col_idx].CodeCount();
            col_idx--;
        }

        // Variable(s) for values
        read_vecs.push_back( make_uniq<Vector>(LogicalType::FLOAT) );
        return_types.push_back(LogicalType::FLOAT);
        names.push_back("value");
    }

    static unique_ptr<PxUnionData> StoreUnionReader(unique_ptr<PxReader> scan_p, idx_t file_idx) {
        auto data = make_uniq<PxUnionData>();
        data->file_name = scan_p->GetFileName();
        data->options = scan_p->options;
        data->names = scan_p->GetNames();
        data->types = scan_p->GetTypes();
        data->reader = std::move(scan_p);

        return data;
    }

};


struct PxBindData : FunctionData {

    shared_ptr<MultiFileList> file_list;
    unique_ptr<MultiFileReader> multi_file_reader;
    MultiFileReaderBindData reader_bind;
    vector<string> names;
    vector<LogicalType> types;
    PxOptions options;
    shared_ptr<PxReader> initial_reader;
    vector<unique_ptr<PxUnionData>> union_readers;

    void Initialize(shared_ptr<PxReader> reader) {
        initial_reader = std::move(reader);
        options = initial_reader->options;
    }

    void Initialize(ClientContext &, shared_ptr<PxReader> reader) {
        Initialize(reader);
    }

    void Initialize(ClientContext &, unique_ptr<PxUnionData> &union_data) {
        Initialize(std::move(union_data->reader));
        names = union_data->names;
        types = union_data->types;
        options = union_data->options;
        initial_reader = std::move(union_data->reader);
    }

    bool Equals(const FunctionData &other_p) const override {
        D_ASSERT(false); // FIXME
        return false;
    }

    unique_ptr<FunctionData> Copy() const override {
        D_ASSERT(false); // FIXME
        //   auto bind_data = make_uniq<AvroBindData>();
        return nullptr;
    }

};



static unique_ptr<FunctionData> PxBindFunction(ClientContext &context, TableFunctionBindInput &input,
                           vector<LogicalType> &return_types, vector<string> &names
) {
    auto &filename = input.inputs[0];
    auto result = make_uniq<PxBindData>();
    result->multi_file_reader = MultiFileReader::Create(input.table_function);

    for (auto &kv : input.named_parameters) {
        if (kv.second.IsNull()) {
            throw BinderException("Cannot use NULL as function argument");
        }
        auto loption = StringUtil::Lower(kv.first);
        // if (result->multi_file_reader->ParseOption(
        //         kv.first, kv.second, result->avro_options.file_options, context)) {
        //     continue;
        // }
        throw InternalException("Unrecognized option %s", loption.c_str());
    }

    result->file_list =
        result->multi_file_reader->CreateFileList(context, filename);


    auto opts = PxOptions();

    result->reader_bind = result->multi_file_reader->BindReader<PxReader>(context, result->types, result->names, *result->file_list, *result, opts);


    return_types = result->types;
    names = result->names;

    return std::move(result);


};


struct PxGlobalState : GlobalTableFunctionState {
  mutex lock;

  MultiFileListScanData scan_data;
  shared_ptr<PxReader> reader;

  vector<column_t> column_ids;
  optional_ptr<TableFilterSet> filters;
};




static bool PxNextFile(ClientContext &context, const PxBindData &bind_data,
                         PxGlobalState &global_state,
                         shared_ptr<PxReader> initial_reader) {
  unique_lock<mutex> parallel_lock(global_state.lock);

  string file;
  if (!bind_data.file_list->Scan(global_state.scan_data, file)) {
    return false;
  }

  // re-use initial reader for first file, no need to parse metadata again
  if (initial_reader) {
    D_ASSERT(file == initial_reader->filename);
    global_state.reader = initial_reader;
  } else {
    global_state.reader =
        make_shared_ptr<PxReader>(context, file, bind_data.options);
  }

  bind_data.multi_file_reader->InitializeReader(
      *global_state.reader, bind_data.options.file_options,
      bind_data.reader_bind, bind_data.types, bind_data.names,
      global_state.column_ids, global_state.filters, file, context, nullptr);
  return true;
}


static void PxTableFunction(ClientContext &context, TableFunctionInput &data,
                              DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<PxBindData>();
  auto &global_state = data.global_state->Cast<PxGlobalState>();
  do {
    output.Reset();
    global_state.reader->Read(output, global_state.column_ids);
    bind_data.multi_file_reader->FinalizeChunk(context, bind_data.reader_bind,
                                               global_state.reader->reader_data,
                                               output, nullptr);


    if (output.size() > 0) {
      return;
    }

    if (!PxNextFile(context, bind_data, global_state, nullptr)) {
      return;
    }
  } while (true);

}


unique_ptr<GlobalTableFunctionState> PxGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
  auto global_state_result = make_uniq<PxGlobalState>();
  auto &global_state = *global_state_result;
  auto &bind_data = input.bind_data->Cast<PxBindData>();

  global_state.column_ids = input.column_ids;
  global_state.filters = input.filters;

  bind_data.file_list->InitializeScan(global_state.scan_data);

  if (!PxNextFile(context, bind_data, global_state, bind_data.initial_reader)) {
    throw InternalException("Cannot scan PX-files!");
  }

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

