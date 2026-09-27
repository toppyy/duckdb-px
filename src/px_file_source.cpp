#include "px_file_source.hpp"

namespace duckdb {

PxFileSource ReadPxFile(ClientContext &context, const string &filename) {
  auto &fs = FileSystem::GetFileSystem(context);
  if (!fs.FileExists(filename)) {
    throw InvalidInputException("PX-file %s not found", filename);
  }

  auto file = fs.OpenFile(filename, FileOpenFlags::FILE_FLAGS_READ);
  auto fsize = file->GetFileSize();
  if (fsize == 0) {
    throw BinderException("PX-file %s is empty", filename);
  }

  PxFileSource source;
  try {
    source.allocated_data = Allocator::Get(context).Allocate(fsize);
  } catch (const Exception &ex) {
    throw BinderException(
        "Failed to allocate memory for PX-file %s (%llu bytes): %s", filename,
        (unsigned long long)fsize, ex.what());
  }
  idx_t n_read = 0;
  try {
    n_read = file->Read(source.allocated_data.get(),
                        source.allocated_data.GetSize());
  } catch (const Exception &ex) {
    throw InvalidInputException("Failed to read PX-file %s: %s", filename,
                                ex.what());
  }
  if (n_read != (idx_t)fsize) {
    throw InvalidInputException(
        "Failed to read PX-file %s (read %llu of %llu bytes)", filename,
        (unsigned long long)n_read, (unsigned long long)fsize);
  }

  source.data = const_char_ptr_cast(source.allocated_data.get());
  source.size = fsize;
  return source;
}

} // namespace duckdb
