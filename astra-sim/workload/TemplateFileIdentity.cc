/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/TemplateFileIdentity.hh"

#include <stdexcept>
#include <sys/stat.h>

namespace AstraSim {

bool TemplateFileIdentity::operator==(const TemplateFileIdentity& other) const {
    return device == other.device && inode == other.inode &&
           size == other.size && modified_seconds == other.modified_seconds &&
           modified_nanoseconds == other.modified_nanoseconds;
}

TemplateFileIdentity read_template_file_identity(const std::string& path) {
    struct stat file_status {};
    if (stat(path.c_str(), &file_status) != 0 || !S_ISREG(file_status.st_mode)) {
        throw std::invalid_argument(
            "template document path must identify a readable regular file");
    }
#if defined(__APPLE__)
    const auto modified_seconds = file_status.st_mtimespec.tv_sec;
    const auto modified_nanoseconds = file_status.st_mtimespec.tv_nsec;
#else
    const auto modified_seconds = file_status.st_mtim.tv_sec;
    const auto modified_nanoseconds = file_status.st_mtim.tv_nsec;
#endif
    return {static_cast<uint64_t>(file_status.st_dev),
            static_cast<uint64_t>(file_status.st_ino),
            static_cast<uint64_t>(file_status.st_size),
            static_cast<int64_t>(modified_seconds),
            static_cast<int64_t>(modified_nanoseconds)};
}

}  // namespace AstraSim
