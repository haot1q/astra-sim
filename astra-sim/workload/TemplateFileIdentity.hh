/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TEMPLATE_FILE_IDENTITY_HH__
#define __TEMPLATE_FILE_IDENTITY_HH__

#include <cstdint>
#include <string>

namespace AstraSim {

// A cached template document must keep the exact file it was parsed from. The
// identity is compared on every reuse so a rewritten path fails closed instead
// of silently executing a stale plan.
struct TemplateFileIdentity {
    uint64_t device = 0;
    uint64_t inode = 0;
    uint64_t size = 0;
    int64_t modified_seconds = 0;
    int64_t modified_nanoseconds = 0;

    bool operator==(const TemplateFileIdentity& other) const;
};

TemplateFileIdentity read_template_file_identity(const std::string& path);

}  // namespace AstraSim

#endif
