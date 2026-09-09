/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __PD_PATH_WORK_HH__
#define __PD_PATH_WORK_HH__

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "astra-sim/system/memory/PdPathServiceConfig.hh"

namespace AstraSim {

struct PdPathSlice {
    uint64_t id;
    uint64_t range_index;
    uint32_t source_rank;
    uint32_t destination_rank;
    uint64_t offset;
    uint64_t range_offset;
    uint64_t bytes;
};

// Immutable static work. Runtime Page/registration leases remain Python-owned.
struct PdPathWork {
    nlohmann::json descriptor;
    nlohmann::json context;
    nlohmann::json binding;
    nlohmann::json profile;
    nlohmann::json route;
    std::string digest;
    std::string descriptor_digest;
    std::vector<PdPathSlice> slices;
};

PdPathWork load_pd_path_work(const std::string& path,
                            const PhysicalServiceConfig& physical,
                            const PdPathServiceConfig& interfaces);

// Recompute the common partition and bounds; no queue or Page state is changed.
std::vector<PdPathSlice> validate_pd_path_ranges(const nlohmann::json& body,
                                              uint64_t quantum);

}  // namespace AstraSim
#endif
