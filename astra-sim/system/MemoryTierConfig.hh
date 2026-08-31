/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __MEMORY_TIER_CONFIG_HH__
#define __MEMORY_TIER_CONFIG_HH__

#include <cstdint>
#include <string>
#include <vector>

#include "extern/helper/json/json.hpp"

namespace AstraSim {

struct MemoryTierConfig {
    uint32_t tier_id;
    std::string tier_name;
    uint32_t num_devices;
    nlohmann::json backend_config;
};

struct MemoryTierConfigSet {
    bool native;
    std::string manifest_digest;
    std::vector<MemoryTierConfig> tiers;
};

MemoryTierConfigSet parse_memory_tier_config(const nlohmann::json& payload);
MemoryTierConfigSet load_memory_tier_config(const std::string& path);
std::string write_temporary_memory_backend_config(
    const nlohmann::json& payload);
void validate_et_manifest_digest(
    const std::string& expected, const std::string& observed);

}  // namespace AstraSim

#endif /* __MEMORY_TIER_CONFIG_HH__ */
