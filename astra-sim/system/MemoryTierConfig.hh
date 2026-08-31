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

struct UcieLinkConfig {
    std::string id;
    std::vector<std::string> endpoints;
    uint32_t stack_count;
    uint64_t header_bytes;
    uint64_t latency_ns;
    nlohmann::json backend_config;
};

struct MovementBandwidthResourceConfig {
    std::string id;
    uint32_t stack_count;
    uint64_t latency_ns;
    nlohmann::json backend_config;
};

struct MovementPathSegmentConfig {
    std::string id;
    std::string kind;
    std::string resource_ref;
    std::string operation;
    std::string byte_rule;
};

struct MovementPathCapabilityConfig {
    std::string id;
    std::string engine_location;
    uint32_t engine_count;
    uint32_t max_priority_burst;
    uint32_t max_in_flight_page_movements;
    std::string timing_provenance;
    std::vector<MovementPathSegmentConfig> segments;
};

struct MemoryTierConfigSet {
    bool native;
    std::string manifest_digest;
    std::vector<MemoryTierConfig> tiers;
    std::vector<UcieLinkConfig> ucie_links;
    bool has_movement_paths = false;
    std::string selected_movement_path_id;
    std::vector<MovementPathCapabilityConfig> movement_path_capabilities;
    std::vector<MovementBandwidthResourceConfig>
        movement_bandwidth_resources;
};

MemoryTierConfigSet parse_memory_tier_config(const nlohmann::json& payload);
MemoryTierConfigSet load_memory_tier_config(const std::string& path);
std::string write_temporary_memory_backend_config(
    const nlohmann::json& payload);
void validate_et_manifest_digest(
    const std::string& expected, const std::string& observed);

}  // namespace AstraSim

#endif /* __MEMORY_TIER_CONFIG_HH__ */
