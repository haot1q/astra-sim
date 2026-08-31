/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>

#include "astra-sim/system/MemoryTierConfig.hh"

namespace {

using json = nlohmann::json;

template <typename Fn>
bool throws_invalid_argument(Fn fn) {
    try {
        fn();
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

json tier(uint32_t id, const char* name, std::size_t devices = 1) {
    json device_entries = json::array();
    for (std::size_t device_id = 0; device_id < devices; ++device_id) {
        device_entries.push_back({
            {"device_id", device_id}, {"capacity_bytes", 1024 + device_id}});
    }
    return {
        {"tier_id", id},
        {"tier_name", name},
        {"backend_kind", "analytical"},
        {"scope", "instance"},
        {"pool_key", name},
        {"devices", device_entries},
        {"num_devices", devices},
        {"mem_bw_gbps", 1000},
        {"mem_latency_ns", 100},
    };
}

json directional_tier(uint32_t id, const char* name) {
    auto value = tier(id, name);
    value.erase("mem_bw_gbps");
    value["bandwidth_resource"] = {
        {"schema_version", "bandwidth-resource-v1"},
        {"read_bytes_per_second", 2'000'000'000ULL},
        {"write_bytes_per_second", 1'000'000'000ULL},
        {"shared_bytes_per_second", 3'000'000'000ULL},
        {"concurrency", "simultaneous"},
        {"turnaround_ns", 0},
    };
    return value;
}

bool contract_holds() {
    const std::string digest = "sha256:" + std::string(64, 'a');
    auto payload = json{
        {"schema_version", "memory-tier-runtime-v1"},
        {"id_mode", "native"},
        {"manifest_digest", digest},
        {"tiers", json::array({
            directional_tier(16, "hbm"), tier(17, "lpddr", 2),
            tier(18, "remote")})},
    };
    const auto parsed = AstraSim::parse_memory_tier_config(payload);
    if (!parsed.native || parsed.manifest_digest != digest ||
        parsed.tiers.size() != 3 || parsed.tiers[1].tier_id != 17 ||
        parsed.tiers[1].backend_config.at("num-devices") != 2 ||
        parsed.tiers[0].backend_config.at("bandwidth-resource")
                .at("read_bytes_per_second") != 2'000'000'000ULL) {
        return false;
    }
    const auto temporary_path =
        AstraSim::write_temporary_memory_backend_config(
            parsed.tiers[1].backend_config);
    std::ifstream temporary_input(temporary_path);
    json temporary_payload;
    temporary_input >> temporary_payload;
    const bool temporary_contract =
        std::filesystem::path(temporary_path).parent_path() ==
            std::filesystem::temp_directory_path() &&
        temporary_payload == parsed.tiers[1].backend_config;
    std::remove(temporary_path.c_str());
    if (!temporary_contract) {
        return false;
    }

    auto zero = payload;
    zero["tiers"][0]["tier_id"] = 0;
    auto unknown_backend = payload;
    unknown_backend["tiers"][0]["backend_kind"] = "future";
    auto stale_devices = payload;
    stale_devices["tiers"][1]["num_devices"] = 3;
    auto mixed = payload;
    mixed["local_mem"] = json::object();
    auto duplicate_bandwidth = payload;
    duplicate_bandwidth["tiers"][0]["mem_bw_gbps"] = 1000;
    auto overcommitted_shared = payload;
    overcommitted_shared["tiers"][0]["bandwidth_resource"]
                         ["shared_bytes_per_second"] = 2'999'999'999ULL;

    return throws_invalid_argument(
               [&]() { AstraSim::parse_memory_tier_config(zero); }) &&
           throws_invalid_argument(
               [&]() { AstraSim::parse_memory_tier_config(unknown_backend); }) &&
           throws_invalid_argument(
               [&]() { AstraSim::parse_memory_tier_config(stale_devices); }) &&
           throws_invalid_argument(
               [&]() { AstraSim::parse_memory_tier_config(mixed); }) &&
           throws_invalid_argument([&]() {
               AstraSim::parse_memory_tier_config(duplicate_bandwidth);
           }) &&
           throws_invalid_argument([&]() {
               AstraSim::parse_memory_tier_config(overcommitted_shared);
           }) &&
           throws_invalid_argument(
               [&]() { AstraSim::validate_et_manifest_digest(digest, ""); }) &&
           throws_invalid_argument([&]() {
               AstraSim::validate_et_manifest_digest(
                   digest, "sha256:" + std::string(64, 'b'));
           });
}

}  // namespace

int main() {
    return contract_holds() ? 0 : 1;
}
