/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/MemoryTierConfig.hh"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <regex>
#include <stdexcept>
#include <unordered_set>
#include <utility>
#include <vector>

#include <unistd.h>

namespace AstraSim {
namespace {

using json = nlohmann::json;

constexpr const char* kSchemaVersion = "memory-tier-runtime-v1";
constexpr uint32_t kNativeTierIdStart = 16;
const std::regex kTierName("^[a-z][a-z0-9_.-]*$");
const std::regex kDigest("^sha256:[0-9a-f]{64}$");

void validate_backend(const json& backend, const std::string& context) {
    if (!backend.contains("memory-type") ||
        !backend["memory-type"].is_string()) {
        throw std::invalid_argument(context + ".memory-type must be a string");
    }
    if (!backend.contains("mem-bw") ||
        !backend["mem-bw"].is_number_integer() ||
        backend["mem-bw"].get<int64_t>() <= 0) {
        throw std::invalid_argument(context + ".mem-bw must be a positive integer");
    }
    if (!backend.contains("mem-latency") ||
        !backend["mem-latency"].is_number_integer() ||
        backend["mem-latency"].get<int64_t>() < 0) {
        throw std::invalid_argument(
            context + ".mem-latency must be a non-negative integer");
    }
    if (!backend.contains("num-devices") ||
        !backend["num-devices"].is_number_integer() ||
        backend["num-devices"].get<int64_t>() <= 0) {
        throw std::invalid_argument(
            context + ".num-devices must be a positive integer");
    }
}

uint32_t legacy_id(const std::string& location) {
    if (location == "LOCAL_MEMORY") return 1;
    if (location == "REMOTE_MEMORY") return 2;
    if (location == "CXL_MEMORY") return 3;
    if (location == "STORAGE_MEMORY") return 4;
    throw std::invalid_argument("unsupported legacy memory-location " + location);
}

std::string legacy_name(uint32_t tier_id) {
    static const char* names[] = {"INVALID", "LOCAL", "REMOTE", "CXL", "STORAGE"};
    if (tier_id == 0 || tier_id > 4) {
        throw std::invalid_argument("invalid legacy tier_id");
    }
    return names[tier_id];
}

}  // namespace

MemoryTierConfigSet parse_memory_tier_config(const json& payload) {
    if (!payload.is_object()) {
        throw std::invalid_argument("memory configuration must be a JSON object");
    }
    const bool native = payload.contains("schema_version") ||
                        payload.contains("tiers") || payload.contains("id_mode");
    const std::vector<std::string> legacy_keys = {
        "local_mem", "remote_mem", "cxl_mem", "storage_mem"};
    if (native) {
        for (const auto& key : legacy_keys) {
            if (payload.contains(key)) {
                throw std::invalid_argument(
                    "legacy/native mixed memory configuration is not allowed");
            }
        }
        if (payload.value("schema_version", "") != kSchemaVersion) {
            throw std::invalid_argument("unsupported memory tier schema_version");
        }
        if (payload.value("id_mode", "") != "native") {
            throw std::invalid_argument("runtime memory id_mode must be native");
        }
        const auto digest = payload.value("manifest_digest", "");
        if (!std::regex_match(digest, kDigest)) {
            throw std::invalid_argument(
                "manifest_digest must be sha256:<64 lowercase hex>");
        }
        if (!payload.contains("tiers") || !payload["tiers"].is_array() ||
            payload["tiers"].empty()) {
            throw std::invalid_argument("tiers must be a non-empty array");
        }

        std::vector<MemoryTierConfig> tiers;
        std::unordered_set<std::string> names;
        for (std::size_t index = 0; index < payload["tiers"].size(); ++index) {
            const auto& raw = payload["tiers"][index];
            const auto context = "tiers[" + std::to_string(index) + "]";
            if (!raw.is_object()) {
                throw std::invalid_argument(context + " must be an object");
            }
            if (!raw.contains("tier_name") || !raw["tier_name"].is_string()) {
                throw std::invalid_argument(context + ".tier_name must be a string");
            }
            const auto name = raw["tier_name"].get<std::string>();
            if (!std::regex_match(name, kTierName) || !names.insert(name).second) {
                throw std::invalid_argument("invalid or duplicate tier_name '" + name + "'");
            }
            const uint32_t expected_id = kNativeTierIdStart + index;
            if (!raw.contains("tier_id") ||
                !raw["tier_id"].is_number_unsigned() ||
                raw["tier_id"].get<uint32_t>() != expected_id) {
                throw std::invalid_argument(
                    context + ".tier_id must be " + std::to_string(expected_id));
            }
            if (index != 0 &&
                payload["tiers"][index - 1]["tier_name"].get<std::string>() >= name) {
                throw std::invalid_argument("native tiers must be sorted by tier_name");
            }
            if (raw.value("backend_kind", "") != "analytical") {
                throw std::invalid_argument(
                    context + ".backend_kind must be analytical in this frontend");
            }
            if (!raw.contains("devices") || !raw["devices"].is_array() ||
                raw["devices"].empty()) {
                throw std::invalid_argument(context + ".devices must be non-empty");
            }
            if (!raw.contains("num_devices") ||
                !raw["num_devices"].is_number_unsigned() ||
                raw["num_devices"].get<std::size_t>() != raw["devices"].size()) {
                throw std::invalid_argument(
                    context + ".num_devices does not match devices");
            }
            for (std::size_t device_id = 0;
                 device_id < raw["devices"].size(); ++device_id) {
                const auto& device = raw["devices"][device_id];
                if (!device.is_object() ||
                    device.value("device_id", UINT32_MAX) != device_id ||
                    !device.contains("capacity_bytes") ||
                    !device["capacity_bytes"].is_number_unsigned() ||
                    device["capacity_bytes"].get<uint64_t>() == 0) {
                    throw std::invalid_argument(
                        context + ".devices must have contiguous IDs and positive capacity");
                }
            }

            json backend = {
                {"memory-type", "MEMORY_POOL"},
                {"memory-location", "REMOTE_MEMORY"},
                {"mem-bw", raw.value("mem_bw_gbps", 0)},
                {"mem-latency", raw.value("mem_latency_ns", -1)},
                {"num-devices", raw["num_devices"]},
            };
            validate_backend(backend, context);
            tiers.push_back({
                expected_id,
                name,
                raw["num_devices"].get<uint32_t>(),
                std::move(backend)});
        }
        return {true, digest, std::move(tiers)};
    }

    std::vector<MemoryTierConfig> tiers;
    const std::vector<std::pair<std::string, std::string>> legacy = {
        {"local_mem", "LOCAL_MEMORY"},
        {"remote_mem", "REMOTE_MEMORY"},
        {"cxl_mem", "CXL_MEMORY"},
        {"storage_mem", "STORAGE_MEMORY"},
    };
    if (payload.contains("memory-type")) {
        auto backend = payload;
        if (!backend.contains("memory-location")) {
            backend["memory-location"] = "REMOTE_MEMORY";
        }
        if (!backend.contains("num-devices")) backend["num-devices"] = 1;
        validate_backend(backend, "memory configuration");
        const auto id = legacy_id(backend["memory-location"]);
        tiers.push_back({
            id,
            legacy_name(id),
            backend["num-devices"].get<uint32_t>(),
            std::move(backend)});
    } else {
        for (const auto& [key, location] : legacy) {
            if (!payload.contains(key)) continue;
            if (!payload[key].is_object()) {
                throw std::invalid_argument(key + " must be an object");
            }
            auto backend = payload[key];
            backend["memory-location"] = location;
            if (!backend.contains("num-devices")) backend["num-devices"] = 1;
            validate_backend(backend, key);
            const auto id = legacy_id(location);
            tiers.push_back({
                id,
                legacy_name(id),
                backend["num-devices"].get<uint32_t>(),
                std::move(backend)});
        }
    }
    if (tiers.empty()) {
        throw std::invalid_argument("memory configuration contains no backends");
    }
    return {false, "", std::move(tiers)};
}

MemoryTierConfigSet load_memory_tier_config(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::invalid_argument("unable to open memory configuration " + path);
    }
    json payload;
    try {
        input >> payload;
    } catch (const json::exception& error) {
        throw std::invalid_argument(
            "unable to parse memory configuration " + path + ": " + error.what());
    }
    return parse_memory_tier_config(payload);
}

std::string write_temporary_memory_backend_config(const json& payload) {
    auto pattern = (
        std::filesystem::temp_directory_path() /
        "astra-sim-memory-XXXXXX").string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    const int descriptor = ::mkstemp(mutable_pattern.data());
    if (descriptor == -1) {
        throw std::runtime_error(
            "unable to create temporary memory backend configuration");
    }
    ::close(descriptor);
    const std::string path(mutable_pattern.data());
    try {
        std::ofstream output(path);
        if (!output) {
            throw std::runtime_error(
                "unable to open temporary memory backend configuration");
        }
        output << payload;
        if (!output) {
            throw std::runtime_error(
                "unable to write temporary memory backend configuration");
        }
    } catch (...) {
        std::remove(path.c_str());
        throw;
    }
    return path;
}

void validate_et_manifest_digest(
    const std::string& expected, const std::string& observed) {
    if (expected.empty() && observed.empty()) {
        return;
    }
    if (expected.empty() || observed.empty() || expected != observed) {
        throw std::invalid_argument(
            "ET/memory tier manifest digest mismatch: expected='" + expected +
            "', observed='" + observed + "'");
    }
}

}  // namespace AstraSim
