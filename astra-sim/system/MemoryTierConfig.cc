/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/MemoryTierConfig.hh"

#include "astra-sim/system/memory/BandwidthResource.hh"

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <regex>
#include <stdexcept>
#include <unordered_map>
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
    const bool has_scalar = backend.contains("mem-bw");
    const bool has_directional = backend.contains("bandwidth-resource");
    if (has_scalar == has_directional) {
        throw std::invalid_argument(
            context + " must declare exactly one of mem-bw or "
            "bandwidth-resource");
    }
    if (has_scalar &&
        (!backend["mem-bw"].is_number_integer() ||
         backend["mem-bw"].get<int64_t>() <= 0)) {
        throw std::invalid_argument(
            context + ".mem-bw must be a positive integer");
    }
    if (has_directional) {
        parse_bandwidth_resource_config(
            backend["bandwidth-resource"],
            context + ".bandwidth-resource");
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

void validate_ucie_endpoints(
    const json& raw,
    const std::string& context,
    const std::unordered_map<std::string, uint32_t>& tier_devices,
    uint32_t stack_count) {
    if (!raw.contains("endpoints") || !raw["endpoints"].is_array() ||
        raw["endpoints"].size() != 2 ||
        !raw["endpoints"][0].is_string() ||
        !raw["endpoints"][1].is_string() ||
        raw["endpoints"][0] == raw["endpoints"][1]) {
        throw std::invalid_argument(
            context + ".endpoints must be two distinct strings");
    }
    const auto left = raw["endpoints"][0].get<std::string>();
    const auto right = raw["endpoints"][1].get<std::string>();
    const bool left_compute = left == "compute";
    const bool right_compute = right == "compute";
    if (left_compute == right_compute) {
        throw std::invalid_argument(
            context + ".endpoints must be compute and one declared tier");
    }
    const auto& peer = left_compute ? right : left;
    const auto found = tier_devices.find(peer);
    if (found == tier_devices.end()) {
        throw std::invalid_argument(
            context + " peer '" + peer + "' is not a declared tier");
    }
    if (stack_count != found->second) {
        throw std::invalid_argument(
            context + ".stack_count must equal the peer device count");
    }
}

std::vector<UcieLinkConfig> parse_ucie_links(
    const json& payload,
    const std::unordered_map<std::string, uint32_t>& tier_devices) {
    if (!payload.contains("ucie_links")) {
        return {};
    }
    const auto& raw_links = payload["ucie_links"];
    if (!raw_links.is_array() || raw_links.empty()) {
        throw std::invalid_argument(
            "ucie_links must be a non-empty array when present");
    }
    static const std::unordered_set<std::string> allowed = {
        "id", "endpoints", "stack_count", "header_bytes", "latency_ns",
        "bandwidth_resource",
    };
    std::vector<UcieLinkConfig> links;
    std::unordered_set<std::string> ids;
    for (std::size_t index = 0; index < raw_links.size(); ++index) {
        const auto context = "ucie_links[" + std::to_string(index) + "]";
        const auto& raw = raw_links[index];
        if (!raw.is_object()) {
            throw std::invalid_argument(context + " must be an object");
        }
        for (const auto& [key, value] : raw.items()) {
            (void)value;
            if (!allowed.count(key)) {
                throw std::invalid_argument(
                    context + " has unknown field '" + key + "'");
            }
        }
        if (!raw.contains("id") || !raw["id"].is_string()) {
            throw std::invalid_argument(context + ".id must be a string");
        }
        const auto id = raw["id"].get<std::string>();
        if (!std::regex_match(id, kTierName) || !ids.insert(id).second ||
            tier_devices.count(id) != 0) {
            throw std::invalid_argument(
                context + ".id is invalid, duplicate, or collides with a tier");
        }
        if (!raw.contains("stack_count") ||
            !raw["stack_count"].is_number_unsigned() ||
            raw["stack_count"].get<uint32_t>() == 0) {
            throw std::invalid_argument(
                context + ".stack_count must be a positive integer");
        }
        const auto stack_count = raw["stack_count"].get<uint32_t>();
        validate_ucie_endpoints(raw, context, tier_devices, stack_count);
        if (!raw.contains("header_bytes") ||
            !raw["header_bytes"].is_number_unsigned()) {
            throw std::invalid_argument(
                context + ".header_bytes must be a non-negative integer");
        }
        if (!raw.contains("latency_ns") ||
            !raw["latency_ns"].is_number_unsigned()) {
            throw std::invalid_argument(
                context + ".latency_ns must be a non-negative integer");
        }
        if (!raw.contains("bandwidth_resource")) {
            throw std::invalid_argument(
                context + " must declare bandwidth_resource");
        }
        json backend = {
            {"memory-type", "MEMORY_POOL"},
            {"memory-location", "REMOTE_MEMORY"},
            {"mem-latency", raw["latency_ns"]},
            {"num-devices", raw["stack_count"]},
            {"bandwidth-resource", raw["bandwidth_resource"]},
        };
        parse_bandwidth_resource_config(
            backend["bandwidth-resource"], context + ".bandwidth_resource");
        links.push_back({
            id,
            {raw["endpoints"][0].get<std::string>(),
             raw["endpoints"][1].get<std::string>()},
            raw["stack_count"].get<uint32_t>(),
            raw["header_bytes"].get<uint64_t>(),
            raw["latency_ns"].get<uint64_t>(),
            std::move(backend),
        });
    }
    return links;
}

uint32_t home_domain_count(const json& payload) {
    if (!payload.contains("home_domain_topology") ||
        !payload["home_domain_topology"].is_object() ||
        !payload["home_domain_topology"].contains("domains") ||
        !payload["home_domain_topology"]["domains"].is_array() ||
        payload["home_domain_topology"]["domains"].empty()) {
        throw std::invalid_argument(
            "movement_paths require non-empty home_domain_topology.domains");
    }
    const auto count =
        payload["home_domain_topology"]["domains"].size();
    if (count > UINT32_MAX) {
        throw std::invalid_argument("home domain count exceeds uint32");
    }
    return static_cast<uint32_t>(count);
}

void validate_closed(const json& raw,
                     const std::unordered_set<std::string>& allowed,
                     const std::string& context) {
    if (!raw.is_object()) {
        throw std::invalid_argument(context + " must be an object");
    }
    for (const auto& [key, value] : raw.items()) {
        (void)value;
        if (!allowed.count(key)) {
            throw std::invalid_argument(
                context + " has unknown field '" + key + "'");
        }
    }
}

std::string required_string(const json& raw,
                            const std::string& field,
                            const std::string& context) {
    if (!raw.contains(field) || !raw[field].is_string() ||
        raw[field].get<std::string>().empty()) {
        throw std::invalid_argument(
            context + "." + field + " must be a non-empty string");
    }
    return raw[field].get<std::string>();
}

uint32_t required_positive_uint32(const json& raw,
                                  const std::string& field,
                                  const std::string& context) {
    if (!raw.contains(field) || !raw[field].is_number_unsigned() ||
        raw[field].get<uint64_t>() == 0 ||
        raw[field].get<uint64_t>() > UINT32_MAX) {
        throw std::invalid_argument(
            context + "." + field + " must be a positive uint32");
    }
    return raw[field].get<uint32_t>();
}

struct ParsedMovementPaths {
    bool present = false;
    std::string selected_id;
    std::vector<MovementPathCapabilityConfig> capabilities;
    std::vector<MovementBandwidthResourceConfig> resources;
};

ParsedMovementPaths parse_movement_paths(
    const json& payload,
    const std::vector<UcieLinkConfig>& ucie_links) {
    if (!payload.contains("movement_paths")) {
        return {};
    }
    const auto& raw = payload["movement_paths"];
    const std::string root = "movement_paths";
    validate_closed(
        raw,
        {"schema_version", "selection_mode", "selected_capability_id",
         "capabilities", "bandwidth_resources"},
        root);
    if (raw.value("schema_version", "") != "movement-path-v1") {
        throw std::invalid_argument(
            "movement_paths.schema_version must be movement-path-v1");
    }
    if (raw.value("selection_mode", "") != "fixed") {
        throw std::invalid_argument(
            "movement_paths.selection_mode must be fixed");
    }
    const auto selected =
        required_string(raw, "selected_capability_id", root);
    if (!raw.contains("bandwidth_resources") ||
        !raw["bandwidth_resources"].is_array() ||
        raw["bandwidth_resources"].empty()) {
        throw std::invalid_argument(
            "movement_paths.bandwidth_resources must be non-empty");
    }
    const uint32_t domains = home_domain_count(payload);
    std::vector<MovementBandwidthResourceConfig> resources;
    std::unordered_set<std::string> resource_ids;
    for (std::size_t index = 0;
         index < raw["bandwidth_resources"].size(); ++index) {
        const auto context = "movement_paths.bandwidth_resources[" +
            std::to_string(index) + "]";
        const auto& item = raw["bandwidth_resources"][index];
        validate_closed(
            item,
            {"id", "stack_count", "latency_ns", "bandwidth_resource"},
            context);
        const auto id = required_string(item, "id", context);
        if (!std::regex_match(id, kTierName) ||
            !resource_ids.insert(id).second) {
            throw std::invalid_argument(
                context + ".id is invalid or duplicate");
        }
        const auto stacks =
            required_positive_uint32(item, "stack_count", context);
        if (stacks != domains) {
            throw std::invalid_argument(
                context + ".stack_count must equal home domain count");
        }
        if (!item.contains("latency_ns") ||
            !item["latency_ns"].is_number_unsigned()) {
            throw std::invalid_argument(
                context + ".latency_ns must be non-negative");
        }
        if (!item.contains("bandwidth_resource")) {
            throw std::invalid_argument(
                context + " must declare bandwidth_resource");
        }
        json backend = {
            {"memory-type", "MEMORY_POOL"},
            {"memory-location", "REMOTE_MEMORY"},
            {"mem-latency", item["latency_ns"]},
            {"num-devices", stacks},
            {"bandwidth-resource", item["bandwidth_resource"]},
        };
        parse_bandwidth_resource_config(
            item["bandwidth_resource"],
            context + ".bandwidth_resource");
        resources.push_back(
            {id, stacks, item["latency_ns"].get<uint64_t>(),
             std::move(backend)});
    }
    std::unordered_set<std::string> ucie_ids;
    for (const auto& link : ucie_links) {
        ucie_ids.insert(link.id);
    }
    if (!raw.contains("capabilities") ||
        !raw["capabilities"].is_array() || raw["capabilities"].empty()) {
        throw std::invalid_argument(
            "movement_paths.capabilities must be non-empty");
    }
    std::vector<MovementPathCapabilityConfig> capabilities;
    std::unordered_set<std::string> capability_ids;
    for (std::size_t index = 0; index < raw["capabilities"].size(); ++index) {
        const auto context = "movement_paths.capabilities[" +
            std::to_string(index) + "]";
        const auto& item = raw["capabilities"][index];
        validate_closed(
            item,
            {"id", "engine_location", "engine_count",
             "max_priority_burst", "max_in_flight_page_movements",
             "timing_provenance", "segments"},
            context);
        const auto id = required_string(item, "id", context);
        if ((id != "base_die_local" && id != "gpu_routed") ||
            !capability_ids.insert(id).second) {
            throw std::invalid_argument(
                context + ".id is unsupported or duplicate");
        }
        const auto expected_location =
            id == "base_die_local" ? "base_die" : "gpu";
        if (required_string(item, "engine_location", context) !=
            expected_location) {
            throw std::invalid_argument(
                context + ".engine_location does not match path");
        }
        const auto engine_count =
            required_positive_uint32(item, "engine_count", context);
        const auto max_priority_burst =
            required_positive_uint32(item, "max_priority_burst", context);
        const auto max_in_flight = required_positive_uint32(
            item, "max_in_flight_page_movements", context);
        if (max_in_flight > engine_count) {
            throw std::invalid_argument(
                context + ".max_in_flight_page_movements exceeds engine_count");
        }
        const auto provenance =
            required_string(item, "timing_provenance", context);
        if (provenance != "estimated" && provenance != "measured") {
            throw std::invalid_argument(
                context + ".timing_provenance is unsupported");
        }
        if (!item.contains("segments") || !item["segments"].is_array() ||
            item["segments"].empty()) {
            throw std::invalid_argument(context + ".segments must be non-empty");
        }
        std::vector<MovementPathSegmentConfig> segments;
        std::unordered_set<std::string> segment_ids;
        for (std::size_t segment_index = 0;
             segment_index < item["segments"].size(); ++segment_index) {
            const auto segment_context = context + ".segments[" +
                std::to_string(segment_index) + "]";
            const auto& segment = item["segments"][segment_index];
            validate_closed(
                segment,
                {"id", "kind", "resource_ref", "operation", "byte_rule"},
                segment_context);
            const auto segment_id =
                required_string(segment, "id", segment_context);
            if (!segment_ids.insert(segment_id).second) {
                throw std::invalid_argument(
                    context + ".segments IDs must be unique");
            }
            const auto kind =
                required_string(segment, "kind", segment_context);
            const auto resource_ref =
                required_string(segment, "resource_ref", segment_context);
            const auto operation =
                required_string(segment, "operation", segment_context);
            if ((kind != "bandwidth_resource" &&
                 kind != "ucie_transaction") ||
                (operation != "read" && operation != "write") ||
                segment.value("byte_rule", "") != "payload") {
                throw std::invalid_argument(
                    segment_context + " has unsupported segment semantics");
            }
            if ((kind == "bandwidth_resource" &&
                 !resource_ids.count(resource_ref)) ||
                (kind == "ucie_transaction" &&
                 !ucie_ids.count(resource_ref))) {
                throw std::invalid_argument(
                    segment_context + " references an unknown resource");
            }
            segments.push_back(
                {segment_id, kind, resource_ref, operation, "payload"});
        }
        const bool valid_base = id == "base_die_local" &&
            segments.size() == 2 &&
            segments[0].kind == "bandwidth_resource" &&
            segments[0].operation == "read" &&
            segments[1].kind == "bandwidth_resource" &&
            segments[1].operation == "write";
        const bool valid_gpu = id == "gpu_routed" &&
            segments.size() == 3 &&
            segments[0].kind == "ucie_transaction" &&
            segments[0].operation == "read" &&
            segments[1].kind == "bandwidth_resource" &&
            segments[1].operation == "write" &&
            segments[2].kind == "ucie_transaction" &&
            segments[2].operation == "write";
        if (!valid_base && !valid_gpu) {
            throw std::invalid_argument(
                context + " does not match its required path shape");
        }
        capabilities.push_back(
            {id, expected_location, engine_count, max_priority_burst,
             max_in_flight, provenance, std::move(segments)});
    }
    if (!capability_ids.count(selected)) {
        throw std::invalid_argument(
            "selected movement path is not a declared capability");
    }
    return {true, selected, std::move(capabilities), std::move(resources)};
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
        std::unordered_map<std::string, uint32_t> devices_by_name;
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
            devices_by_name.emplace(name, raw["num_devices"].get<uint32_t>());
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

            const bool has_scalar = raw.contains("mem_bw_gbps");
            const bool has_directional = raw.contains("bandwidth_resource");
            if (has_scalar == has_directional) {
                throw std::invalid_argument(
                    context + " must declare exactly one of mem_bw_gbps or "
                    "bandwidth_resource");
            }
            json backend = {
                {"memory-type", "MEMORY_POOL"},
                {"memory-location", "REMOTE_MEMORY"},
                {"mem-latency", raw.value("mem_latency_ns", -1)},
                {"num-devices", raw["num_devices"]},
            };
            if (has_scalar) {
                backend["mem-bw"] = raw["mem_bw_gbps"];
            } else {
                backend["bandwidth-resource"] = raw["bandwidth_resource"];
            }
            validate_backend(backend, context);
            tiers.push_back({
                expected_id,
                name,
                raw["num_devices"].get<uint32_t>(),
                std::move(backend)});
        }
        auto ucie_links = parse_ucie_links(payload, devices_by_name);
        auto movement_paths = parse_movement_paths(payload, ucie_links);
        return {
            true,
            digest,
            std::move(tiers),
            std::move(ucie_links),
            movement_paths.present,
            std::move(movement_paths.selected_id),
            std::move(movement_paths.capabilities),
            std::move(movement_paths.resources),
        };
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
    if (payload.contains("ucie_links")) {
        throw std::invalid_argument(
            "legacy memory configuration cannot declare ucie_links");
    }
    return {false, "", std::move(tiers), {}, false, "", {}, {}};
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
