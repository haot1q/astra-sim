/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "PdPathServiceConfig.hh"

#include <set>
#include <stdexcept>
#include <tuple>

#include "astra-sim/system/memory/BandwidthResource.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim {
namespace {
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;
using PhysicalKey = std::tuple<std::string, std::string, std::string, uint32_t>;

std::string text(const Json& value) {
    if (!value.is_string() || value.get<std::string>().empty()) {
        throw std::invalid_argument("P/D interface identifier must be nonempty text");
    }
    return value.get<std::string>();
}

uint64_t unsigned_value(const Json& value) {
    // Original memory projection stores nonnegative latency as a signed integer.
    if (!value.is_number_integer() ||
        (!value.is_number_unsigned() && value.get<int64_t>() < 0)) {
        throw std::invalid_argument("P/D value must be uint64");
    }
    return value.get<uint64_t>();
}

uint64_t latency(const Json& raw) {
    if (raw.at("mode") == "not_modeled") {
        Wire::fields(raw, {"mode"});
        return 0;
    }
    Wire::fields(raw, {"mode", "value_ns"});
    if (raw["mode"] != "explicit") throw std::invalid_argument("unknown P/D latency mode");
    return unsigned_value(raw["value_ns"]);
}

Json bandwidth(const Json& raw) {
    return bandwidth_resource_config_to_json(parse_bandwidth_resource_config(raw, "P/D interface"));
}

Json backend_bandwidth(const Json& backend) {
    if (backend.contains("bandwidth-resource")) return bandwidth(backend["bandwidth-resource"]);
    const auto rate = unsigned_value(backend.at("mem-bw"));
    if (rate == 0 || rate > UINT64_MAX / 1000000000ULL) {
        throw std::invalid_argument("invalid aliased scalar bandwidth");
    }
    const auto bytes = rate * 1000000000ULL;
    return bandwidth_resource_config_to_json({bytes, bytes, bytes,
                                              BandwidthConcurrency::Serialized, 0});
}

PhysicalKey physical_key(const Json& value) {
    return {Wire::name(value.at("owner_kind")), Wire::name(value.at("owner_id")),
            Wire::name(value.at("resource_key")), Wire::uint32(value.at("physical_device_id"))};
}

void reachable(const Json& resource, const Json& rank) {
    const auto kind = resource.at("owner_kind").get<std::string>();
    const auto expected = kind == "accelerator" ? text(rank.at("accelerator_id"))
        : kind == "node" ? std::to_string(Wire::uint32(rank.at("node_id"))) : "global";
    if ((kind != "accelerator" && kind != "node" && kind != "global") ||
        resource.at("owner_id") != expected) {
        throw std::invalid_argument("P/D interface physical owner is unreachable");
    }
}

std::map<std::string, Json> declarations(
    Json& raw, const PhysicalServiceConfig& physical, PdPathServiceConfig& result) {
    auto resources = physical.resources;
    std::set<PhysicalKey> identities;
    for (const auto& [id, resource] : resources) identities.insert(physical_key(resource));
    std::map<std::string, Json> declared;
    for (const auto& item : Wire::array(raw)) {
        Wire::fields(item, {"id", "owner_kind", "owner_id", "resource_key", "physical_device_id",
                            "bandwidth_resource", "latency", "rounding_mode"});
        const auto id = Wire::name(item["id"]);
        if (resources.count(id) || !declared.emplace(id, item).second ||
            !identities.insert(physical_key(item)).second || item["rounding_mode"] != "ceil_ns_v1") {
            throw std::invalid_argument("duplicate P/D physical declaration or invalid rounding");
        }
        result.declared_backends.emplace(id, Json{
            {"memory-type", "MEMORY_POOL"}, {"memory-location", "REMOTE_MEMORY"},
            {"num-devices", 1u}, {"mem-latency", latency(item["latency"])},
            {"bandwidth-resource", bandwidth(item["bandwidth_resource"])}});
        result.rounding.emplace(id, MemoryTimeRounding::Ceil);
    }
    raw = Json::array();
    for (const auto& [id, item] : declared) {
        resources.emplace(id, item);
        raw.push_back(item);
    }
    return resources;
}

void bindings(Json& raw, const Json& profile, const PhysicalServiceConfig& physical,
              const std::map<std::string, Json>& resources, PdPathServiceConfig& result) {
    std::map<std::string, Json> interfaces;
    for (const auto& item : Wire::array(profile.at("resources"))) {
        if (item.at("kind") == "interface" && !interfaces.emplace(text(item.at("id")), item).second) {
            throw std::invalid_argument("duplicate P/D interface");
        }
    }
    std::map<std::pair<uint32_t, std::string>, Json> rows;
    std::set<std::string> used, covered;
    for (const auto& row : Wire::array(raw)) {
        Wire::fields(row, {"rank", "interface_id", "physical_resource_ref", "rounding_mode"});
        const auto rank = Wire::uint32(row["rank"]);
        const auto interface = text(row["interface_id"]);
        const auto ref = Wire::name(row["physical_resource_ref"]);
        const auto key = std::make_pair(rank, interface);
        if (!physical.ranks.count(rank) || !interfaces.count(interface) || !resources.count(ref) ||
            !rows.emplace(key, row).second) {
            throw std::invalid_argument("unknown or duplicate P/D interface binding");
        }
        reachable(resources.at(ref), physical.ranks.at(rank));
        const bool existing = physical.backends.count(ref);
        const auto& backend = existing ? physical.backends.at(ref) : result.declared_backends.at(ref);
        const auto& specification = interfaces.at(interface);
        if ((existing && resources.at(ref).at("kind") == "ucie") ||
            row["rounding_mode"] != (existing ? "floor_ns_v1" : "ceil_ns_v1") ||
            backend_bandwidth(backend) != bandwidth(specification.at("bandwidth_resource")) ||
            unsigned_value(backend.at("mem-latency")) != latency(specification.at("latency"))) {
            throw std::invalid_argument("P/D interface parameters or rounding differ from physical owner");
        }
        result.bindings.emplace(key, ref);
        if (existing) result.rounding.emplace(ref, MemoryTimeRounding::Floor);
        used.insert(ref);
        covered.insert(interface);
    }
    if (covered.size() != interfaces.size()) throw std::invalid_argument("incomplete P/D interface bindings");
    for (const auto& [id, backend] : result.declared_backends) {
        if (!used.count(id)) throw std::invalid_argument("unreferenced P/D physical declaration");
    }
    raw = Json::array();
    for (const auto& [key, row] : rows) raw.push_back(row);
}
}  // namespace

PdPathServiceConfig load_pd_path_service_config(
    const std::string& path, const PhysicalServiceConfig& physical) {
    if (path.empty()) return {};
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "profile", "profile_digest", "service_binding_digest",
                        "service_activation_id", "declared_resources", "bindings", "binding_digest"});
    if (body["schema_version"] != "pd-path-service-binding-v1" ||
        body.at("profile").at("schema_version") != "pd-memory-path-profile-v1" ||
        body["service_binding_digest"] != physical.identity.binding_digest ||
        body["service_activation_id"] != physical.identity.activation_id ||
        body["profile_digest"] != Wire::digest(body["profile"], true)) {
        throw std::invalid_argument("P/D path service identity mismatch");
    }
    PdPathServiceConfig result;
    result.binding_digest = Wire::name(body["binding_digest"]);
    result.profile_digest = Wire::name(body["profile_digest"]);
    body.erase("binding_digest");
    const auto resources = declarations(body["declared_resources"], physical, result);
    bindings(body["bindings"], body["profile"], physical, resources, result);
    if (Wire::digest(body, true) != result.binding_digest) {
        throw std::invalid_argument("P/D path service content digest mismatch");
    }
    return result;
}

}  // namespace AstraSim
