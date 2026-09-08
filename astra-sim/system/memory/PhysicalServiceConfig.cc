/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "PhysicalServiceConfig.hh"
#include "ServiceBindingJson.hh"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace AstraSim {
namespace {
using Json = nlohmann::json;
namespace Wire = ServiceBindingJson;

struct Contract {
    Json backend;
    Json signature;
    std::vector<ServiceEndpoint> peers;
};
using Contracts = std::map<ServiceEndpoint, Contract>;
using Rows = std::map<std::string, Json>;
using Ranks = std::map<uint32_t, Json>;

Contract contract(Json backend, std::vector<ServiceEndpoint> peers = {}) {
    backend["num-devices"] = 1u;
    return {backend, backend, std::move(peers)};
}

ServiceEndpoint memory_endpoint(const std::string& label) {
    const auto colon = label.find(':');
    if (colon == std::string::npos) {
        throw std::invalid_argument("physical service Home endpoint lacks device selector");
    }
    const auto digits = label.substr(colon + 1);
    if (digits.empty() || digits.find_first_not_of("0123456789") != std::string::npos) {
        throw std::invalid_argument("physical service Home device must be uint32");
    }
    const auto device = std::stoull(digits);
    if (device > UINT32_MAX) throw std::invalid_argument("Home device exceeds uint32");
    return {"memory", label.substr(0, colon), static_cast<uint32_t>(device)};
}

Contracts service_contracts(const MemoryTierConfigSet& config) {
    Contracts result;
    for (const auto& tier : config.tiers) {
        for (uint32_t device = 0; device < tier.num_devices; ++device) {
            result.emplace(ServiceEndpoint{"memory", tier.tier_name, device},
                           contract(tier.backend_config));
        }
    }
    for (const auto& link : config.ucie_links) {
        const auto peer = link.endpoints[0] == "compute" ? link.endpoints[1] : link.endpoints[0];
        for (uint32_t device = 0; device < link.stack_count; ++device) {
            ServiceEndpoint memory{"memory", peer, device};
            if (!result.count(memory)) throw std::invalid_argument("undefined UCIe peer device");
            auto projected = contract(link.backend_config, {memory});
            projected.signature["transport-header-bytes"] = link.header_bytes;
            result.emplace(ServiceEndpoint{"ucie", link.id, device}, std::move(projected));
        }
    }
    std::map<uint32_t, std::vector<ServiceEndpoint>> domains;
    if (config.native_payload.contains("home_domain_topology")) {
        for (const auto& domain : config.native_payload["home_domain_topology"]["domains"]) {
            const auto id = Wire::uint32(domain.at("home_domain_id"));
            if (!domains.emplace(id, std::vector<ServiceEndpoint>{
                    memory_endpoint(domain.at("hot")), memory_endpoint(domain.at("cold"))}).second) {
                throw std::invalid_argument("duplicate Home domain");
            }
        }
    }
    for (const auto& resource : config.movement_bandwidth_resources) {
        for (uint32_t device = 0; device < resource.stack_count; ++device) {
            if (!domains.count(device)) throw std::invalid_argument("undefined movement Home domain");
            for (const auto& peer : domains.at(device)) {
                if (!result.count(peer)) throw std::invalid_argument("undefined movement memory endpoint");
            }
            result.emplace(ServiceEndpoint{"movement", resource.id, device},
                           contract(resource.backend_config, domains.at(device)));
        }
    }
    return result;
}

Ranks parse_ranks(const Json& raw, uint32_t count) {
    Ranks result;
    std::map<std::string, uint32_t> accelerator_nodes;
    std::map<uint32_t, uint32_t> instance_nodes;
    for (const auto& rank : Wire::array(raw)) {
        Wire::fields(rank, {"rank", "instance_id", "node_id", "accelerator_id"});
        const auto id = Wire::uint32(rank["rank"]);
        const auto instance = Wire::uint32(rank["instance_id"]);
        const auto node = Wire::uint32(rank["node_id"]);
        const auto accelerator = Wire::name(rank["accelerator_id"]);
        if (id >= count || !result.emplace(id, rank).second) {
            throw std::invalid_argument("invalid/duplicate physical service rank");
        }
        if (accelerator_nodes.emplace(accelerator, node).first->second != node ||
            instance_nodes.emplace(instance, node).first->second != node) {
            throw std::invalid_argument("accelerator/instance crosses physical nodes");
        }
    }
    if (result.size() != count || count == 0) {
        throw std::invalid_argument("physical service ranks do not cover actual topology");
    }
    return result;
}

Rows parse_resources(const Json& raw) {
    Rows result;
    std::set<std::tuple<std::string, std::string, std::string, uint32_t>> physical;
    for (auto resource : Wire::array(raw)) {
        Wire::fields(resource, {"id", "kind", "owner_kind", "owner_id", "resource_key",
                               "physical_device_id", "memory_endpoints"});
        const auto id = Wire::name(resource["id"]);
        const auto kind = Wire::name(resource["kind"]);
        const auto owner = Wire::name(resource["owner_kind"]);
        const auto owner_id = Wire::name(resource["owner_id"]);
        const auto key = Wire::name(resource["resource_key"]);
        const auto device = Wire::uint32(resource["physical_device_id"]);
        if ((kind != "memory" && kind != "ucie" && kind != "movement") ||
            (owner != "accelerator" && owner != "node" && owner != "global")) {
            throw std::invalid_argument("unsupported physical resource kind or owner_kind");
        }
        std::set<std::string> peers;
        for (const auto& peer : Wire::array(resource["memory_endpoints"])) {
            if (!peers.insert(Wire::name(peer)).second) {
                throw std::invalid_argument("duplicate physical memory endpoint");
            }
        }
        resource["memory_endpoints"] = peers;
        if (!result.emplace(id, resource).second ||
            !physical.emplace(owner, owner_id, key, device).second) {
            throw std::invalid_argument("duplicate resource ID or physical tuple");
        }
    }
    return result;
}

void reachable(const Json& resource, const Json& rank) {
    const std::string kind = resource["owner_kind"];
    const std::string expected = kind == "accelerator"
        ? rank["accelerator_id"].get<std::string>()
        : kind == "node" ? std::to_string(Wire::uint32(rank["node_id"])) : "global";
    if (resource["owner_id"] != expected ||
        (resource["kind"] == "ucie" && kind != "accelerator")) {
        throw std::invalid_argument("rank " + rank["rank"].dump() +
                                    ": unreachable physical resource " + resource["id"].dump());
    }
}

struct BindingRows {
    std::map<ServiceAccess, Json> rows;
    std::map<std::string, Json> backends;
};

BindingRows parse_bindings(const Json& raw, const Ranks& ranks,
                          const Rows& resources, const Contracts& contracts) {
    BindingRows result;
    std::map<std::string, Json> signatures;
    for (const auto& row : Wire::array(raw)) {
        Wire::fields(row, {"rank", "kind", "logical_ref", "logical_device_id",
                           "physical_resource_ref"});
        const auto rank = Wire::uint32(row["rank"]);
        const auto kind = Wire::name(row["kind"]);
        const auto logical = Wire::name(row["logical_ref"]);
        const auto device = Wire::uint32(row["logical_device_id"]);
        const auto ref = Wire::name(row["physical_resource_ref"]);
        const ServiceEndpoint endpoint{kind, logical, device};
        const ServiceAccess access{rank, kind, logical, device};
        if (!ranks.count(rank) || !contracts.count(endpoint) || !resources.count(ref)) {
            throw std::invalid_argument("unknown rank/logical endpoint/physical resource binding");
        }
        const auto& resource = resources.at(ref);
        if (resource["kind"] != kind || !result.rows.emplace(access, row).second) {
            throw std::invalid_argument("cross-kind alias or duplicate physical binding");
        }
        reachable(resource, ranks.at(rank));
        const auto& service = contracts.at(endpoint);
        if (signatures.emplace(ref, service.signature).first->second != service.signature) {
            throw std::invalid_argument("conflicting physical service parameters: " + ref);
        }
        result.backends.emplace(ref, service.backend);
    }
    if (result.rows.size() != ranks.size() * contracts.size() ||
        result.backends.size() != resources.size()) {
        throw std::invalid_argument("incomplete physical bindings or unreferenced resource");
    }
    return result;
}

void validate_wiring(const BindingRows& bindings, const Rows& resources,
                     const Contracts& contracts) {
    std::map<std::string, std::set<std::string>> endpoints;
    for (const auto& [key, row] : bindings.rows) {
        const auto& [rank, kind, logical, device] = key;
        auto& required = endpoints[row["physical_resource_ref"]];
        for (const auto& [peer_kind, peer_name, peer_device] :
             contracts.at({kind, logical, device}).peers) {
            required.insert(bindings.rows.at({rank, peer_kind, peer_name, peer_device})
                                ["physical_resource_ref"].get<std::string>());
        }
    }
    for (const auto& [ref, resource] : resources) {
        const auto declared = resource["memory_endpoints"].get<std::set<std::string>>();
        if (declared != endpoints.at(ref) ||
            (resource["kind"] == "ucie" && declared.size() != 1)) {
            throw std::invalid_argument("physical memory endpoint wiring mismatch: " + ref);
        }
        for (const auto& peer : declared) {
            if (resources.at(peer)["kind"] != "memory") {
                throw std::invalid_argument("physical peer must be a memory resource");
            }
        }
    }
}

template <typename Key>
Json values(const std::map<Key, Json>& source) {
    auto result = Json::array();
    for (const auto& [key, value] : source) result.push_back(value);
    return result;
}

}  // namespace

PhysicalServiceConfig load_physical_service_config(
    const std::string& path, const MemoryTierConfigSet& memory, uint32_t ranks) {
    if (path.empty() || !memory.native) {
        throw std::invalid_argument("native backend requires explicit physical service bindings");
    }
    auto manifest_body = memory.native_payload;
    manifest_body.erase("manifest_digest");
    if (Wire::digest(manifest_body) != memory.manifest_digest) {
        throw std::invalid_argument("physical service tier manifest content digest mismatch");
    }
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "tier_manifest_digest", "ranks", "resources",
                        "bindings", "binding_digest", "activation_id"});
    if (body["schema_version"] != "physical-service-binding-v1" ||
        body["tier_manifest_digest"] != memory.manifest_digest) {
        throw std::invalid_argument("physical service schema/manifest mismatch");
    }
    const auto activation = Wire::name(body["activation_id"]);
    const auto digest = Wire::name(body["binding_digest"]);
    body.erase("activation_id");
    body.erase("binding_digest");
    const auto rank_rows = parse_ranks(body["ranks"], ranks);
    const auto resources = parse_resources(body["resources"]);
    const auto contracts = service_contracts(memory);
    auto bindings = parse_bindings(body["bindings"], rank_rows, resources, contracts);
    validate_wiring(bindings, resources, contracts);
    body["ranks"] = values(rank_rows);
    body["resources"] = values(resources);
    body["bindings"] = values(bindings.rows);
    if (Wire::digest(body) != digest) {
        throw std::invalid_argument("physical service binding content digest mismatch");
    }
    PhysicalServiceConfig result{{digest, activation}, {}, std::move(bindings.backends)};
    for (const auto& [key, row] : bindings.rows) {
        result.bindings.emplace(key, row["physical_resource_ref"]);
    }
    return result;
}

}  // namespace AstraSim
