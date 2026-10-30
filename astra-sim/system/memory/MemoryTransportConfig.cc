#include "MemoryTransportConfig.hh"

#include <set>
#include <stdexcept>
#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim {
MemoryTransportConfig load_memory_transport_config(const std::string& path,
    const PhysicalServiceConfig& physical, const TransportServiceConfig& transport,
    const std::string& manifest_digest) {
    if (path.empty()) return {};
    namespace Wire = ServiceBindingJson;
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "mode", "additional_fabric_cost", "physical_network_sha256",
        "transport_projection_digest", "service_binding_digest", "service_activation_id",
        "tier_manifest_digest", "compute_binding_digest", "operation_projection_digest", "operations"});
    if (body["schema_version"] != "physical-memory-routes-v1" ||
        body["mode"] != "append_distinct_ucie_fabric_v1" || transport.projection_digest.empty() ||
        body["physical_network_sha256"] != transport.physical_network_sha256 ||
        body["transport_projection_digest"] != transport.projection_digest ||
        body["service_binding_digest"] != physical.identity.binding_digest ||
        body["service_activation_id"] != physical.identity.activation_id ||
        body["tier_manifest_digest"] != manifest_digest) {
        throw std::invalid_argument("memory transport identity or mode mismatch");
    }
    const auto& cost = body["additional_fabric_cost"];
    Wire::fields(cost, {"declaration", "reference"});
    if (cost["declaration"] != "excluded_from_base_cost_and_ucie_v1" ||
        !cost["reference"].is_string() || cost["reference"].get<std::string>().find_first_not_of(" \t\n\r") == std::string::npos) {
        throw std::invalid_argument("memory transport requires excluded fabric cost source");
    }
    MemoryTransportConfig result;
    const auto compute_digest = Wire::name(body["compute_binding_digest"]);
    if (compute_digest.size() != 71 || compute_digest.substr(0, 7) != "sha256:" ||
        compute_digest.substr(7).find_first_not_of("0123456789abcdef") != std::string::npos) {
        throw std::invalid_argument("memory transport requires H2 binding digest");
    }
    result.digest = Wire::name(body["operation_projection_digest"]);
    body.erase("operation_projection_digest");
    if (result.digest != Wire::digest(body)) {
        throw std::invalid_argument("memory transport projection digest mismatch");
    }
    for (const auto& row : Wire::array(body["operations"])) {
        Wire::fields(row, {"rank", "logical_ucie_link", "logical_device_id", "operation", "position", "service_link_ids"});
        const auto rank = Wire::uint32(row["rank"]), device = Wire::uint32(row["logical_device_id"]);
        const auto link = Wire::name(row["logical_ucie_link"]);
        if (!physical.bindings.count({rank, "ucie", link, device}) ||
            (row["operation"] != "read" && row["operation"] != "write") ||
            (row["position"] != "before" && row["position"] != "after")) {
            throw std::invalid_argument("memory transport unknown endpoint or operation");
        }
        MemoryTransportOperation operation{row["position"] == "before", {}};
        std::set<std::string> resources;
        for (const auto& value : Wire::array(row["service_link_ids"])) {
            const auto id = Wire::name(value);
            const auto found = transport.links.find(id);
            if (found == transport.links.end() || !resources.insert(found->second.resource).second) {
                throw std::invalid_argument("memory transport unknown link or repeated resource");
            }
            operation.links.push_back(id);
        }
        const auto direction = row["operation"] == "read" ? MemoryOperation::Read : MemoryOperation::Write;
        if (!result.operations.emplace(MemoryTransportKey{rank, link, device, direction}, operation).second) {
            throw std::invalid_argument("duplicate memory transport operation");
        }
    }
    if (result.operations.empty()) throw std::invalid_argument("memory transport requires operations");
    for (const auto& [key, operation] : result.operations) {
        const auto [rank, link, device, direction] = key;
        if (!result.operations.count({rank, link, device,
                direction == MemoryOperation::Read ? MemoryOperation::Write : MemoryOperation::Read})) {
            throw std::invalid_argument("memory transport endpoint requires both directions");
        }
    }
    return result;
}
}  // namespace AstraSim
