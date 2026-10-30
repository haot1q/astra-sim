/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "TransportServiceConfig.hh"

#include <algorithm>
#include <stdexcept>

#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim {
namespace {
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;

uint64_t unsigned_value(const Json& value) {
    if (!value.is_number_integer() ||
        (!value.is_number_unsigned() && value.get<int64_t>() < 0)) {
        throw std::invalid_argument("transport service requires uint64 bytes/s and ns");
    }
    return value.get<uint64_t>();
}

void resources(const Json& rows, const PhysicalServiceConfig& physical,
               TransportServiceConfig& result) {
    for (const auto& row : Wire::array(rows)) {
        Wire::fields(row, {"id", "bandwidth_bytes_per_second", "latency_ns"});
        const auto id = Wire::name(row["id"]);
        const auto rate = unsigned_value(row["bandwidth_bytes_per_second"]);
        const BandwidthResource service({rate, rate, rate, BandwidthConcurrency::Serialized, 0});
        if (physical.resources.count(id) || !result.resources.emplace(id,
                TransportResource{service.config(), unsigned_value(row["latency_ns"])}).second) {
            throw std::invalid_argument("transport resource ID collision");
        }
    }
}

void links(const Json& rows, TransportServiceConfig& result) {
    for (const auto& row : Wire::array(rows)) {
        Wire::fields(row, {"id", "source", "target", "resource"});
        TransportLink link{Wire::name(row["id"]), Wire::name(row["source"]),
                           Wire::name(row["target"]), Wire::name(row["resource"])};
        if (link.source == link.target || !result.resources.count(link.resource) ||
            !result.links.emplace(link.id, link).second) {
            throw std::invalid_argument("transport duplicate link or invalid resource/endpoints");
        }
    }
}
}  // namespace

TransportServiceConfig load_transport_service_config(
    const std::string& path, const PhysicalServiceConfig& physical) {
    if (path.empty()) return {};
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "physical_network_sha256", "service_binding_digest",
        "service_activation_id", "policy", "rounding_mode", "rank_count", "resources", "links",
        "projection_digest"});
    const bool propagation = body["policy"] == "fifo_service_then_propagate_v1";
    if (body["schema_version"] != "transport-service-v1" ||
        (!propagation && body["policy"] != "fifo_serialized_latency_v1") ||
        body["rounding_mode"] != "ceil_ns_v1" ||
        body["service_binding_digest"] != physical.identity.binding_digest ||
        body["service_activation_id"] != physical.identity.activation_id ||
        Wire::uint32(body["rank_count"]) != physical.ranks.size()) {
        throw std::invalid_argument("transport identity, policy or rank coverage mismatch");
    }
    TransportServiceConfig result;
    result.policy = propagation ? TransportLatencyPolicy::Propagation : TransportLatencyPolicy::Serialized;
    result.physical_network_sha256 = Wire::name(body["physical_network_sha256"]);
    const auto& sha = result.physical_network_sha256;
    if (sha.size() != 64 || !std::all_of(sha.begin(), sha.end(), [](char ch) {
            return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'); })) {
        throw std::invalid_argument("transport requires H1 SHA256 identity");
    }
    result.projection_digest = Wire::name(body["projection_digest"]);
    body.erase("projection_digest");
    if (result.projection_digest != Wire::digest(body)) {
        throw std::invalid_argument("transport projection digest mismatch");
    }
    resources(body["resources"], physical, result);
    links(body["links"], result);
    return result;
}
}  // namespace AstraSim
