/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "PdPathStages.hh"

#include <algorithm>
#include <set>
#include "PdPathWire.hh"
#include "UcieTransport.hh"
#include "astra-sim/system/Sys.hh"

namespace AstraSim {
namespace {
using namespace PdPathWire;
namespace Wire = ServiceBindingJson;

void network(const PdPathWork& work, const Json& resource,
             const PhysicalServiceConfig& physical, const Json& actual) {
    const auto& snapshot = work.context.at("network");
    require(!actual.is_null() && snapshot.is_object() && snapshot.at("network_config") == actual,
            "P/D work differs from actual network configuration");
    require(snapshot.at("owner_id") == resource.at("owner_ref") &&
            snapshot.at("network_config_digest") == work.binding.at("network_config_digest"),
            "P/D network owner/digest mismatch");
    std::set<std::pair<std::string, std::string>> expected, observed;
    for (const auto& row : Wire::array(snapshot.at("coverage"))) {
        require(expected.emplace(text(row.at("component_id")), text(row.at("action"))).second,
                "duplicate network coverage");
    }
    for (const auto& row : Wire::array(resource.at("coverage"))) {
        require(observed.emplace(text(row.at("component_id")), text(row.at("action"))).second,
                "duplicate network resource coverage");
    }
    require(!expected.empty() && expected == observed, "P/D network coverage mismatch");
    std::set<uint32_t> ranks;
    for (const auto& row : Wire::array(snapshot.at("rank_owners"))) {
        const auto rank = Wire::uint32(row.at("rank"));
        require(ranks.insert(rank).second && physical.ranks.count(rank) &&
                physical.ranks.at(rank).at("instance_id") == row.at("instance_id"),
                "P/D actual network rank owner mismatch");
    }
    require(ranks.size() == physical.ranks.size(), "P/D network omits actual ranks");
}

PdPathStage stage(const PdPathWork& work, const PdPathSlice& slice, const Json& segment,
                  const Json& resource, const std::map<std::string, Json>& buffers,
                  const std::vector<Sys*>& systems, PhysicalServiceFactory& services) {
    const bool destination = resource.at("side") == "destination";
    const std::string side = destination ? "destination" : "source";
    const auto rank = destination ? slice.destination_rank : slice.source_rank;
    auto* sys = systems.at(rank);
    const auto& buffer = buffers.at(text(work.binding.at("ranges").at(slice.range_index).at(side + "_buffer_id")));
    const auto& location = buffer.at("location");
    const auto device = Wire::uint32(location.at("device_id"));
    const auto tier_id = Wire::uint32(location.at("tier_id"));
    const auto tier_name = text(location.at("tier_name"));
    const auto& tiers = services.at(rank).memory;
    const auto actual = std::find_if(tiers.begin(), tiers.end(), [&](const auto& tier) {
        return tier.tier_id == tier_id && tier.tier_name == tier_name && device < tier.num_devices;
    });
    require(actual != tiers.end(), "P/D buffer tier name/id/device differs from actual registry");
    const auto kind = text(resource.at("kind"));
    require(segment.at("operation") == "read" || segment.at("operation") == "write" ||
            (kind == "network" && segment.at("operation") == "transfer"),
            "P/D stage requires read or write");
    PdPathStage result{text(segment.at("id")), kind, text(resource.at("id")), "", rank, device,
        segment.at("operation") == "read" ? MemoryOperation::Read : MemoryOperation::Write,
        slice.bytes, nullptr};
    const auto ref = text(resource.at("owner_ref"));
    if (kind == "interface") {
        result.api = services.pd_interface(rank, result.resource);
        result.api->set_sys(rank, sys);
        result.device = 0;
        result.physical_resource = services.pd_path_config().bindings.at({rank, result.resource});
    } else if (kind == "memory") {
        require(location.at("tier_name") == ref, "P/D memory resource differs from actual buffer tier");
        result.api = sys->memory_api(tier_id, device);
        result.physical_resource = services.configuration().bindings.at({rank, "memory", ref, device});
    } else if (kind == "ucie") {
        const auto& link = sys->ucie_link(ref);
        require(device < link.stack_count, "P/D UCIe device is out of range");
        const auto rule = text(segment.at("byte_rule"));
        const auto operation = rule == "ucie_write_request" ? MemoryOperation::Write : MemoryOperation::Read;
        if (operation == MemoryOperation::Write) add(slice.bytes, link.header_bytes);
        const auto hops = ucie_transaction_hops(operation, slice.bytes, link.header_bytes);
        result.bytes = 0;
        for (const auto& hop : hops) {
            if ("ucie_" + std::string(hop.name) == rule) {
                require(hop.operation == result.operation, "P/D UCIe direction mismatch");
                result.bytes = hop.bytes;
            }
        }
        require(result.bytes > 0 || (rule == "ucie_read_request" && link.header_bytes == 0),
                "P/D UCIe byte rule has no actual transaction hop");
        result.api = link.api;
        result.physical_resource = services.configuration().bindings.at({rank, "ucie", ref, device});
    } else {
        require(kind == "network", "unknown P/D stage kind");
        result.physical_resource = ref;
    }
    return result;
}
}  // namespace

std::vector<std::vector<PdPathStage>> bind_pd_path_stages(
    const PdPathWork& work, const std::vector<Sys*>& systems,
    PhysicalServiceFactory& services, const Json& actual_network) {
    const auto resources = index(work.profile.at("resources"), "id");
    const auto buffers = index(work.context.at("buffers"), "buffer_id");
    std::set<uint32_t> checked;
    size_t network_stages = 0;
    for (const auto& segment : work.route.at("segments")) {
        const auto& resource = resources.at(text(segment.at("resource_id")));
        if (resource.at("kind") == "network") {
            network(work, resource, services.configuration(), actual_network);
            ++network_stages;
        }
    }
    require(network_stages <= 1, "P/D route repeats actual network service");
    std::vector<std::vector<PdPathStage>> result;
    for (const auto& slice : work.slices) {
        for (const auto& [rank, side] : std::vector<std::pair<uint32_t, size_t>>{
                {slice.source_rank, 0}, {slice.destination_rank, 1}}) {
            if (!checked.insert(rank).second) continue;
            auto* sys = systems.at(rank);
            sys->validate_tier_manifest_digest(text(work.context.at("endpoints")[side].at("manifest").at("manifest_digest")));
            sys->validate_service_metadata(services.identity().binding_digest, services.identity().activation_id, rank);
        }
        std::vector<PdPathStage> stages;
        for (const auto& segment : work.route.at("segments")) {
            stages.push_back(stage(work, slice, segment, resources.at(text(segment.at("resource_id"))),
                                   buffers, systems, services));
        }
        result.push_back(std::move(stages));
    }
    return result;
}
}  // namespace AstraSim
