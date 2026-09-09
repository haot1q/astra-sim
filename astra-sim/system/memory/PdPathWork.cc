/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "PdPathWork.hh"

#include <filesystem>
#include <set>
#include "PdPathWire.hh"
#include "PdOwnerIdentity.hh"

namespace AstraSim {
namespace {
using namespace PdPathWire;
namespace Wire = ServiceBindingJson;

Json document(const Json& envelope, const std::string& name) {
    const auto raw = text(envelope.at(name + "_json"));
    require(Wire::digest_bytes(raw) == envelope.at(name + "_digest"), "P/D document digest mismatch");
    return Wire::parse(raw);
}

void identities(const Json& body, const PhysicalServiceConfig& physical,
                const PdPathServiceConfig& interfaces) {
    require(body.at("service_binding_digest") == physical.identity.binding_digest &&
            body.at("service_activation_id") == physical.identity.activation_id &&
            body.at("path_service_binding_digest") == interfaces.binding_digest &&
            body.at("profile_digest") == interfaces.profile_digest && !interfaces.binding_digest.empty(),
            "P/D work differs from actual service activation");
    const auto& context = body.at("context");
    const auto& binding = body.at("binding");
    require((context.at("schema_version") == "pd-memory-context-v1" ||
             context.at("schema_version") == "pd-memory-context-v2" ||
             context.at("schema_version") == "pd-memory-context-v3") &&
            binding.at("schema_version") == "pd-memory-path-binding-v1" &&
            body.at("descriptor").at("schema_version") == "pd-kv-transfer-v1" &&
            body.at("profile").at("schema_version") == "pd-memory-path-profile-v1",
            "unknown P/D nested schema");
    require(context.at("descriptor") == body.at("descriptor") &&
            context.at("descriptor_digest") == body.at("descriptor_digest") &&
            binding.at("descriptor_digest") == body.at("descriptor_digest") &&
            binding.at("profile_digest") == body.at("profile_digest") &&
            binding.at("context_digest") == body.at("context_digest"), "P/D nested identity mismatch");
    const auto& endpoints = Wire::array(context.at("endpoints"));
    require(endpoints.size() == 2, "P/D context requires two endpoints");
    std::set<uint32_t> used;
    for (size_t side = 0; side < 2; ++side) {
        const std::string label = side == 0 ? "source" : "destination";
        const auto& endpoint = endpoints[side].at("endpoint");
        require(endpoint.at("instance_id") == body.at("descriptor").at(label + "_instance_id") &&
                endpoints[side].at("manifest").at("manifest_digest") == binding.at(label + "_manifest_digest"),
                "P/D endpoint/manifest identity mismatch");
        auto first = Wire::uint32(endpoint.at("first_global_rank"));
        auto end = add(first, number(endpoint.at("num_npus"), true));
        require(end <= physical.ranks.size(), "P/D endpoint exceeds backend ranks");
        std::vector<uint32_t> endpoint_ranks;
        for (uint64_t rank = first; rank < end; ++rank) {
            require(used.insert(rank).second && physical.ranks.count(rank) &&
                    physical.ranks.at(rank).at("instance_id") == endpoint.at("instance_id"),
                    "P/D actual rank/instance mismatch");
            endpoint_ranks.push_back(rank);
        }
        std::set<uint32_t> actual_ranks;
        for (const auto& [rank, owner] : physical.ranks) {
            if (owner.at("instance_id") == endpoint.at("instance_id")) actual_ranks.insert(rank);
        }
        require(actual_ranks == std::set<uint32_t>(endpoint_ranks.begin(), endpoint_ranks.end()),
                "P/D endpoint must cover every actual instance rank");
        if (context.at("schema_version") == "pd-memory-context-v3") {
            validate_pd_pipeline_context(endpoints[side], physical, endpoint_ranks);
        } else {
            validate_pd_context_owner(endpoints[side], context.at("schema_version") == "pd-memory-context-v2",
                                      physical, endpoint_ranks);
        }
    }
    validate_pd_descriptor_geometry(body.at("descriptor"), endpoints);
    std::set<uint32_t> sources, destinations;
    for (const auto& pair : Wire::array(body.at("descriptor").at("rank_pairs"))) {
        for (const auto* side : {"source", "destination"}) {
            const auto rank = Wire::uint32(pair.at(std::string(side) + "_rank"));
            const bool source = std::string(side) == "source";
            auto& ranks = source ? sources : destinations;
            require(used.count(rank) && ranks.insert(rank).second && physical.ranks.at(rank).at("instance_id") ==
                    body.at("descriptor").at(std::string(side) + "_instance_id"), "P/D pair owner mismatch");
        }
    }
    require(sources.size() + destinations.size() == used.size(), "P/D pairs omit actual endpoint ranks");
}

void service_order(const Json& segments, const std::map<std::string, Json>& resources) {
    std::vector<size_t> reads, writes, source_ucie, destination_ucie;
    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& segment = segments[i];
        const auto& resource = resources.at(text(segment.at("resource_id")));
        if (resource.at("kind") == "memory") {
            const bool read = resource.at("side") == "source" && segment.at("operation") == "read";
            require(read || (resource.at("side") == "destination" && segment.at("operation") == "write"),
                    "P/D memory service side/operation mismatch");
            (read ? reads : writes).push_back(i);
        } else if (resource.at("kind") == "ucie") {
            require(resource.at("side") == "source" || resource.at("side") == "destination",
                    "P/D UCIe requires local endpoint side");
            (resource.at("side") == "source" ? source_ucie : destination_ucie).push_back(i);
        }
    }
    require(reads.size() == 1 && writes.size() == 1 && reads[0] < writes[0] &&
            writes[0] == segments.size() - 1, "P/D requires exactly source read then terminal destination write");
    if (source_ucie.empty()) {
        require(reads[0] == 0, "P/D source read must be first");
    } else {
        require(source_ucie.size() == 2 && source_ucie[0] == 0 && reads[0] == 1 && source_ucie[1] == 2 &&
                segments[0].at("byte_rule") == "ucie_read_request" &&
                segments[2].at("byte_rule") == "ucie_read_response" &&
                segments[0].at("resource_id") == segments[2].at("resource_id") &&
                segments[0].at("from_port") == segments[2].at("to_port") &&
                segments[1].at("from_port") == segments[1].at("to_port"),
                "P/D UCIe source must be request/memory/response");
    }
    if (!destination_ucie.empty()) {
        require(destination_ucie.size() == 1 && destination_ucie[0] + 1 == writes[0] &&
                segments[destination_ucie[0]].at("byte_rule") == "ucie_write_request" &&
                segments[writes[0]].at("from_port") == segments[writes[0]].at("to_port"),
                "P/D UCIe destination must be request/memory");
    }
}

Json route(const Json& profile) {
    const auto resources = index(profile.at("resources"), "id");
    const auto routes = index(profile.at("routes"), "id");
    const auto& selection = profile.at("selection");
    require(selection.at("initiation") == "push" || selection.at("initiation") == "pull",
            "unsupported P/D initiation");
    require(selection.at("release") == "whole_prefill", "P/D chunk release is not enabled in this executor");
    auto result = routes.at(text(selection.at("route_id")));
    const auto quantum = number(result.at("service_quantum_bytes"), true);
    require(number(result.at("in_flight_bytes"), true) >= quantum, "P/D window is smaller than quantum");
    const auto& segments = Wire::array(result.at("segments"));
    require(!segments.empty(), "P/D route is empty");
    std::set<std::string> ids;
    std::string cursor = text(segments[0].at("from_port"));
    for (const auto& segment : segments) {
        Wire::fields(segment, {"id", "resource_id", "from_port", "to_port", "operation", "byte_rule"});
        require(ids.insert(text(segment.at("id"))).second && cursor == segment.at("from_port"),
                "P/D duplicate segment or disconnected route");
        cursor = text(segment.at("to_port"));
        const auto& resource = resources.at(text(segment.at("resource_id")));
        const auto kind = text(resource.at("kind"));
        require(kind == "memory" || kind == "interface" || kind == "network" || kind == "ucie",
                "unsupported P/D service kind");
        const Json connection = {{"from_port", segment.at("from_port")}, {"to_port", segment.at("to_port")},
                                 {"operation", segment.at("operation")}};
        const auto& connections = Wire::array(resource.at("connections"));
        require(std::find(connections.begin(), connections.end(), connection) != connections.end(),
                "P/D segment is not a declared resource connection");
        const auto rule = text(segment.at("byte_rule"));
        require(kind == "ucie" ? (rule == "ucie_read_request" || rule == "ucie_read_response" ||
                                   rule == "ucie_write_request") : rule == "payload", "unsupported P/D byte rule");
    }
    service_order(segments, resources);
    return result;
}
}  // namespace

PdPathWork load_pd_path_work(const std::string& path, const PhysicalServiceConfig& physical,
                            const PdPathServiceConfig& interfaces) {
    // This is a defensive input limit, not a transfer/window/page-size limit.
    require(std::filesystem::file_size(path) <= 64 * 1024 * 1024, "P/D work sidecar exceeds 64 MiB");
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "descriptor_json", "descriptor_digest", "profile_json",
        "profile_digest", "context_json", "context_digest", "binding_json", "binding_digest",
        "service_binding_digest", "service_activation_id", "path_service_binding_digest", "slices", "work_digest"});
    require(body["schema_version"] == "pd-path-work-v1", "unsupported P/D work schema");
    const auto digest = text(body["work_digest"]);
    body.erase("work_digest");
    require(Wire::digest(body, true) == digest, "P/D work digest mismatch");
    for (const auto* name : {"descriptor", "profile", "context", "binding"}) body[name] = document(body, name);
    identities(body, physical, interfaces);
    auto selected = route(body["profile"]);
    auto slices = validate_pd_path_ranges(body, number(selected["service_quantum_bytes"], true));
    return {body["descriptor"], body["context"], body["binding"], body["profile"], selected,
            digest, text(body["descriptor_digest"]), std::move(slices)};
}
}  // namespace AstraSim
