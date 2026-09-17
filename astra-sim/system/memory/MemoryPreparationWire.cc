/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#include "MemoryPreparationWire.hh"

#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "ServiceBindingJson.hh"

namespace AstraSim {
namespace {

namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;

std::string text(const Json& value) {
    if (!value.is_string() || value.get<std::string>().empty()) {
        throw std::invalid_argument(
            "direct preparation requires a nonempty string");
    }
    return value.get<std::string>();
}

uint64_t uint64(const Json& value, const std::string& field) {
    if (!value.is_number_unsigned()) {
        throw std::invalid_argument(field + " must be uint64");
    }
    return value.get<uint64_t>();
}

std::vector<std::string> strings(
    const Json& value, const std::string& field) {
    if (!value.is_array()) {
        throw std::invalid_argument(field + " must be a string array");
    }
    std::vector<std::string> result;
    std::set<std::string> unique;
    for (const auto& item : value) {
        const auto parsed = text(item);
        if (!unique.insert(parsed).second) {
            throw std::invalid_argument(field + " contains a duplicate");
        }
        result.push_back(parsed);
    }
    return result;
}

void add_string(
    ChakraProtoMsg::Node& node,
    const std::string& name,
    const std::string& value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_string_val(value);
}

void add_uint64(
    ChakraProtoMsg::Node& node,
    const std::string& name,
    uint64_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_uint64_val(value);
}

void add_uint32(
    ChakraProtoMsg::Node& node,
    const std::string& name,
    uint32_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_uint32_val(value);
}

void add_strings(
    ChakraProtoMsg::Node& node,
    const std::string& name,
    const std::vector<std::string>& values) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    auto* list = attribute->mutable_string_list();
    for (const auto& value : values) {
        list->add_values(value);
    }
}

struct MovementInput {
    uint32_t source_tier;
    uint32_t source_device;
    uint32_t destination_tier;
    uint32_t destination_device;
    uint64_t bytes;
    std::vector<std::string> dependencies;
};

MovementInput validate_input(
    const Json& sidecar, const Json& event, uint32_t rank) {
    Wire::fields(
        sidecar,
        {"schema_version", "run_id", "instance_id", "manifest_digest",
         "selected_path", "events", "completion_owner"});
    if (sidecar.at("schema_version") != "memory-events-v1" ||
        sidecar.at("completion_owner") != "external_preparation") {
        throw std::invalid_argument(
            "direct preparation requires memory-events-v1 external work");
    }
    Wire::fields(
        event,
        {"event_id", "page_id", "transaction_id",
         "expected_residency_version", "home_domain_id",
         "source_iteration_id", "npu_id", "kind", "phase", "source",
         "destination", "bytes", "priority_class", "depends_on",
         "releases"});
    if (Wire::uint32(event.at("npu_id")) != rank ||
        !event.at("releases").is_array() ||
        !event.at("releases").empty()) {
        throw std::invalid_argument(
            "direct preparation event has invalid rank or releases");
    }
    const auto& source = event.at("source");
    const auto& destination = event.at("destination");
    Wire::fields(source, {"tier_id", "device_id"});
    Wire::fields(destination, {"tier_id", "device_id"});
    MovementInput input{
        Wire::uint32(source.at("tier_id")),
        Wire::uint32(source.at("device_id")),
        Wire::uint32(destination.at("tier_id")),
        Wire::uint32(destination.at("device_id")),
        uint64(event.at("bytes"), "movement bytes"),
        strings(event.at("depends_on"), "movement dependencies")};
    if (input.source_tier == 0 || input.bytes == 0) {
        throw std::invalid_argument(
            "direct preparation requires a nonzero source tier and bytes");
    }
    return input;
}

struct PathAttributes {
    std::string contract_status;
    std::string schema_version;
    std::string timing_provenance;
    std::vector<std::string> segment_ids;
    std::vector<std::string> segment_kinds;
    std::vector<std::string> segment_refs;
    std::vector<std::string> segment_operations;
    std::vector<std::string> segment_byte_rules;
};

PathAttributes parse_path(const Json& path) {
    PathAttributes result;
    result.contract_status = text(path.at("contract_status"));
    if (result.contract_status == "implemented") {
        Wire::fields(
            path,
            {"id", "engine_count", "max_priority_burst",
             "max_in_flight_page_movements", "resource_ids",
             "contract_status", "schema_version", "timing_provenance",
             "segments"});
        result.schema_version = text(path.at("schema_version"));
        result.timing_provenance = text(path.at("timing_provenance"));
        for (const auto& segment : Wire::array(path.at("segments"))) {
            Wire::fields(
                segment,
                {"id", "kind", "resource_ref", "operation", "byte_rule"});
            result.segment_ids.push_back(text(segment.at("id")));
            result.segment_kinds.push_back(text(segment.at("kind")));
            result.segment_refs.push_back(text(segment.at("resource_ref")));
            result.segment_operations.push_back(
                text(segment.at("operation")));
            result.segment_byte_rules.push_back(
                text(segment.at("byte_rule")));
        }
        if (result.segment_ids.empty()) {
            throw std::invalid_argument(
                "implemented direct preparation path requires segments");
        }
        return result;
    }
    if (result.contract_status != "compatibility_checkpoint") {
        throw std::invalid_argument(
            "direct preparation path contract status is unsupported");
    }
    Wire::fields(
        path,
        {"id", "engine_count", "max_priority_burst",
         "max_in_flight_page_movements", "resource_ids",
         "contract_status"});
    result.schema_version = "adr-0020-checkpoint";
    result.timing_provenance = "proxy_unimplemented";
    return result;
}

void add_identity_attributes(
    ChakraProtoMsg::Node& node, const Json& sidecar, const Json& event) {
    add_string(
        node, "memory_movement_schema_version",
        text(sidecar.at("schema_version")));
    add_string(
        node, "memory_movement_manifest_digest",
        text(sidecar.at("manifest_digest")));
    add_string(node, "movement_run_id", text(sidecar.at("run_id")));
    add_string(
        node, "movement_instance_id", text(sidecar.at("instance_id")));
    add_string(node, "movement_event_id", text(event.at("event_id")));
    add_uint32(
        node, "movement_source_iteration_id",
        Wire::uint32(event.at("source_iteration_id")));
    add_string(node, "movement_kind", text(event.at("kind")));
    add_string(node, "movement_phase", text(event.at("phase")));
    add_string(
        node, "movement_priority_class",
        text(event.at("priority_class")));
    add_string(node, "movement_page_id", text(event.at("page_id")));
    add_string(
        node, "movement_transaction_id",
        text(event.at("transaction_id")));
    add_uint32(
        node, "movement_expected_residency_version",
        Wire::uint32(event.at("expected_residency_version")));
    add_uint32(
        node, "movement_home_domain_id",
        Wire::uint32(event.at("home_domain_id")));
}

void add_path_attributes(
    ChakraProtoMsg::Node& node,
    const Json& path,
    const PathAttributes& values) {
    add_string(node, "movement_path_id", text(path.at("id")));
    add_string(
        node, "movement_path_schema_version", values.schema_version);
    add_string(
        node, "movement_path_contract_status", values.contract_status);
    add_string(
        node, "movement_path_timing_provenance",
        values.timing_provenance);
    add_uint32(
        node, "movement_engine_count",
        Wire::uint32(path.at("engine_count")));
    add_uint32(
        node, "movement_max_priority_burst",
        Wire::uint32(path.at("max_priority_burst")));
    add_uint32(
        node, "movement_max_in_flight_page_movements",
        Wire::uint32(path.at("max_in_flight_page_movements")));
    add_strings(
        node, "movement_resource_ids",
        strings(path.at("resource_ids"), "movement resource_ids"));
    add_strings(node, "movement_segment_ids", values.segment_ids);
    add_strings(node, "movement_segment_kinds", values.segment_kinds);
    add_strings(
        node, "movement_segment_resource_refs", values.segment_refs);
    add_strings(
        node, "movement_segment_operations", values.segment_operations);
    add_strings(
        node, "movement_segment_byte_rules", values.segment_byte_rules);
}

}  // namespace

std::shared_ptr<Chakra::ETFeederNode> direct_movement_node(
    const Json& sidecar,
    const Json& event,
    uint32_t rank,
    uint64_t node_id) {
    const auto& path = sidecar.at("selected_path");
    const auto input = validate_input(sidecar, event, rank);
    const auto path_attributes = parse_path(path);

    auto node = std::make_shared<ChakraProtoMsg::Node>();
    node->set_id(node_id);
    node->set_name(
        "MEMORY_MOVEMENT_" + text(event.at("event_id")));
    node->set_type(ChakraProtoMsg::MEM_LOAD_NODE);
    add_uint64(*node, "tensor_size", input.bytes);
    add_uint32(*node, "tensor_loc", input.source_tier);
    add_uint32(*node, "tensor_device", input.source_device);
    add_uint32(
        *node, "movement_destination_tier_id",
        input.destination_tier);
    add_uint32(
        *node, "movement_destination_device_id",
        input.destination_device);
    add_strings(
        *node, "movement_dependencies",
        input.dependencies);
    add_identity_attributes(*node, sidecar, event);
    add_path_attributes(*node, path, path_attributes);
    return std::make_shared<Chakra::ETFeederNode>(node);
}

}  // namespace AstraSim
