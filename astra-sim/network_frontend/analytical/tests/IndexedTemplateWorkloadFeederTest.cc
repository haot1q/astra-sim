/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "astra-sim/workload/IndexedTemplatePlan.hh"
#include "astra-sim/workload/IndexedTemplateWorkloadFeeder.hh"

namespace {

using Json = nlohmann::json;
using AstraSim::IndexedTemplatePlan;
using AstraSim::IndexedTemplateWorkloadFeeder;

Json expression(const std::string& operation, Json arguments) {
    return Json{{operation, std::move(arguments)}};
}

Json memory_attributes(uint64_t location) {
    return {
        {"tensor_size",
         expression("min",
                    Json::array(
                        {2U,
                         expression(
                             "sub",
                             Json::array(
                                 {expression("param", "history"),
                                  expression(
                                      "mul",
                                      Json::array(
                                          {expression("index", true), 2U}))}))}))},
        {"tensor_loc", location},
        {"tensor_device", 0U},
        {"tensor_channel", 0U},
    };
}

Json compute_attributes() {
    return {
        {"duration_ns", 3U},
        {"num_ops", expression("param", "batch_size")},
        {"tensor_size", expression("param", "history")},
        {"is_cpu_op", false},
    };
}

Json definition() {
    const Json ports = Json::array({
        {{"name", "history"}, {"type", "uint64"}},
        {{"name", "batch_size"}, {"type", "uint64"}},
    });
    const Json domains = Json::array({
        {{"id", "tiles"},
         {"extent",
          expression(
              "ceil_div",
              Json::array({expression("param", "history"), 2U}))}},
    });
    const Json recipes = Json::array({
        {{"id", "read_a"},
         {"kind", "memory_load"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(1U)}},
        {{"id", "read_b"},
         {"kind", "memory_load"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(2U)}},
        {{"id", "compute"},
         {"kind", "compute"},
         {"domain", "tiles"},
         {"attrs", compute_attributes()}},
        {{"id", "write"},
         {"kind", "memory_store"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(1U)}},
        {{"id", "layer1_read_a"},
         {"kind", "memory_load"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(1U)}},
        {{"id", "layer1_read_b"},
         {"kind", "memory_load"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(2U)}},
        {{"id", "layer1_compute"},
         {"kind", "compute"},
         {"domain", "tiles"},
         {"attrs", compute_attributes()}},
        {{"id", "layer1_write"},
         {"kind", "memory_store"},
         {"domain", "tiles"},
         {"attrs", memory_attributes(1U)}},
        {{"id", "all_reduce"},
         {"kind", "collective"},
         {"domain", nullptr},
         {"attrs",
          {{"comm_type", "all_reduce"},
           {"comm_size", expression("param", "history")},
           {"comm_priority", 0U},
           {"involved_dim", Json::array({true})}}}},
        {{"id", "send"},
         {"kind", "send"},
         {"domain", nullptr},
         {"attrs",
          {{"comm_size", expression("param", "history")},
           {"comm_src", 0U},
           {"comm_dst", 1U},
           {"comm_tag", 9U}}}},
        {{"id", "recv"},
         {"kind", "recv"},
         {"domain", nullptr},
         {"attrs",
          {{"comm_size", expression("param", "history")},
           {"comm_src", 0U},
           {"comm_dst", 1U},
           {"comm_tag", 9U}}}},
    });
    const Json edges = Json::array({
        {{"from", "read_a"}, {"to", "compute"}, {"relation", "same_index"}},
        {{"from", "read_b"}, {"to", "compute"}, {"relation", "same_index"}},
        {{"from", "compute"}, {"to", "write"}, {"relation", "same_index"}},
        {{"from", "write"}, {"to", "layer1_read_a"}, {"relation", "same_index"}},
        {{"from", "write"}, {"to", "layer1_read_b"}, {"relation", "same_index"}},
        {{"from", "layer1_read_a"},
         {"to", "layer1_compute"},
         {"relation", "same_index"}},
        {{"from", "layer1_read_b"},
         {"to", "layer1_compute"},
         {"relation", "same_index"}},
        {{"from", "layer1_compute"},
         {"to", "layer1_write"},
         {"relation", "same_index"}},
        {{"from", "layer1_write"},
         {"to", "all_reduce"},
         {"relation", "all_to_one"}},
        {{"from", "all_reduce"}, {"to", "send"}, {"relation", "one_to_one"}},
        {{"from", "send"}, {"to", "recv"}, {"relation", "one_to_one"}},
    });
    Json result = {
        {"schema_version", "template-definition-v3-proof"},
        {"template_id", "indexed-proof"},
        {"definition_digest", ""},
        {"tier_manifest_digest", "sha256:tier"},
        {"service_binding_digest", "sha256:service"},
        {"service_activation_id", "activation"},
        {"ports", ports},
        {"domains", domains},
        {"recipes", recipes},
        {"edges", edges},
    };
    result["definition_digest"] =
        AstraSim::IndexedTemplatePlan::definitionDigest(result);
    return result;
}

Json invocation(const Json& definition, uint64_t history, uint64_t batch_size) {
    const auto peer_history =
        history > std::numeric_limits<uint64_t>::max() - 2
            ? history
            : history + 2;
    return {
        {"schema_version", "template-invocation-v3-proof"},
        {"definition_id", definition.at("template_id")},
        {"definition_digest", definition.at("definition_digest")},
        {"ranks",
         {{{"rank", 0U},
           {"bindings",
            {{"history", history}, {"batch_size", batch_size}}}},
          {{"rank", 1U},
           {"bindings",
            {{"history", peer_history}, {"batch_size", batch_size}}}}}},
    };
}

struct Observed {
    uint64_t event_count = 0;
    std::vector<uint64_t> memory_bytes;
    std::vector<uint32_t> memory_locations;
    std::vector<uint64_t> compute_ops;
    std::vector<uint32_t> point_to_point_tags;
    std::vector<uint64_t> communication_bytes;
    std::vector<int> types;
    bool child_contract_valid = true;
};

Observed drain(IndexedTemplateWorkloadFeeder& feeder) {
    Observed result;
    while (feeder.hasNodesToIssue()) {
        auto node = feeder.getNextIssuableNode();
        if (node == nullptr) return {};
        ++result.event_count;
        result.types.push_back(node->type());
        if (node->type() == ChakraProtoMsg::MEM_LOAD_NODE ||
            node->type() == ChakraProtoMsg::MEM_STORE_NODE) {
            result.memory_bytes.push_back(node->tensor_size());
            result.memory_locations.push_back(node->tensor_loc());
        } else if (node->type() == ChakraProtoMsg::COMP_NODE) {
            result.compute_ops.push_back(node->num_ops());
        } else if (node->type() == ChakraProtoMsg::COMM_COLL_NODE) {
            result.communication_bytes.push_back(node->comm_size());
        } else if (node->type() == ChakraProtoMsg::COMM_SEND_NODE ||
                   node->type() == ChakraProtoMsg::COMM_RECV_NODE) {
            result.communication_bytes.push_back(node->comm_size());
            result.point_to_point_tags.push_back(node->comm_tag());
        }
        const auto children = feeder.childNodes(node->id());
        const auto linked = node->getChildren();
        for (const auto& child : children) {
            const auto& dependencies =
                child->getChakraNode()->data_deps();
            result.child_contract_valid &=
                std::find(dependencies.begin(), dependencies.end(),
                          node->id()) != dependencies.end() &&
                std::find(linked.begin(), linked.end(), child) != linked.end();
        }
        if (node->type() == ChakraProtoMsg::MEM_LOAD_NODE) {
            const auto repeated = feeder.childNodes(node->id());
            result.child_contract_valid &=
                repeated.size() == children.size() &&
                node->getChildren().size() == children.size();
        }
        feeder.freeChildrenNodes(node->id());
        feeder.removeNode(node->id());
    }
    return result;
}

bool dynamic_extent_preserves_events_and_sparse_state() {
    const auto raw_definition = definition();
    auto first_plan = IndexedTemplatePlan::compile(
        raw_definition, invocation(raw_definition, 4U, 2U), 0);
    IndexedTemplateWorkloadFeeder first(std::move(first_plan));
    const auto first_work = drain(first);
    if (first_work.event_count != 19 ||
        first_work.memory_bytes !=
            std::vector<uint64_t>(12, 2U) ||
        first_work.compute_ops != std::vector<uint64_t>(4, 2U) ||
        std::count(first_work.types.begin(), first_work.types.end(),
                   ChakraProtoMsg::COMM_COLL_NODE) != 1 ||
        std::count(first_work.types.begin(), first_work.types.end(),
                   ChakraProtoMsg::COMM_SEND_NODE) != 1 ||
        std::count(first_work.types.begin(), first_work.types.end(),
                   ChakraProtoMsg::COMM_RECV_NODE) != 1 ||
        !first_work.child_contract_valid ||
        first.peakTrackedEventCount() > 7 ||
        first.trackedEventCount() != 0) {
        return false;
    }

    auto second_plan = IndexedTemplatePlan::compile(
        raw_definition, invocation(raw_definition, 5U, 4U), 0);
    const auto join =
        second_plan.event(second_plan.eventId(8, 0));
    if (second_plan.requiredParentCount(join) != 3) return false;
    for (uint64_t tile = 0; tile < 3; ++tile) {
        const auto parent =
            second_plan.event(second_plan.eventId(7, tile));
        const auto children = second_plan.children(parent);
        if (children.size() != 1 || children.front().id != join.id ||
            second_plan.exposeChildAfterCompletion(
                parent, join, tile) != (tile == 2)) {
            return false;
        }
    }
    IndexedTemplateWorkloadFeeder second(std::move(second_plan));
    const auto second_work = drain(second);
    std::vector<int> expected_types;
    for (int tile = 0; tile < 3; ++tile) {
        (void)tile;
        expected_types.insert(
            expected_types.end(),
            {ChakraProtoMsg::MEM_LOAD_NODE,
             ChakraProtoMsg::MEM_LOAD_NODE,
             ChakraProtoMsg::COMP_NODE,
             ChakraProtoMsg::MEM_STORE_NODE,
             ChakraProtoMsg::MEM_LOAD_NODE,
             ChakraProtoMsg::MEM_LOAD_NODE,
             ChakraProtoMsg::COMP_NODE,
             ChakraProtoMsg::MEM_STORE_NODE});
    }
    expected_types.insert(
        expected_types.end(),
        {ChakraProtoMsg::COMM_COLL_NODE,
         ChakraProtoMsg::COMM_SEND_NODE,
         ChakraProtoMsg::COMM_RECV_NODE});
    if (second_work.event_count != 27 ||
        second_work.memory_bytes !=
            std::vector<uint64_t>(
                {2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
                 1, 1, 1, 1, 1, 1}) ||
        second_work.compute_ops != std::vector<uint64_t>(6, 4U) ||
        second_work.memory_locations !=
            std::vector<uint32_t>(
                {1, 2, 1, 1, 2, 1, 1, 2, 1,
                 1, 2, 1, 1, 2, 1, 1, 2, 1}) ||
        second_work.communication_bytes !=
            std::vector<uint64_t>({5, 5, 5}) ||
        second_work.point_to_point_tags !=
            std::vector<uint32_t>({9, 9}) ||
        second_work.types != expected_types ||
        !second_work.child_contract_valid ||
        second.peakTrackedEventCount() > 9 ||
        second.trackedEventCount() != 0) {
        return false;
    }

    auto long_plan = IndexedTemplatePlan::compile(
        raw_definition, invocation(raw_definition, 2001U, 4U), 0);
    IndexedTemplateWorkloadFeeder long_run(std::move(long_plan));
    const auto long_work = drain(long_run);
    auto peer_plan = IndexedTemplatePlan::compile(
        raw_definition, invocation(raw_definition, 4U, 2U), 1);
    IndexedTemplateWorkloadFeeder peer(std::move(peer_plan));
    const auto peer_work = drain(peer);
    return long_work.event_count == 8011 &&
           long_run.peakTrackedEventCount() <= 9 &&
           long_run.trackedEventCount() == 0 &&
           long_run.materializedEventCount() == 0 &&
           peer.serviceRank() == 1 && peer_work.event_count == 27;
}

bool issue_all_ready_tracks_the_actual_dag_width() {
    const auto raw_definition = definition();
    IndexedTemplateWorkloadFeeder feeder(
        IndexedTemplatePlan::compile(
            raw_definition, invocation(raw_definition, 2001U, 4U), 0));
    uint64_t issued = 0;
    while (feeder.getNextIssuableNode() != nullptr) ++issued;
    return issued == 2002 &&
           feeder.trackedEventCount() == issued &&
           feeder.materializedEventCount() == issued &&
           feeder.peakTrackedEventCount() == issued;
}

bool invalid_inputs_fail_before_any_event() {
    const auto raw_definition = definition();
    auto production_schema = raw_definition;
    production_schema["schema_version"] = "template-definition-v3";
    production_schema["definition_digest"] =
        IndexedTemplatePlan::definitionDigest(production_schema);
    try {
        IndexedTemplatePlan::compile(
            production_schema,
            invocation(production_schema, 5U, 4U), 0);
        return false;
    } catch (const std::invalid_argument&) {
    }

    auto bad_kind = raw_definition;
    bad_kind["recipes"][0]["kind"] = "merged_memory_bill";
    bad_kind["definition_digest"] =
        IndexedTemplatePlan::definitionDigest(bad_kind);
    try {
        IndexedTemplatePlan::compile(
            bad_kind, invocation(bad_kind, 5U, 4U), 0);
        return false;
    } catch (const std::invalid_argument&) {
    }

    auto stale = invocation(raw_definition, 5U, 4U);
    stale["definition_digest"] = "sha256:stale";
    try {
        IndexedTemplatePlan::compile(raw_definition, stale, 0);
        return false;
    } catch (const std::invalid_argument&) {
    }

    auto zero = invocation(raw_definition, 0U, 4U);
    try {
        IndexedTemplateWorkloadFeeder feeder(
            IndexedTemplatePlan::compile(raw_definition, zero, 0));
        return false;
    } catch (const std::invalid_argument&) {
    }
    auto overflow = invocation(
        raw_definition, std::numeric_limits<uint64_t>::max(), 4U);
    try {
        IndexedTemplatePlan::compile(raw_definition, overflow, 0);
        return false;
    } catch (const std::invalid_argument&) {
    }
    return true;
}

Json read_json(const char* path) {
    std::ifstream stream(path);
    if (!stream) {
        throw std::invalid_argument(
            std::string("cannot read indexed template file ") + path);
    }
    Json result;
    stream >> result;
    return result;
}

// Cross-language check: a definition/invocation pair written by the Python
// wire must compile and drain here without any local JSON construction.
bool external_wire_drains(const char* definition_path,
                          const char* invocation_path,
                          const char* rank_text) {
    IndexedTemplateWorkloadFeeder feeder(IndexedTemplatePlan::compile(
        read_json(definition_path), read_json(invocation_path),
        static_cast<uint32_t>(std::stoul(rank_text))));
    const auto work = drain(feeder);
    std::cout << "INDEXED_TEMPLATE_DRAIN events=" << work.event_count
              << " peak=" << feeder.peakTrackedEventCount()
              << " tracked=" << feeder.trackedEventCount() << std::endl;
    return work.child_contract_valid && feeder.trackedEventCount() == 0 &&
           feeder.materializedEventCount() == 0;
}

}  // namespace

int main() {
    const char* definition_path = std::getenv("ASTRA_INDEXED_DEFINITION_PATH");
    const char* invocation_path = std::getenv("ASTRA_INDEXED_INVOCATION_PATH");
    const char* rank_text = std::getenv("ASTRA_INDEXED_RANK");
    if (definition_path != nullptr || invocation_path != nullptr ||
        rank_text != nullptr) {
        if (definition_path == nullptr || invocation_path == nullptr ||
            rank_text == nullptr) {
            return 1;
        }
        return external_wire_drains(definition_path, invocation_path, rank_text)
                   ? 0
                   : 1;
    }
    return dynamic_extent_preserves_events_and_sparse_state() &&
                   issue_all_ready_tracks_the_actual_dag_width() &&
                   invalid_inputs_fail_before_any_event()
               ? 0
               : 1;
}
