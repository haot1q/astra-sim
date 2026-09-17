/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/workload/HardwareResource.hh"
#include "astra-sim/workload/TemplateRegistry.hh"
#include "astra-sim/workload/TemplateWorkloadFeeder.hh"
#include "astra-sim/workload/WorkloadFeeder.hh"
#include "protoio.hh"

namespace {

using Json = nlohmann::json;
using AstraSim::ChakraEtWorkloadFeeder;
using AstraSim::ServiceBindingJson::digest;
using AstraSim::TemplateRegistry;
using AstraSim::TemplateWorkloadFeeder;

std::string write_json(const std::string& name, const Json& value) {
    const auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream output(path);
    output << value;
    return path.string();
}

Json compute_attrs(const Json& duration) {
    return {{"duration_ns", duration},
            {"num_ops", 0U},
            {"tensor_size", 1U},
            {"is_cpu_op", false}};
}

Json definition_with_templates(Json templates) {
    Json result = {
        {"schema_version", "template-definition-v2"},
        {"template_id", "nested-test"},
        {"definition_digest", ""},
        {"tier_manifest_digest", "sha256:tier"},
        {"service_binding_digest", "sha256:service"},
        {"service_activation_id", "activation"},
        {"templates", std::move(templates)}};
    const Json identity = {
        {"schema_version", result.at("schema_version")},
        {"template_id", result.at("template_id")},
        {"tier_manifest_digest", result.at("tier_manifest_digest")},
        {"service_binding_digest", result.at("service_binding_digest")},
        {"service_activation_id", result.at("service_activation_id")},
        {"templates", result.at("templates")}};
    result["definition_digest"] = digest(identity, true);
    return result;
}

Json valid_definition() {
    const Json child = {
        {"id", "child"},
        {"ports", {{{"name", "delay"}, {"type", "uint64"}}}},
        {"nodes",
         {{{"id", "leaf"},
           {"kind", "compute"},
           {"deps", Json::array()},
           {"attrs", compute_attrs("$delay")}},
          {{"id", "middle"},
           {"kind", "compute"},
           {"deps", {"leaf"}},
           {"attrs", compute_attrs("$delay")}},
          {{"id", "terminal"},
           {"kind", "compute"},
           {"deps", {"middle"}},
           {"attrs", compute_attrs("$delay")}}}}};
    const Json parent = {
        {"id", "parent"},
        {"ports", {{{"name", "parent_delay"}, {"type", "uint64"}}}},
        {"nodes",
         {{{"id", "first"},
           {"kind", "call"},
           {"deps", Json::array()},
           {"template", "child"},
           {"bindings", {{"delay", "$parent_delay"}}}},
          {{"id", "second"},
           {"kind", "call"},
           {"deps", {"first"}},
           {"template", "child"},
           {"bindings", {{"delay", "$parent_delay"}}}}}}};
    return definition_with_templates({child, parent});
}

Json invocation(const Json& definition, uint64_t delay) {
    return {{"schema_version", "template-invocation-v2"},
            {"definition_id", definition.at("template_id")},
            {"definition_digest", definition.at("definition_digest")},
            {"ranks",
             {{{"rank", 0U},
               {"root_template", "parent"},
               {"bindings", {{"parent_delay", delay}}}}}}};
}

std::vector<uint64_t> drain(AstraSim::WorkloadFeeder& feeder) {
    std::vector<uint64_t> runtimes;
    while (feeder.hasNodesToIssue()) {
        auto node = feeder.getNextIssuableNode();
        if (node == nullptr) return {};
        runtimes.push_back(node->runtime());
        feeder.freeChildrenNodes(node->id());
        feeder.removeNode(node->id());
    }
    return runtimes;
}

bool repeated_nested_invocations_are_isolated() {
    TemplateRegistry registry;
    const auto definition_json = valid_definition();
    const auto definition_path =
        write_json("astra-template-v2-definition.json", definition_json);
    const auto invocation_path =
        write_json("astra-template-v2-invocation.json",
                   invocation(definition_json, 7U));
    const auto definition = registry.loadDefinition(definition_path);
    bool wrong_rank_rejected = false;
    try {
        registry.loadInvocation(invocation_path, 1, *definition);
    } catch (const std::invalid_argument&) {
        wrong_rank_rejected = true;
    }
    if (!wrong_rank_rejected) return false;
    {
        TemplateWorkloadFeeder feeder(
            definition, registry.loadInvocation(invocation_path, 0, *definition),
            &registry);
        if (feeder.frameCount() != 3 || registry.activeFrameCount() != 3 ||
            feeder.materializedLeafCount() != 1) {
            return false;
        }
        auto first = feeder.getNextIssuableNode();
        const auto children = feeder.childNodes(first->id());
        if (children.size() != 1 || first->getChildren().size() != 1 ||
            first->getChildren().front()->id() != children.front()->id() ||
            feeder.materializedLeafCount() != 2) {
            return false;
        }
        feeder.freeChildrenNodes(first->id());
        feeder.removeNode(first->id());
        auto remaining = drain(feeder);
        if (first->runtime() != 7 ||
            remaining != std::vector<uint64_t>({7, 7, 7, 7, 7}) ||
            feeder.materializedLeafCount() != 0 ||
            feeder.peakMaterializedLeafCount() != 2 ||
            registry.activeFrameCount() != 0) {
            return false;
        }
    }
    write_json("astra-template-v2-invocation.json",
               invocation(definition_json, 11U));
    {
        TemplateWorkloadFeeder feeder(
            registry.loadDefinition(definition_path),
            registry.loadInvocation(invocation_path, 0, *definition), &registry);
        if (feeder.materializedLeafCount() != 1 ||
            drain(feeder) != std::vector<uint64_t>({11, 11, 11, 11, 11, 11})) {
            return false;
        }
    }
    auto replacement = definition_json;
    replacement["service_activation_id"] = "replacement-activation";
    replacement = definition_with_templates(replacement.at("templates"));
    replacement["service_activation_id"] = "replacement-activation";
    const Json replacement_identity = {
        {"schema_version", replacement.at("schema_version")},
        {"template_id", replacement.at("template_id")},
        {"tier_manifest_digest", replacement.at("tier_manifest_digest")},
        {"service_binding_digest", replacement.at("service_binding_digest")},
        {"service_activation_id", replacement.at("service_activation_id")},
        {"templates", replacement.at("templates")}};
    replacement["definition_digest"] = digest(replacement_identity, true);
    write_json("astra-template-v2-definition.json", replacement);
    bool replacement_rejected = false;
    try {
        registry.loadDefinition(definition_path);
    } catch (const std::invalid_argument&) {
        replacement_rejected = true;
    }
    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    return replacement_rejected && registry.definitionLoadCount() == 1 &&
           registry.planCompilationCount() == 2 &&
           registry.invocationCount() == 2 &&
           registry.activeFrameCount() == 0 &&
           registry.peakFrameCount() == 3 &&
           registry.materializedLeafCount() == 0 &&
           registry.peakMaterializedLeafCount() == 2;
}

bool rejects_bad_binding_and_call_cycle() {
    TemplateRegistry registry;
    auto bad_binding = valid_definition();
    bad_binding["templates"][1]["nodes"][0]["bindings"] =
        Json{{"unknown", "$parent_delay"}};
    bad_binding = definition_with_templates(bad_binding.at("templates"));
    const auto binding_path =
        write_json("astra-template-v2-bad-binding.json", bad_binding);
    bool binding_rejected = false;
    try {
        registry.loadDefinition(binding_path);
    } catch (const std::invalid_argument&) {
        binding_rejected = true;
    }
    const Json first = {
        {"id", "first"}, {"ports", Json::array()},
        {"nodes", {{{"id", "call"}, {"kind", "call"},
                    {"deps", Json::array()}, {"template", "second"},
                    {"bindings", Json::object()}}}}};
    const Json second = {
        {"id", "second"}, {"ports", Json::array()},
        {"nodes", {{{"id", "call"}, {"kind", "call"},
                    {"deps", Json::array()}, {"template", "first"},
                    {"bindings", Json::object()}}}}};
    const auto cycle_path = write_json(
        "astra-template-v2-cycle.json",
        definition_with_templates({first, second}));
    bool cycle_rejected = false;
    try {
        registry.loadDefinition(cycle_path);
    } catch (const std::invalid_argument&) {
        cycle_rejected = true;
    }
    std::remove(binding_path.c_str());
    std::remove(cycle_path.c_str());
    return binding_rejected && cycle_rejected;
}

bool sibling_compute_nodes_share_the_workload_resource() {
    TemplateRegistry registry;
    auto definition_json = valid_definition();
    definition_json["templates"][1]["nodes"][1]["deps"] = Json::array();
    definition_json =
        definition_with_templates(definition_json.at("templates"));
    const auto definition_path =
        write_json("astra-template-v2-resource-definition.json", definition_json);
    const auto invocation_path =
        write_json("astra-template-v2-resource-invocation.json",
                   invocation(definition_json, 5U));
    const auto definition = registry.loadDefinition(definition_path);
    TemplateWorkloadFeeder feeder(
        definition, registry.loadInvocation(invocation_path, 0, *definition),
        &registry);
    auto first = feeder.getNextIssuableNode();
    auto second = feeder.getNextIssuableNode();
    AstraSim::HardwareResource resource(1);
    const bool initially_available =
        first != nullptr && second != nullptr &&
        resource.is_available(first) && resource.is_available(second);
    resource.occupy(first);
    const bool serialized = !resource.is_available(second);
    resource.release(first);
    feeder.freeChildrenNodes(first->id());
    feeder.removeNode(first->id());
    feeder.freeChildrenNodes(second->id());
    feeder.removeNode(second->id());
    const bool remainder_completed =
        drain(feeder) == std::vector<uint64_t>({5, 5, 5, 5});
    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    return initially_available && serialized && remainder_completed &&
           registry.activeFrameCount() == 0;
}

bool recorded_compute_allows_zero_tensor_size() {
    TemplateRegistry registry;
    auto definition_json = valid_definition();
    for (auto& node : definition_json["templates"][0]["nodes"]) {
        node["attrs"]["tensor_size"] = 0U;
    }
    definition_json =
        definition_with_templates(definition_json.at("templates"));
    const auto definition_path =
        write_json("astra-template-v2-zero-tensor-definition.json",
                   definition_json);
    const auto invocation_path =
        write_json("astra-template-v2-zero-tensor-invocation.json",
                   invocation(definition_json, 3U));
    const auto definition = registry.loadDefinition(definition_path);
    TemplateWorkloadFeeder feeder(
        definition, registry.loadInvocation(invocation_path, 0, *definition),
        &registry);
    auto first = feeder.getNextIssuableNode();
    const bool accepted = first != nullptr && first->tensor_size() == 0;
    feeder.freeChildrenNodes(first->id());
    feeder.removeNode(first->id());
    const bool completed =
        drain(feeder) == std::vector<uint64_t>({3, 3, 3, 3, 3});
    bool roofline_rejected = false;
    try {
        TemplateWorkloadFeeder roofline_feeder(
            definition, registry.loadInvocation(invocation_path, 0, *definition),
            &registry, true);
    } catch (const std::invalid_argument&) {
        roofline_rejected = true;
    }
    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    return accepted && completed && roofline_rejected &&
           registry.activeFrameCount() == 0 &&
           registry.materializedLeafCount() == 0;
}

bool native_memory_tier_ids_are_not_limited_to_legacy_slots() {
    TemplateRegistry registry;
    const Json memory = {
        {"id", "memory"},
        {"ports", Json::array()},
        {"nodes",
         {{{"id", "load"},
           {"kind", "memory_load"},
           {"deps", Json::array()},
           {"attrs",
            {{"tensor_size", 4096U},
             {"tensor_loc", 17U},
             {"tensor_device", 2U},
             {"tensor_channel", 0U}}}}}}};
    const auto definition_json = definition_with_templates({memory});
    const auto definition_path =
        write_json("astra-template-v2-native-memory-definition.json",
                   definition_json);
    const Json invocation_json = {
        {"schema_version", "template-invocation-v2"},
        {"definition_id", definition_json.at("template_id")},
        {"definition_digest", definition_json.at("definition_digest")},
        {"ranks",
         {{{"rank", 0U},
           {"root_template", "memory"},
           {"bindings", Json::object()}}}}};
    const auto invocation_path =
        write_json("astra-template-v2-native-memory-invocation.json",
                   invocation_json);
    const auto definition = registry.loadDefinition(definition_path);
    TemplateWorkloadFeeder feeder(
        definition, registry.loadInvocation(invocation_path, 0, *definition),
        &registry);
    const auto node = feeder.getNextIssuableNode();
    const bool accepted =
        node != nullptr && node->tensor_loc() == 17 && node->tensor_size() == 4096;
    feeder.freeChildrenNodes(node->id());
    feeder.removeNode(node->id());
    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    return accepted && !feeder.hasNodesToIssue() &&
           registry.activeFrameCount() == 0 &&
           registry.materializedLeafCount() == 0;
}

// The nested definition lowers to one chain of six compute leaves. Write that
// same chain as a real Chakra ET so the reference feeder and the template
// feeder can be compared directly, instead of asserting a precomputed sum.
std::string write_reference_et(const std::string& name, uint64_t delay) {
    const auto path = (std::filesystem::temp_directory_path() / name).string();
    ProtoOutputStream stream(path);
    ChakraProtoMsg::GlobalMetadata metadata;
    for (const auto& entry :
         std::vector<std::pair<std::string, std::string>>{
             {"tier_manifest_digest", "sha256:tier"},
             {"service_binding_digest", "sha256:service"},
             {"service_activation_id", "activation"}}) {
        auto* attribute = metadata.add_attr();
        attribute->set_name(entry.first);
        attribute->set_string_val(entry.second);
    }
    auto* rank = metadata.add_attr();
    rank->set_name("service_rank");
    rank->set_uint64_val(0U);
    stream.write(metadata);
    for (uint64_t id = 1; id <= 6; ++id) {
        ChakraProtoMsg::Node node;
        node.set_id(id);
        node.set_name("reference-leaf-" + std::to_string(id));
        node.set_type(ChakraProtoMsg::COMP_NODE);
        node.set_duration_micros(delay);
        if (id > 1) node.add_data_deps(id - 1);
        for (const auto& entry :
             std::vector<std::pair<std::string, uint64_t>>{{"num_ops", 0U},
                                                           {"tensor_size", 1U}}) {
            auto* attribute = node.add_attr();
            attribute->set_name(entry.first);
            attribute->set_uint64_val(entry.second);
        }
        auto* cpu = node.add_attr();
        cpu->set_name("is_cpu_op");
        cpu->set_bool_val(false);
        stream.write(node);
    }
    return path;
}

bool template_invocation_matches_the_reference_et_of_the_same_dag() {
    constexpr uint64_t kDelay = 5;
    TemplateRegistry registry;
    const auto definition_json = valid_definition();
    const auto definition_path =
        write_json("astra-template-v2-differential-definition.json",
                   definition_json);
    const auto invocation_path =
        write_json("astra-template-v2-differential-invocation.json",
                   invocation(definition_json, kDelay));
    const auto definition = registry.loadDefinition(definition_path);
    TemplateWorkloadFeeder templated(
        definition, registry.loadInvocation(invocation_path, 0, *definition),
        &registry);
    const auto template_identity =
        std::vector<std::string>({templated.tierManifestDigest(),
                                  templated.serviceBindingDigest(),
                                  templated.serviceActivationId()});
    const auto template_rank = templated.serviceRank();
    const auto template_work = drain(templated);

    const auto et_path =
        write_reference_et("astra-template-v2-differential-reference.et", kDelay);
    ChakraEtWorkloadFeeder reference(et_path);
    const auto reference_identity =
        std::vector<std::string>({reference.tierManifestDigest(),
                                  reference.serviceBindingDigest(),
                                  reference.serviceActivationId()});
    const auto reference_rank = reference.serviceRank();
    const auto reference_work = drain(reference);

    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    std::remove(et_path.c_str());
    return template_work ==
               std::vector<uint64_t>(6, kDelay) &&
           template_work == reference_work &&
           template_identity == reference_identity &&
           template_rank == reference_rank &&
           !templated.hasNodesToIssue() && !reference.hasNodesToIssue() &&
           registry.activeFrameCount() == 0 &&
           registry.materializedLeafCount() == 0;
}

// The frontend controller parses one frozen, ordered metrics line. Pin the
// exact text here so a renamed or reordered field fails in the backend instead
// of silently dropping mechanism evidence. Executing after both JSON files are
// deleted also proves the drain needs no ET or definition file access.
bool mechanism_metrics_line_is_a_stable_ordered_protocol() {
    TemplateRegistry registry;
    const auto definition_json = valid_definition();
    const auto definition_path =
        write_json("astra-template-v2-metrics-definition.json",
                   definition_json);
    const auto invocation_path =
        write_json("astra-template-v2-metrics-invocation.json",
                   invocation(definition_json, 13U));
    const auto definition = registry.loadDefinition(definition_path);
    auto rank_invocation = registry.loadInvocation(invocation_path, 0,
                                                   *definition);
    std::remove(definition_path.c_str());
    std::remove(invocation_path.c_str());
    TemplateWorkloadFeeder feeder(definition, std::move(rank_invocation),
                                  &registry);
    const bool completed =
        drain(feeder) == std::vector<uint64_t>({13, 13, 13, 13, 13, 13});
    const auto line = AstraSim::format_template_metrics_line(3, registry, 0);
    return completed &&
           line ==
               "TEMPLATE_V2_METRICS rank=3 definition_load_count=1 "
               "plan_compilation_count=2 invocation_count=1 et_read_count=0 "
               "active_frames=0 peak_frames=3 materialized_leaves=0 "
               "peak_materialized_leaves=2" &&
           line.find('\n') == std::string::npos;
}

}  // namespace

int main() {
    return repeated_nested_invocations_are_isolated() &&
                   rejects_bad_binding_and_call_cycle() &&
                   sibling_compute_nodes_share_the_workload_resource() &&
                   recorded_compute_allows_zero_tensor_size() &&
                   native_memory_tier_ids_are_not_limited_to_legacy_slots() &&
                   template_invocation_matches_the_reference_et_of_the_same_dag() &&
                   mechanism_metrics_line_is_a_stable_ordered_protocol()
               ? 0
               : 1;
}
