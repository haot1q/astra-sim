/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplateRegistry.hh"

#include <limits>
#include <sstream>
#include <stdexcept>

#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/workload/IndexedTemplateProgramFeeder.hh"
#include "astra-sim/workload/IndexedTemplateWorkloadFeeder.hh"

namespace AstraSim {
namespace {

namespace Wire = ServiceBindingJson;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v3: " + message);
    }
}

}  // namespace

std::shared_ptr<const nlohmann::json> IndexedTemplateRegistry::loadDefinition(
    const std::string& path) {
    const auto file_identity = read_template_file_identity(path);
    const auto cached = definitions_.find(path);
    if (cached != definitions_.end()) {
        require(cached->second.file_identity == file_identity,
                "cached definition path changed during the run");
        return cached->second.document;
    }
    constexpr std::size_t kMaximumCachedDefinitions = 64;
    require(definitions_.size() < kMaximumCachedDefinitions,
            "definition cache capacity exceeded");
    auto document = std::make_shared<const nlohmann::json>(Wire::read(path));
    definitions_.emplace(path, CachedDefinition{file_identity, document});
    ++definition_load_count_;
    return document;
}

IndexedTemplatePlan IndexedTemplateRegistry::compile(
    const std::string& definition_path, const std::string& invocation_path,
    uint32_t expected_rank) {
    const auto definition = loadDefinition(definition_path);
    const auto invocation = Wire::read(invocation_path);
    ++invocation_load_count_;
    auto plan =
        IndexedTemplatePlan::compile(*definition, invocation, expected_rank);
    ++plan_compilation_count_;
    return plan;
}

std::vector<IndexedTemplatePlan> IndexedTemplateRegistry::compileProgram(
    const std::string& definition_path, const std::string& invocation_path,
    uint32_t expected_rank) {
    const auto program = loadDefinition(definition_path);
    const auto invocation = Wire::read(invocation_path);
    ++invocation_load_count_;
    require(
        program->is_object() &&
            program->value("schema_version", std::string()) ==
                "template-definition-v3-program-proof" &&
            program->contains("definitions") &&
            program->at("definitions").is_array() &&
            !program->at("definitions").empty(),
        "program definition is malformed");
    require(
        invocation.is_object() &&
            invocation.value("schema_version", std::string()) ==
                "template-invocation-v3-program-proof" &&
            invocation.contains("ranks") &&
            invocation.at("ranks").is_array(),
        "program invocation is malformed");
    std::unordered_map<std::string, const nlohmann::json*> definitions;
    for (const auto& definition : program->at("definitions")) {
        require(
            definition.is_object() && definition.contains("template_id") &&
                definition.at("template_id").is_string(),
            "program child definition is malformed");
        const auto identifier =
            definition.at("template_id").get<std::string>();
        require(
            definitions.emplace(identifier, &definition).second,
            "program repeats a child definition");
    }
    const nlohmann::json* selected = nullptr;
    for (const auto& rank : invocation.at("ranks")) {
        require(
            rank.is_object() && rank.contains("rank") &&
                rank.at("rank").is_number_unsigned() &&
                rank.contains("calls") && rank.at("calls").is_array(),
            "program rank invocation is malformed");
        if (rank.at("rank").get<uint64_t>() == expected_rank) {
            require(selected == nullptr, "program repeats a rank");
            selected = &rank;
        }
    }
    require(selected != nullptr, "program omits the requested rank");
    require(!selected->at("calls").empty(), "program rank has no calls");
    std::vector<IndexedTemplatePlan> plans;
    plans.reserve(selected->at("calls").size());
    uint64_t event_id_offset = 0;
    for (const auto& call : selected->at("calls")) {
        require(
            call.is_object() && call.contains("definition_id") &&
                call.at("definition_id").is_string() &&
                call.contains("definition_digest") &&
                call.at("definition_digest").is_string() &&
                call.contains("bindings") && call.at("bindings").is_object(),
            "program call is malformed");
        const auto identifier = call.at("definition_id").get<std::string>();
        const auto found = definitions.find(identifier);
        require(found != definitions.end(), "program call has unknown definition");
        const nlohmann::json child_invocation = {
            {"schema_version", "template-invocation-v3-proof"},
            {"definition_id", identifier},
            {"definition_digest", call.at("definition_digest")},
            {"ranks", nlohmann::json::array({
                 nlohmann::json{
                     {"rank", expected_rank},
                     {"bindings", call.at("bindings")},
                 }})},
        };
        auto plan = IndexedTemplatePlan::compile(
            *found->second, child_invocation, expected_rank);
        require(
            plan.eventCount() <=
                std::numeric_limits<uint64_t>::max() - event_id_offset,
            "program event ids exceed uint64");
        plan.offsetEventIds(event_id_offset);
        event_id_offset += plan.eventCount();
        plans.push_back(std::move(plan));
        ++plan_compilation_count_;
    }
    return plans;
}

std::string format_indexed_template_metrics_line(
    uint32_t rank, const IndexedTemplateRegistry& registry,
    const IndexedTemplateWorkloadFeeder& feeder, uint64_t et_read_count,
    uint64_t exposure_baselines) {
    std::ostringstream line;
    line << "TEMPLATE_V3_METRICS"
         << " rank=" << rank
         << " definition_load_count=" << registry.definitionLoadCount()
         << " plan_compilation_count=" << registry.planCompilationCount()
         << " invocation_count=" << registry.invocationLoadCount()
         << " program_calls=1"
         << " et_read_count=" << et_read_count
         << " cached_definitions=" << registry.cachedDefinitionCount()
         << " tracked_events=" << feeder.trackedEventCount()
         << " peak_tracked_events=" << feeder.peakTrackedEventCount()
         << " materialized_events=" << feeder.materializedEventCount()
         << " exposure_baselines=" << exposure_baselines;
    return line.str();
}

std::string format_indexed_template_program_metrics_line(
    uint32_t rank, const IndexedTemplateRegistry& registry,
    const IndexedTemplateProgramFeeder& feeder, uint64_t et_read_count,
    uint64_t exposure_baselines) {
    std::ostringstream line;
    line << "TEMPLATE_V3_METRICS"
         << " rank=" << rank
         << " definition_load_count=" << registry.definitionLoadCount()
         << " plan_compilation_count=" << registry.planCompilationCount()
         << " invocation_count=" << registry.invocationLoadCount()
         << " program_calls=" << feeder.completedCallCount()
         << " et_read_count=" << et_read_count
         << " cached_definitions=" << registry.cachedDefinitionCount()
         << " tracked_events=" << feeder.trackedEventCount()
         << " peak_tracked_events=" << feeder.peakTrackedEventCount()
         << " materialized_events=" << feeder.materializedEventCount()
         << " exposure_baselines=" << exposure_baselines;
    return line.str();
}

}  // namespace AstraSim
