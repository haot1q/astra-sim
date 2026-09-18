/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplateRegistry.hh"

#include <sstream>
#include <stdexcept>

#include "astra-sim/system/memory/ServiceBindingJson.hh"
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
         << " et_read_count=" << et_read_count
         << " cached_definitions=" << registry.cachedDefinitionCount()
         << " tracked_events=" << feeder.trackedEventCount()
         << " peak_tracked_events=" << feeder.peakTrackedEventCount()
         << " materialized_events=" << feeder.materializedEventCount()
         << " exposure_baselines=" << exposure_baselines;
    return line.str();
}

}  // namespace AstraSim
