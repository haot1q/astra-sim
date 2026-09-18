/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INDEXED_TEMPLATE_REGISTRY_HH__
#define __INDEXED_TEMPLATE_REGISTRY_HH__

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "astra-sim/workload/IndexedTemplatePlan.hh"
#include "astra-sim/workload/TemplateFileIdentity.hh"

namespace AstraSim {

// Run-scoped owner of indexed template documents. One definition is parsed and
// digest-checked once per run; every step only rebinds invocation ports, so
// cached state stays proportional to the definition, never to the events one
// invocation would issue.
class IndexedTemplateRegistry {
  public:
    IndexedTemplatePlan compile(const std::string& definition_path,
                                const std::string& invocation_path,
                                uint32_t expected_rank);

    uint64_t definitionLoadCount() const { return definition_load_count_; }
    uint64_t planCompilationCount() const { return plan_compilation_count_; }
    uint64_t invocationLoadCount() const { return invocation_load_count_; }
    uint64_t cachedDefinitionCount() const {
        return static_cast<uint64_t>(definitions_.size());
    }

  private:
    struct CachedDefinition {
        TemplateFileIdentity file_identity;
        std::shared_ptr<const nlohmann::json> document;
    };

    std::shared_ptr<const nlohmann::json> loadDefinition(
        const std::string& path);

    std::unordered_map<std::string, CachedDefinition> definitions_;
    uint64_t definition_load_count_ = 0;
    uint64_t plan_compilation_count_ = 0;
    uint64_t invocation_load_count_ = 0;
};

class IndexedTemplateWorkloadFeeder;

std::string format_indexed_template_metrics_line(
    uint32_t rank, const IndexedTemplateRegistry& registry,
    const IndexedTemplateWorkloadFeeder& feeder, uint64_t et_read_count,
    uint64_t exposure_baselines);

}  // namespace AstraSim

#endif
