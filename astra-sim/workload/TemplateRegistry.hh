/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TEMPLATE_REGISTRY_HH__
#define __TEMPLATE_REGISTRY_HH__

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "astra-sim/workload/TemplateFileIdentity.hh"
#include "extern/helper/json/json.hpp"

namespace AstraSim {

struct TemplatePort {
    std::string name;
    std::string type;
};

struct TemplateNodeDefinition {
    std::string id;
    std::string kind;
    std::vector<std::string> dependencies;
    std::string callee;
    std::unordered_map<std::string, nlohmann::json> bindings;
    std::unordered_map<std::string, nlohmann::json> attributes;
};

struct TemplateDefinition {
    std::string id;
    std::vector<TemplatePort> ports;
    std::vector<TemplateNodeDefinition> nodes;
};

struct TemplateLeafPlan {
    uint64_t id;
    std::string name;
    std::string kind;
    std::unordered_map<std::string, nlohmann::json> attributes;
    std::vector<uint64_t> parents;
    std::vector<uint64_t> children;
};

struct TemplateExecutionPlan {
    std::string root_template;
    std::vector<TemplateLeafPlan> leaves;
    std::vector<uint64_t> roots;
    std::vector<std::string> frame_templates;
};

struct TemplateDefinitionSet {
    std::string id;
    std::string digest;
    std::string tier_manifest_digest;
    std::string service_binding_digest;
    std::string service_activation_id;
    std::unordered_map<std::string, TemplateDefinition> templates;
    std::unordered_map<std::string, TemplateExecutionPlan> plans;
};

struct TemplateInvocation {
    std::string definition_id;
    std::string definition_digest;
    uint32_t rank;
    std::string root_template;
    std::unordered_map<std::string, nlohmann::json> bindings;
};

class TemplateRegistry;

// Mechanism metrics protocol line consumed by the frontend controller. Field
// order is frozen: rank, definition_load_count, plan_compilation_count,
// invocation_count, et_read_count, active_frames, peak_frames,
// materialized_leaves, peak_materialized_leaves. `et_read_count` belongs to the
// Chakra ET path and is therefore supplied by the Workload, not the registry.
std::string format_template_metrics_line(uint32_t rank,
                                         const TemplateRegistry& registry,
                                         uint64_t et_read_count);

class TemplateRegistry {
  public:
    std::shared_ptr<const TemplateDefinitionSet> loadDefinition(
        const std::string& path);
    TemplateInvocation loadInvocation(
        const std::string& path,
        uint32_t expected_rank,
        const TemplateDefinitionSet& definition);

    uint64_t definitionLoadCount() const;
    uint64_t invocationCount() const;
    uint64_t planCompilationCount() const;
    uint64_t activeFrameCount() const;
    uint64_t peakFrameCount() const;
    uint64_t materializedLeafCount() const;
    uint64_t peakMaterializedLeafCount() const;
    void acquireFrames(uint64_t count);
    void releaseFrames(uint64_t count);
    void acquireMaterializedLeaf();
    void releaseMaterializedLeaf();

  private:
    struct CachedDefinition {
        TemplateFileIdentity file_identity;
        std::shared_ptr<const TemplateDefinitionSet> definition;
    };

    std::unordered_map<std::string,
        CachedDefinition> definitions_;
    uint64_t definition_load_count_ = 0;
    uint64_t invocation_count_ = 0;
    uint64_t plan_compilation_count_ = 0;
    uint64_t active_frame_count_ = 0;
    uint64_t peak_frame_count_ = 0;
    uint64_t materialized_leaf_count_ = 0;
    uint64_t peak_materialized_leaf_count_ = 0;
};

}  // namespace AstraSim

#endif
