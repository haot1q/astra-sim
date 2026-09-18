/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INDEXED_TEMPLATE_PLAN_HH__
#define __INDEXED_TEMPLATE_PLAN_HH__

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "astra-sim/workload/IndexedTemplateExpression.hh"
#include "extern/helper/json/json.hpp"

namespace AstraSim {

enum class IndexedEdgeRelation {
    SameIndex,
    SameOuter,
    AllToOne,
    OneToAll,
    OneToOne,
};

struct IndexedTemplateDomain {
    std::vector<std::string> axes;
    std::vector<uint64_t> extents;
    uint64_t count;
};

struct IndexedTemplateRecipe {
    std::string id;
    std::string kind;
    std::optional<std::string> domain;
    uint64_t base_id;
    uint64_t count;
    uint64_t stride;
    std::vector<std::string> axes;
    std::vector<uint64_t> extents;
    std::unordered_map<std::string, nlohmann::json> attributes;
};

struct IndexedTemplateEdge {
    std::size_t from_recipe;
    std::size_t to_recipe;
    IndexedEdgeRelation relation;
};

struct IndexedTemplateEvent {
    uint64_t id;
    std::size_t recipe;
    uint64_t index;
    IndexedIndices indices;
};

class IndexedTemplatePlan {
  public:
    static IndexedTemplatePlan compile(
        const nlohmann::json& definition,
        const nlohmann::json& invocation,
        uint32_t expected_rank);
    static std::string definitionDigest(
        const nlohmann::json& definition);

    IndexedTemplateEvent event(uint64_t event_id) const;
    uint64_t eventId(std::size_t recipe, uint64_t index) const;
    nlohmann::json attributes(const IndexedTemplateEvent& event) const;
    std::vector<IndexedTemplateEvent> children(
        const IndexedTemplateEvent& event) const;
    std::vector<uint64_t> fixedParentIds(
        const IndexedTemplateEvent& event) const;
    uint64_t requiredParentCount(const IndexedTemplateEvent& event) const;
    bool isAllToOne(
        const IndexedTemplateEvent& parent,
        const IndexedTemplateEvent& child) const;
    bool exposeChildAfterCompletion(
        const IndexedTemplateEvent& parent,
        const IndexedTemplateEvent& child,
        uint64_t completed_parent_count) const;

    const std::vector<IndexedTemplateRecipe>& recipes() const;
    const std::vector<IndexedTemplateEdge>& edges() const;
    uint64_t eventCount() const;
    uint32_t rank() const;
    const std::string& tierManifestDigest() const;
    const std::string& serviceBindingDigest() const;
    const std::string& serviceActivationId() const;

  private:
    static const nlohmann::json& selectRank(
        const nlohmann::json& invocation,
        uint32_t expected_rank);
    void loadBindings(
        const nlohmann::json& definition,
        const nlohmann::json& rank);
    std::unordered_map<std::string, IndexedTemplateDomain> loadDomains(
        const nlohmann::json& definition) const;
    void loadRecipes(
        const nlohmann::json& definition,
        const std::unordered_map<std::string, IndexedTemplateDomain>& domains);
    void assignEventIds();
    void loadEdges(const nlohmann::json& definition);
    void validateGraph() const;

    std::vector<IndexedTemplateRecipe> recipes_;
    std::vector<IndexedTemplateEdge> edges_;
    std::unordered_map<std::string, uint64_t> bindings_;
    IndexedVectorBindings vectors_;
    uint64_t event_count_ = 0;
    uint32_t rank_ = 0;
    std::string tier_manifest_digest_;
    std::string service_binding_digest_;
    std::string service_activation_id_;
};

}  // namespace AstraSim

#endif
