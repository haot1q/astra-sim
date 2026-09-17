/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TEMPLATE_WORKLOAD_FEEDER_HH__
#define __TEMPLATE_WORKLOAD_FEEDER_HH__

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <vector>

#include "astra-sim/workload/TemplateRegistry.hh"
#include "astra-sim/workload/WorkloadFeeder.hh"

namespace AstraSim {

class TemplateWorkloadFeeder final : public WorkloadFeeder {
  public:
    TemplateWorkloadFeeder(
        std::shared_ptr<const TemplateDefinitionSet> definitions,
        TemplateInvocation invocation,
        TemplateRegistry* registry,
        bool roofline_enabled = false);
    ~TemplateWorkloadFeeder() override;

    bool hasNodesToIssue() override;
    std::shared_ptr<Chakra::ETFeederNode> getNextIssuableNode() override;
    void pushBackIssuableNode(uint64_t node_id) override;
    std::shared_ptr<Chakra::ETFeederNode> lookupNode(
        uint64_t node_id) override;
    std::vector<std::shared_ptr<Chakra::ETFeederNode>> childNodes(
        uint64_t node_id) override;
    void freeChildrenNodes(uint64_t node_id) override;
    void removeNode(uint64_t node_id) override;
    void printGraph() override;
    const std::string& tierManifestDigest() const override;
    const std::string& serviceBindingDigest() const override;
    const std::string& serviceActivationId() const override;
    std::optional<uint32_t> serviceRank() const override;
    bool isTemplateV2() const override { return true; }

    uint64_t frameCount() const;
    uint64_t materializedLeafCount() const;
    uint64_t peakMaterializedLeafCount() const;

  private:
    enum class LeafStatus {
        Blocked,
        Ready,
        Issued,
        Complete,
    };
    struct LeafState {
        uint64_t remaining_dependencies;
        LeafStatus status;
    };

    std::shared_ptr<Chakra::ETFeederNode> materializeLeaf(uint64_t node_id);
    std::shared_ptr<Chakra::ETFeederNode> makeLeaf(
        const TemplateLeafPlan& leaf);
    void validateLeaf(const TemplateLeafPlan& leaf) const;
    void releaseFrames();
    void releaseMaterializedLeaves();

    std::shared_ptr<const TemplateDefinitionSet> definitions_;
    const TemplateExecutionPlan* plan_;
    TemplateInvocation invocation_;
    TemplateRegistry* registry_;
    bool roofline_enabled_;
    uint64_t frame_count_ = 0;
    uint64_t unfinished_leaf_count_ = 0;
    uint64_t peak_materialized_leaf_count_ = 0;
    bool frames_released_ = false;
    std::vector<LeafState> states_;
    std::map<uint64_t, std::shared_ptr<Chakra::ETFeederNode>> materialized_;
    std::priority_queue<uint64_t, std::vector<uint64_t>,
                        std::greater<uint64_t>> ready_;
};

}  // namespace AstraSim

#endif
