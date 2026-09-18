/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INDEXED_TEMPLATE_PROGRAM_FEEDER_HH__
#define __INDEXED_TEMPLATE_PROGRAM_FEEDER_HH__

#include <memory>
#include <vector>

#include "astra-sim/workload/IndexedTemplateWorkloadFeeder.hh"

namespace AstraSim {

// A finite serial list of reusable indexed calls. Only the current child owns
// execution-frame state; completing it constructs the next child feeder.
class IndexedTemplateProgramFeeder final : public WorkloadFeeder {
  public:
    explicit IndexedTemplateProgramFeeder(
        std::vector<IndexedTemplatePlan> plans);

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
    bool isTemplateV3() const override { return true; }

    uint64_t trackedEventCount() const;
    uint64_t peakTrackedEventCount() const;
    uint64_t materializedEventCount() const;
    uint64_t completedCallCount() const { return completed_call_count_; }

  private:
    bool advance();
    IndexedTemplateWorkloadFeeder& current();
    const IndexedTemplateWorkloadFeeder& current() const;

    std::vector<IndexedTemplatePlan> plans_;
    std::size_t next_plan_ = 0;
    std::unique_ptr<IndexedTemplateWorkloadFeeder> current_;
    uint64_t completed_call_count_ = 0;
    uint64_t peak_tracked_event_count_ = 0;
    std::string tier_manifest_digest_;
    std::string service_binding_digest_;
    std::string service_activation_id_;
    uint32_t rank_ = 0;
};

}  // namespace AstraSim

#endif
