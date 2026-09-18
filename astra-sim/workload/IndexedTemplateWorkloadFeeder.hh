/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INDEXED_TEMPLATE_WORKLOAD_FEEDER_HH__
#define __INDEXED_TEMPLATE_WORKLOAD_FEEDER_HH__

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "astra-sim/workload/IndexedTemplatePlan.hh"
#include "astra-sim/workload/WorkloadFeeder.hh"

namespace AstraSim {

class IndexedTemplateWorkloadFeeder final : public WorkloadFeeder {
  public:
    explicit IndexedTemplateWorkloadFeeder(IndexedTemplatePlan plan);

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

  private:
    enum class EventStatus {
        Blocked,
        Ready,
        Issued,
        Complete,
    };
    struct EventState {
        uint64_t completed_parents = 0;
        EventStatus status = EventStatus::Blocked;
    };

    void activateRoot(std::size_t recipe);
    void advanceRoot(std::size_t recipe);
    void updatePeak();
    std::shared_ptr<Chakra::ETFeederNode> materialize(
        const IndexedTemplateEvent& event);
    std::shared_ptr<Chakra::ETFeederNode> makeNode(
        const IndexedTemplateEvent& event) const;

    IndexedTemplatePlan plan_;
    uint64_t unfinished_event_count_;
    uint64_t peak_tracked_event_count_ = 0;
    std::unordered_map<uint64_t, EventState> states_;
    std::map<uint64_t, std::shared_ptr<Chakra::ETFeederNode>> materialized_;
    std::unordered_map<uint64_t, std::set<uint64_t>> exposed_children_;
    std::vector<uint64_t> root_cursor_;
    std::priority_queue<uint64_t, std::vector<uint64_t>,
                        std::greater<uint64_t>> ready_;
};

}  // namespace AstraSim

#endif
