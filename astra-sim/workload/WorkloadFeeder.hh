/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __WORKLOAD_FEEDER_HH__
#define __WORKLOAD_FEEDER_HH__

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "extern/graph_frontend/chakra/src/feeder/et_feeder.h"
#include "astra-sim/workload/JointAccessIndex.hh"

namespace AstraSim {

class WorkloadFeeder {
  public:
    virtual ~WorkloadFeeder() = default;

    virtual bool hasNodesToIssue() = 0;
    virtual std::shared_ptr<Chakra::ETFeederNode> getNextIssuableNode() = 0;
    virtual void pushBackIssuableNode(uint64_t node_id) = 0;
    virtual std::shared_ptr<Chakra::ETFeederNode> lookupNode(
        uint64_t node_id) = 0;
    virtual std::vector<std::shared_ptr<Chakra::ETFeederNode>> childNodes(
        uint64_t node_id) = 0;
    virtual void freeChildrenNodes(uint64_t node_id) = 0;
    virtual void removeNode(uint64_t node_id) = 0;
    virtual void printGraph() = 0;

    virtual const std::string& tierManifestDigest() const = 0;
    virtual const std::string& serviceBindingDigest() const = 0;
    virtual const std::string& serviceActivationId() const = 0;
    virtual std::optional<uint32_t> serviceRank() const = 0;
    virtual bool isTemplateV2() const { return false; }
    virtual bool isTemplateV3() const { return false; }
    virtual const JointNodeAccess* jointAccess(const ChakraProtoMsg::Node&,
                                              uint32_t, uint32_t) const { return nullptr; }
};

class TemplateIdleWorkloadFeeder final : public WorkloadFeeder {
  public:
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

  private:
    const std::string empty_;
};

class ChakraEtWorkloadFeeder final : public WorkloadFeeder {
  public:
    explicit ChakraEtWorkloadFeeder(const std::string& filename,
        std::shared_ptr<JointSourceContext> context = nullptr,
        uint32_t rank = 0, uint32_t iteration = 0,
        const MemoryTierRegistry* tiers = nullptr);

    std::size_t readyNodeCount() const { return feeder_.readyNodeCount(); }
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
    const JointNodeAccess* jointAccess(const ChakraProtoMsg::Node& node,
                                      uint32_t rank, uint32_t iteration) const override;

  private:
    std::unique_ptr<JointAccessIndex> joint_index_;
    Chakra::ETFeeder feeder_;
};

}  // namespace AstraSim

#endif
