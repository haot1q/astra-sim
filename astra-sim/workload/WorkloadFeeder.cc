/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/WorkloadFeeder.hh"
#include "astra-sim/common/Logging.hh"

#include <cstdlib>

#include <stdexcept>

namespace AstraSim {

bool TemplateIdleWorkloadFeeder::hasNodesToIssue() {
    return false;
}

std::shared_ptr<Chakra::ETFeederNode>
TemplateIdleWorkloadFeeder::getNextIssuableNode() {
    return nullptr;
}

void TemplateIdleWorkloadFeeder::pushBackIssuableNode(uint64_t) {
    throw std::logic_error("template-v2 idle feeder has no nodes");
}

std::shared_ptr<Chakra::ETFeederNode>
TemplateIdleWorkloadFeeder::lookupNode(uint64_t) {
    throw std::logic_error("template-v2 idle feeder has no nodes");
}

std::vector<std::shared_ptr<Chakra::ETFeederNode>>
TemplateIdleWorkloadFeeder::childNodes(uint64_t) {
    throw std::logic_error("template-v2 idle feeder has no nodes");
}

void TemplateIdleWorkloadFeeder::freeChildrenNodes(uint64_t) {
    throw std::logic_error("template-v2 idle feeder has no nodes");
}

void TemplateIdleWorkloadFeeder::removeNode(uint64_t) {
    throw std::logic_error("template-v2 idle feeder has no nodes");
}

void TemplateIdleWorkloadFeeder::printGraph() {}

const std::string& TemplateIdleWorkloadFeeder::tierManifestDigest() const {
    return empty_;
}

const std::string& TemplateIdleWorkloadFeeder::serviceBindingDigest() const {
    return empty_;
}

const std::string& TemplateIdleWorkloadFeeder::serviceActivationId() const {
    return empty_;
}

std::optional<uint32_t> TemplateIdleWorkloadFeeder::serviceRank() const {
    return std::nullopt;
}

namespace {
[[noreturn]] void fail_joint_contract(const std::exception& error) {
    // A callback exception would otherwise be swallowed by Sys::call_events.
    LoggerFactory::get_logger("workload")->critical("Joint association contract failed: {}", error.what());
    std::exit(EXIT_FAILURE);
}

std::unique_ptr<JointAccessIndex> prepare_joint_index(const std::string& filename,
    const std::shared_ptr<JointSourceContext>& context, uint32_t rank,
    uint32_t iteration, const MemoryTierRegistry* tiers) {
    if (context) return JointAccessIndex::load_and_validate(filename, context, rank, iteration, tiers);
    JointAccessIndex::probe_legacy(filename);
    return nullptr;
}
}  // namespace

ChakraEtWorkloadFeeder::ChakraEtWorkloadFeeder(const std::string& filename,
    std::shared_ptr<JointSourceContext> context, uint32_t rank, uint32_t iteration,
    const MemoryTierRegistry* tiers)
    : joint_index_(prepare_joint_index(filename, context, rank, iteration, tiers)), feeder_(filename) {
    if (joint_index_) joint_index_->verify_file_identity(filename);
}

const JointNodeAccess* ChakraEtWorkloadFeeder::jointAccess(const ChakraProtoMsg::Node& node,
    uint32_t rank, uint32_t iteration) const {
    try {
        if (joint_index_) return joint_index_->lookup(node, rank, iteration);
        JointAccessIndex::reject_legacy_reference(node);
        return nullptr;
    } catch (const std::exception& error) {
        fail_joint_contract(error);
    }
}

bool ChakraEtWorkloadFeeder::hasNodesToIssue() {
    return feeder_.hasNodesToIssue();
}

std::shared_ptr<const JointNodeAccess> ChakraEtWorkloadFeeder::jointAccessLease(
    const ChakraProtoMsg::Node& node, uint32_t rank, uint32_t iteration) const {
    const auto* access = jointAccess(node, rank, iteration);
    return access ? std::shared_ptr<const JointNodeAccess>(joint_index_, access) : nullptr;
}

std::shared_ptr<Chakra::ETFeederNode>
ChakraEtWorkloadFeeder::getNextIssuableNode() {
    auto node = feeder_.getNextIssuableNode();
    if (node && !joint_index_) {
        try {
            JointAccessIndex::reject_legacy_reference(*node->getChakraNode());
        } catch (const std::exception& error) {
            fail_joint_contract(error);
        }
    }
    return node;
}

void ChakraEtWorkloadFeeder::pushBackIssuableNode(uint64_t node_id) {
    feeder_.pushBackIssuableNode(node_id);
}

std::shared_ptr<Chakra::ETFeederNode>
ChakraEtWorkloadFeeder::lookupNode(uint64_t node_id) {
    return feeder_.lookupNode(node_id);
}

std::vector<std::shared_ptr<Chakra::ETFeederNode>>
ChakraEtWorkloadFeeder::childNodes(uint64_t node_id) {
    return feeder_.lookupNode(node_id)->getChildren();
}

void ChakraEtWorkloadFeeder::freeChildrenNodes(uint64_t node_id) {
    feeder_.freeChildrenNodes(node_id);
}

void ChakraEtWorkloadFeeder::removeNode(uint64_t node_id) {
    feeder_.removeNode(node_id);
}

void ChakraEtWorkloadFeeder::printGraph() {
    feeder_.printGraph();
}

const std::string& ChakraEtWorkloadFeeder::tierManifestDigest() const {
    return feeder_.tierManifestDigest();
}

const std::string& ChakraEtWorkloadFeeder::serviceBindingDigest() const {
    return feeder_.serviceBindingDigest();
}

const std::string& ChakraEtWorkloadFeeder::serviceActivationId() const {
    return feeder_.serviceActivationId();
}

std::optional<uint32_t> ChakraEtWorkloadFeeder::serviceRank() const {
    return feeder_.serviceRank();
}

}  // namespace AstraSim
