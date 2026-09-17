/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/WorkloadFeeder.hh"

namespace AstraSim {

ChakraEtWorkloadFeeder::ChakraEtWorkloadFeeder(const std::string& filename)
    : feeder_(filename) {}

bool ChakraEtWorkloadFeeder::hasNodesToIssue() {
    return feeder_.hasNodesToIssue();
}

std::shared_ptr<Chakra::ETFeederNode>
ChakraEtWorkloadFeeder::getNextIssuableNode() {
    return feeder_.getNextIssuableNode();
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
