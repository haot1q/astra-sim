/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplateProgramFeeder.hh"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace AstraSim {
namespace {

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v3: " + message);
    }
}

}  // namespace

IndexedTemplateProgramFeeder::IndexedTemplateProgramFeeder(
    std::vector<IndexedTemplatePlan> plans)
    : plans_(std::move(plans)) {
    require(!plans_.empty(), "program requires at least one call");
    tier_manifest_digest_ = plans_.front().tierManifestDigest();
    service_binding_digest_ = plans_.front().serviceBindingDigest();
    service_activation_id_ = plans_.front().serviceActivationId();
    rank_ = plans_.front().rank();
    for (const auto& plan : plans_) {
        require(
            plan.tierManifestDigest() == tier_manifest_digest_ &&
                plan.serviceBindingDigest() == service_binding_digest_ &&
                plan.serviceActivationId() == service_activation_id_ &&
                plan.rank() == rank_,
            "program calls require one rank and service identity");
        require(plan.eventCount() != 0, "program call must emit events");
    }
    advance();
}

bool IndexedTemplateProgramFeeder::advance() {
    if (current_ != nullptr) {
        if (current_->hasNodesToIssue()) return true;
        if (current_->trackedEventCount() != 0 ||
            current_->materializedEventCount() != 0) {
            return false;
        }
        peak_tracked_event_count_ = std::max(
            peak_tracked_event_count_, current_->peakTrackedEventCount());
        ++completed_call_count_;
        current_.reset();
    }
    if (next_plan_ == plans_.size()) return false;
    current_ = std::make_unique<IndexedTemplateWorkloadFeeder>(
        std::move(plans_.at(next_plan_++)));
    return true;
}

IndexedTemplateWorkloadFeeder& IndexedTemplateProgramFeeder::current() {
    require(current_ != nullptr, "program has no active call");
    return *current_;
}

const IndexedTemplateWorkloadFeeder&
IndexedTemplateProgramFeeder::current() const {
    require(current_ != nullptr, "program has no active call");
    return *current_;
}

bool IndexedTemplateProgramFeeder::hasNodesToIssue() {
    return advance() && current().hasNodesToIssue();
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateProgramFeeder::getNextIssuableNode() {
    if (!advance()) return nullptr;
    return current().getNextIssuableNode();
}

void IndexedTemplateProgramFeeder::pushBackIssuableNode(uint64_t node_id) {
    current().pushBackIssuableNode(node_id);
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateProgramFeeder::lookupNode(uint64_t node_id) {
    return current().lookupNode(node_id);
}

std::vector<std::shared_ptr<Chakra::ETFeederNode>>
IndexedTemplateProgramFeeder::childNodes(uint64_t node_id) {
    return current().childNodes(node_id);
}

void IndexedTemplateProgramFeeder::freeChildrenNodes(uint64_t node_id) {
    current().freeChildrenNodes(node_id);
}

void IndexedTemplateProgramFeeder::removeNode(uint64_t node_id) {
    current().removeNode(node_id);
}

void IndexedTemplateProgramFeeder::printGraph() {
    current().printGraph();
}

const std::string& IndexedTemplateProgramFeeder::tierManifestDigest() const {
    return tier_manifest_digest_;
}

const std::string& IndexedTemplateProgramFeeder::serviceBindingDigest() const {
    return service_binding_digest_;
}

const std::string& IndexedTemplateProgramFeeder::serviceActivationId() const {
    return service_activation_id_;
}

std::optional<uint32_t> IndexedTemplateProgramFeeder::serviceRank() const {
    return rank_;
}

uint64_t IndexedTemplateProgramFeeder::trackedEventCount() const {
    return current_ == nullptr ? 0 : current_->trackedEventCount();
}

uint64_t IndexedTemplateProgramFeeder::peakTrackedEventCount() const {
    return std::max(
        peak_tracked_event_count_,
        current_ == nullptr ? 0 : current_->peakTrackedEventCount());
}

uint64_t IndexedTemplateProgramFeeder::materializedEventCount() const {
    return current_ == nullptr ? 0 : current_->materializedEventCount();
}

}  // namespace AstraSim
