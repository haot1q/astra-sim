/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/MemoryMovementExecutor.hh"

#include <algorithm>
#include <stdexcept>
#include "astra-sim/system/Sys.hh"
#include "astra-sim/workload/Workload.hh"
#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"

namespace AstraSim {
namespace {
std::string wait_string(const std::shared_ptr<Chakra::ETFeederNode>& node,
                        const std::string& name) {
    if (!node->has_other_attr(name)) {
        throw std::invalid_argument("memory wait is missing " + name);
    }
    const auto& attribute = node->get_other_attr(name);
    if (!attribute.has_string_val() || attribute.string_val().empty()) {
        throw std::invalid_argument("memory wait requires string " + name);
    }
    return attribute.string_val();
}
}  // namespace

bool MemoryMovementExecutor::is_wait_node(
    const std::shared_ptr<Chakra::ETFeederNode>& node) const {
    return node->has_other_attr("memory_movement_schema_version") &&
           wait_string(node, "memory_movement_schema_version") == "memory-wait-v1";
}

void MemoryMovementExecutor::require_prior_event(
    const std::string& event_id, const std::string& instance_id,
    uint32_t source_iteration) const {
    const auto original = accepted_identities_.find(event_id);
    if (scheduler_ == nullptr || !scheduler_->accepted(event_id) ||
        original == accepted_identities_.end() ||
        original->second.instance_id != instance_id ||
        original->second.source_iteration >= source_iteration) {
        throw std::invalid_argument(
            "prior movement was not submitted by this rank/instance in an earlier iteration: " + event_id);
    }
}


void MemoryMovementExecutor::validate_prior_dependencies(
    const DmaJob& job, const std::string& instance_id, const std::string& schema,
    const std::vector<std::string>& prior_dependencies) const {
    const auto source_iteration = job.source_iteration_id;
    std::unordered_set<std::string> declared_prior;
    if (schema == "memory-events-v2") {
        for (const auto& prior : prior_dependencies) {
            if (!declared_prior.insert(prior).second) {
                throw std::invalid_argument("duplicate prior movement declaration");
            }
            if (std::find(job.dependencies.begin(), job.dependencies.end(), prior) ==
                job.dependencies.end()) {
                throw std::invalid_argument("unused prior movement declaration");
            }
            require_prior_event(prior, instance_id, source_iteration);
        }
    }
    for (const auto& dependency : job.dependencies) {
        const auto original = accepted_identities_.find(dependency);
        if (original == accepted_identities_.end()) continue;
        if (original->second.instance_id != instance_id ||
            original->second.source_iteration > source_iteration) {
            throw std::invalid_argument("movement dependency has a different owner or future iteration");
        }
        if (original->second.source_iteration < source_iteration &&
            (schema != "memory-events-v2" || declared_prior.count(dependency) != 1)) {
            throw std::invalid_argument("earlier movement dependency lacks a prior declaration");
        }
    }
}

bool MemoryMovementExecutor::submit_wait(
    const std::shared_ptr<Chakra::ETFeederNode>& node, Workload* workload) {
    rethrow_failure();
    try {
        if (!is_wait_node(node) || workload == nullptr || node->tensor_size() != 0 ||
            node->is_cpu_op() || node->type() != ChakraProtoMsg::MEM_LOAD_NODE) {
            throw std::invalid_argument("memory wait requires a zero-transfer typed gate");
        }
        const auto digest = wait_string(node, "memory_movement_manifest_digest");
        sys_->validate_tier_manifest_digest(digest);
        if (wait_string(node, "movement_run_id") != run_id_ || digest != manifest_digest_) {
            throw std::invalid_argument("memory wait differs from the original movement run");
        }
        const auto instance = wait_string(node, "movement_instance_id");
        if (!node->has_other_attr("movement_source_iteration_id") ||
            !node->get_other_attr("movement_source_iteration_id").has_uint32_val() ||
            !node->has_other_attr("movement_dependencies") ||
            !node->get_other_attr("movement_dependencies").has_string_list()) {
            throw std::invalid_argument("memory wait requires iteration and dependency attributes");
        }
        const auto source_iteration =
            node->get_other_attr("movement_source_iteration_id").uint32_val();
        const auto& dependencies = node->get_other_attr("movement_dependencies").string_list().values();
        if (dependencies.empty()) {
            throw std::invalid_argument("memory wait cannot have empty dependencies");
        }
        std::unordered_set<std::string> unique, pending;
        for (const auto& event_id : dependencies) {
            if (!unique.insert(event_id).second) {
                throw std::invalid_argument("duplicate memory wait dependency");
            }
            require_prior_event(event_id, instance, source_iteration);
            if (!scheduler_->completed(event_id)) pending.insert(event_id);
        }
        if (pending.empty()) return false;
        const auto wait_id = next_wait_id_++;
        waiters_.emplace(wait_id, Waiter{workload, workload->iteration, node, pending});
        for (const auto& event_id : pending) waiting_by_event_[event_id].push_back(wait_id);
        return true;
    } catch (...) {
        if (!failure_) failure_ = std::current_exception();
        throw;
    }
}

void MemoryMovementExecutor::complete_waiters(const std::string& event_id) {
    const auto found = waiting_by_event_.find(event_id);
    if (found == waiting_by_event_.end()) return;
    const auto ids = std::move(found->second);
    waiting_by_event_.erase(found);
    for (const auto wait_id : ids) {
        auto& waiter = waiters_.at(wait_id);
        if (waiter.dependencies.erase(event_id) != 1) {
            throw std::logic_error("memory wait dependency completed twice");
        }
        if (!waiter.dependencies.empty()) continue;
        const auto completed = std::move(waiter);
        waiters_.erase(wait_id);
        completed.workload->complete_memory_wait(completed.node, completed.iteration);
    }
}
}  // namespace AstraSim
