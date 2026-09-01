/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/DmaScheduler.hh"

#include <algorithm>
#include <queue>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace AstraSim {
namespace {

bool is_supported_path(const std::string& path_id) {
    return path_id == "base_die_local" || path_id == "gpu_routed";
}

bool is_high_priority(DmaPriorityClass priority) {
    return priority == DmaPriorityClass::DecodeCritical ||
           priority == DmaPriorityClass::PrefillCritical ||
           priority == DmaPriorityClass::Demand;
}

}  // namespace

DmaScheduler::DmaScheduler(DmaSchedulerConfig config)
    : config_(std::move(config)),
      engine_busy_(config_.engine_count, false) {
    if (!is_supported_path(config_.selected_path_id)) {
        throw std::invalid_argument(
            "selected DMA path must be base_die_local or gpu_routed");
    }
    if (config_.engine_count == 0) {
        throw std::invalid_argument("DMA engine_count must be positive");
    }
    if (config_.max_priority_burst == 0) {
        throw std::invalid_argument("DMA max_priority_burst must be positive");
    }
    if (config_.max_in_flight_page_movements == 0 ||
        config_.max_in_flight_page_movements > config_.engine_count) {
        throw std::invalid_argument(
            "DMA max_in_flight_page_movements must be in [1, engine_count]");
    }
}

void DmaScheduler::submit(DmaJob job) {
    if (job.event_id.empty()) {
        throw std::invalid_argument("DMA event_id must not be empty");
    }
    if (job.bytes == 0) {
        throw std::invalid_argument("DMA job bytes must be positive");
    }
    if (job.path_id != config_.selected_path_id) {
        throw std::invalid_argument(
            "DMA job path differs from the run-global selected path");
    }
    if (job.source_device_id != job.destination_device_id) {
        throw std::invalid_argument(
            "page movement must target the HBM paired with its source device");
    }
    if (job.resource_ids.empty()) {
        throw std::invalid_argument("DMA job must declare path resources");
    }
    std::unordered_set<std::string> unique_resources;
    for (const auto& resource : job.resource_ids) {
        if (resource.empty() || !unique_resources.insert(resource).second) {
            throw std::invalid_argument(
                "DMA path resources must be non-empty and unique");
        }
    }
    for (const auto& dependency : job.dependencies) {
        if (dependency.empty() || dependency == job.event_id) {
            throw std::invalid_argument("invalid DMA event dependency");
        }
    }
    if (!accepted_ids_.insert(job.event_id).second) {
        throw std::invalid_argument("duplicate run-global DMA event_id");
    }
    const std::string event_id = job.event_id;
    const uint64_t submission_seq = next_submission_seq_++;
    const auto inserted =
        pending_.emplace(event_id, PendingJob{std::move(job), submission_seq});
    if (!inserted.second) {
        throw std::logic_error("DMA event unexpectedly already pending");
    }
    std::unordered_set<std::string> unresolved;
    for (const auto& dependency : inserted.first->second.job.dependencies) {
        if (completed_ids_.find(dependency) == completed_ids_.end()) {
            unresolved.insert(dependency);
        }
    }
    unresolved_dependencies_[event_id] = unresolved.size();
    for (const auto& dependency : unresolved) {
        dependents_[dependency].push_back(event_id);
    }
    if (unresolved.empty()) {
        enqueue_dependency_ready(inserted.first->second);
    }
    dependency_graph_dirty_ = true;
}

void DmaScheduler::validate_dependencies() {
    if (!dependency_graph_dirty_) {
        return;
    }
    ++diagnostics_.full_dependency_validations;
    for (const auto& entry : pending_) {
        for (const auto& dependency : entry.second.job.dependencies) {
            if (accepted_ids_.find(dependency) == accepted_ids_.end()) {
                throw std::invalid_argument(
                    "DMA event has a dangling dependency '" + dependency + "'");
            }
        }
    }
    std::unordered_map<std::string, std::size_t> pending_indegree;
    std::unordered_map<std::string, std::vector<std::string>> pending_edges;
    for (const auto& entry : pending_) {
        pending_indegree.emplace(entry.first, 0);
    }
    for (const auto& entry : pending_) {
        const std::unordered_set<std::string> unique_dependencies(
            entry.second.job.dependencies.begin(),
            entry.second.job.dependencies.end());
        for (const auto& dependency : unique_dependencies) {
            if (pending_.find(dependency) == pending_.end()) {
                continue;
            }
            ++pending_indegree.at(entry.first);
            pending_edges[dependency].push_back(entry.first);
        }
    }
    std::queue<std::string> dependency_ready;
    for (const auto& entry : pending_indegree) {
        if (entry.second == 0) {
            dependency_ready.push(entry.first);
        }
    }
    std::size_t visited = 0;
    while (!dependency_ready.empty()) {
        const std::string event_id = dependency_ready.front();
        dependency_ready.pop();
        ++visited;
        const auto dependents = pending_edges.find(event_id);
        if (dependents == pending_edges.end()) {
            continue;
        }
        for (const auto& dependent : dependents->second) {
            auto& indegree = pending_indegree.at(dependent);
            if (--indegree == 0) {
                dependency_ready.push(dependent);
            }
        }
    }
    if (visited != pending_.size()) {
        throw std::invalid_argument(
            "DMA event dependency graph contains a cycle");
    }
    dependency_graph_dirty_ = false;
}

void DmaScheduler::enqueue_dependency_ready(const PendingJob& pending) {
    time_waiting_.push(
        {pending.job.ready_ns, pending.submission_seq, pending.job.event_id});
}

void DmaScheduler::promote_time_ready(uint64_t now_ns) {
    while (!time_waiting_.empty() && time_waiting_.top().ready_ns <= now_ns) {
        const TimeReadyEntry timed = time_waiting_.top();
        time_waiting_.pop();
        const auto found = pending_.find(timed.event_id);
        if (found == pending_.end()) {
            throw std::logic_error("time-ready DMA event is not pending");
        }
        const auto& pending = found->second;
        const bool high = is_high_priority(pending.job.priority);
        const std::size_t queue_index =
            (high ? 0U : 2U) + (pending.job.page_movement ? 1U : 0U);
        ready_[queue_index].push({static_cast<uint8_t>(pending.job.priority),
                                  pending.job.ready_ns, pending.submission_seq,
                                  pending.job.event_id});
    }
}

std::optional<std::size_t> DmaScheduler::select_ready_queue() const {
    const bool page_allowed =
        in_flight_page_movements_ < config_.max_in_flight_page_movements;
    const auto better = [this](std::optional<std::size_t> current,
                               std::size_t candidate) {
        if (ready_[candidate].empty()) {
            return current;
        }
        if (!current.has_value() ||
            ReadyGreater{}(ready_[*current].top(), ready_[candidate].top())) {
            return std::optional<std::size_t>{candidate};
        }
        return current;
    };
    std::optional<std::size_t> high = better(std::nullopt, 0);
    std::optional<std::size_t> low = better(std::nullopt, 2);
    if (page_allowed) {
        high = better(high, 1);
        low = better(low, 3);
    }
    if (consecutive_high_priority_starts_ >= config_.max_priority_burst &&
        low.has_value()) {
        return low;
    }
    if (!high.has_value()) {
        return low;
    }
    if (!low.has_value()) {
        return high;
    }
    return ReadyGreater{}(ready_[*high].top(), ready_[*low].top()) ? low : high;
}

uint32_t DmaScheduler::first_free_engine() const {
    const auto engine =
        std::find(engine_busy_.begin(), engine_busy_.end(), false);
    if (engine == engine_busy_.end()) {
        throw std::logic_error("no free DMA engine");
    }
    return static_cast<uint32_t>(engine - engine_busy_.begin());
}

std::vector<DmaDispatch> DmaScheduler::dispatch_ready(uint64_t now_ns) {
    std::vector<DmaDispatch> dispatches;
    promote_time_ready(now_ns);
    while (in_flight_.size() < engine_busy_.size()) {
        const auto selected = select_ready_queue();
        if (!selected.has_value()) {
            break;
        }
        const ReadyEntry ready = ready_[*selected].top();
        ready_[*selected].pop();
        ++diagnostics_.ready_queue_pops;
        const auto found = pending_.find(ready.event_id);
        if (found == pending_.end()) {
            throw std::logic_error("ready DMA event is not pending");
        }
        PendingJob pending = std::move(found->second);
        pending_.erase(found);
        unresolved_dependencies_.erase(ready.event_id);
        const uint32_t engine_id = first_free_engine();
        engine_busy_[engine_id] = true;
        if (is_high_priority(pending.job.priority)) {
            ++consecutive_high_priority_starts_;
        } else {
            consecutive_high_priority_starts_ = 0;
        }
        if (pending.job.page_movement) {
            ++in_flight_page_movements_;
        }
        DmaDispatch dispatch{pending.job, engine_id, now_ns};
        const std::string event_id = pending.job.event_id;
        const auto inserted = in_flight_.emplace(
            event_id, InFlightJob{std::move(pending.job), engine_id, now_ns});
        if (!inserted.second) {
            throw std::logic_error("DMA event unexpectedly already in flight");
        }
        dispatches.push_back(std::move(dispatch));
    }
    return dispatches;
}

DmaReceipt DmaScheduler::complete(const std::string& event_id,
                                  uint64_t finish_ns) {
    const auto found = in_flight_.find(event_id);
    if (found == in_flight_.end()) {
        throw std::invalid_argument("completion for unknown DMA event_id");
    }
    const InFlightJob& active = found->second;
    if (finish_ns < active.start_ns) {
        throw std::invalid_argument("DMA finish time precedes start time");
    }
    const DmaReceipt receipt{
        event_id,
        active.job.source_iteration_id,
        active.job.ready_ns,
        active.start_ns,
        finish_ns,
        active.start_ns - active.job.ready_ns,
        finish_ns - active.start_ns,
        active.job.bytes,
        active.job.kind,
        active.job.phase,
        active.job.priority,
        active.job.path_id,
        active.engine_id,
        active.job.resource_ids,
    };
    engine_busy_.at(active.engine_id) = false;
    if (active.job.page_movement) {
        if (in_flight_page_movements_ == 0) {
            throw std::logic_error("DMA page-movement counter underflow");
        }
        --in_flight_page_movements_;
    }
    completed_ids_.insert(event_id);
    const auto dependents = dependents_.find(event_id);
    if (dependents != dependents_.end()) {
        for (const auto& dependent_id : dependents->second) {
            ++diagnostics_.dependency_release_visits;
            auto count = unresolved_dependencies_.find(dependent_id);
            if (count == unresolved_dependencies_.end() || count->second == 0) {
                throw std::logic_error(
                    "DMA dependency readiness index is inconsistent");
            }
            --count->second;
            if (count->second == 0) {
                const auto pending = pending_.find(dependent_id);
                if (pending == pending_.end()) {
                    throw std::logic_error(
                        "dependent DMA event is not pending");
                }
                enqueue_dependency_ready(pending->second);
            }
        }
        dependents_.erase(dependents);
    }
    in_flight_.erase(found);
    return receipt;
}

DmaJob DmaScheduler::active_job(const std::string& event_id) const {
    const auto found = in_flight_.find(event_id);
    if (found == in_flight_.end()) {
        throw std::invalid_argument("unknown in-flight DMA event_id");
    }
    return found->second.job;
}

bool DmaScheduler::drained() const {
    return pending_.empty() && in_flight_.empty();
}

std::size_t DmaScheduler::pending_count() const {
    return pending_.size();
}

std::size_t DmaScheduler::in_flight_count() const {
    return in_flight_.size();
}

const DmaSchedulerConfig& DmaScheduler::config() const {
    return config_;
}

const DmaSchedulerDiagnostics& DmaScheduler::diagnostics() const {
    return diagnostics_;
}

DmaPriorityClass parse_dma_priority_class(const std::string& value) {
    if (value == "decode_critical") {
        return DmaPriorityClass::DecodeCritical;
    }
    if (value == "prefill_critical") {
        return DmaPriorityClass::PrefillCritical;
    }
    if (value == "demand") {
        return DmaPriorityClass::Demand;
    }
    if (value == "background_fill") {
        return DmaPriorityClass::BackgroundFill;
    }
    if (value == "demote") {
        return DmaPriorityClass::Demote;
    }
    throw std::invalid_argument("unknown DMA priority_class");
}

std::string dma_priority_class_name(DmaPriorityClass value) {
    switch (value) {
    case DmaPriorityClass::DecodeCritical:
        return "decode_critical";
    case DmaPriorityClass::PrefillCritical:
        return "prefill_critical";
    case DmaPriorityClass::Demand:
        return "demand";
    case DmaPriorityClass::BackgroundFill:
        return "background_fill";
    case DmaPriorityClass::Demote:
        return "demote";
    }
    throw std::invalid_argument("unknown DMA priority class");
}

}  // namespace AstraSim
