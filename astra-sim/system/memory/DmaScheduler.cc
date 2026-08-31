/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/DmaScheduler.hh"

#include <algorithm>
#include <functional>
#include <limits>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace AstraSim {
namespace {

bool is_supported_path(const std::string& path_id) {
    return path_id == "base_die_local" || path_id == "gpu_routed";
}

const std::vector<std::string>& required_resources(const std::string& path_id) {
    static const std::vector<std::string> base_die_local = {
        "lpddr_read", "base_die_dma", "local_stack_fabric", "hbm_write"};
    static const std::vector<std::string> gpu_routed = {
        "lpddr_read", "base_to_gpu_link", "gpu_dma", "gpu_to_base_link",
        "hbm_write"};
    return path_id == "base_die_local" ? base_die_local : gpu_routed;
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
    for (const auto& required : required_resources(job.path_id)) {
        const bool present =
            std::any_of(job.resource_ids.begin(), job.resource_ids.end(),
                        [&required](const std::string& resource) {
                            return resource.find(required) != std::string::npos;
                        });
        if (!present) {
            throw std::invalid_argument(
                "DMA path is missing required resource '" + required + "'");
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
    pending_.push_back({std::move(job), next_submission_seq_++});
}

void DmaScheduler::validate_dependencies() const {
    std::unordered_map<std::string, const DmaJob*> pending_by_id;
    for (const auto& pending : pending_) {
        pending_by_id.emplace(pending.job.event_id, &pending.job);
        for (const auto& dependency : pending.job.dependencies) {
            if (accepted_ids_.find(dependency) == accepted_ids_.end()) {
                throw std::invalid_argument(
                    "DMA event has a dangling dependency '" + dependency + "'");
            }
        }
    }
    std::unordered_set<std::string> visiting;
    std::unordered_set<std::string> visited;
    const std::function<void(const std::string&)> visit =
        [&](const std::string& event_id) {
            if (visited.find(event_id) != visited.end()) {
                return;
            }
            if (!visiting.insert(event_id).second) {
                throw std::invalid_argument(
                    "DMA event dependency graph contains a cycle");
            }
            const auto found = pending_by_id.find(event_id);
            if (found != pending_by_id.end()) {
                for (const auto& dependency : found->second->dependencies) {
                    if (pending_by_id.find(dependency) != pending_by_id.end()) {
                        visit(dependency);
                    }
                }
            }
            visiting.erase(event_id);
            visited.insert(event_id);
        };
    for (const auto& pending : pending_) {
        visit(pending.job.event_id);
    }
}

bool DmaScheduler::dependencies_complete(const PendingJob& pending) const {
    return std::all_of(
        pending.job.dependencies.begin(), pending.job.dependencies.end(),
        [this](const std::string& dependency) {
            return completed_ids_.find(dependency) != completed_ids_.end();
        });
}

std::optional<std::size_t> DmaScheduler::select_pending(uint64_t now_ns) const {
    const auto ordering_key = [](const PendingJob& pending) {
        return std::make_tuple(static_cast<uint8_t>(pending.job.priority),
                               pending.job.ready_ns, pending.submission_seq,
                               pending.job.event_id);
    };
    std::optional<std::size_t> best;
    std::optional<std::size_t> best_lower_priority;
    for (std::size_t index = 0; index < pending_.size(); ++index) {
        const auto& candidate = pending_[index];
        if (candidate.job.ready_ns > now_ns ||
            !dependencies_complete(candidate) ||
            (candidate.job.page_movement &&
             in_flight_page_movements_ >=
                 config_.max_in_flight_page_movements)) {
            continue;
        }
        if (!best.has_value() ||
            ordering_key(candidate) < ordering_key(pending_[*best])) {
            best = index;
        }
        if (!is_high_priority(candidate.job.priority) &&
            (!best_lower_priority.has_value() ||
             ordering_key(candidate) <
                 ordering_key(pending_[*best_lower_priority]))) {
            best_lower_priority = index;
        }
    }
    if (consecutive_high_priority_starts_ >= config_.max_priority_burst &&
        best_lower_priority.has_value()) {
        return best_lower_priority;
    }
    return best;
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
    while (in_flight_.size() < engine_busy_.size()) {
        const auto selected = select_pending(now_ns);
        if (!selected.has_value()) {
            break;
        }
        PendingJob pending = std::move(pending_[*selected]);
        pending_.erase(pending_.begin() +
                       static_cast<std::ptrdiff_t>(*selected));
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
