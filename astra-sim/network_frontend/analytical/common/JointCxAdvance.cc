#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <stdexcept>

namespace AstraSimAnalytical {
namespace {
constexpr uint64_t kMaxBeforeBatches = 65536;
uint64_t business_progress(const JointRuntimeClient::Json& status) {
    const auto admitted = JointRuntimeClient::decimal(status.at("admitted_nodes"));
    const auto completed = JointRuntimeClient::decimal(status.at("completed_nodes"));
    if (completed > UINT64_MAX - admitted) throw std::overflow_error("joint progress overflow");
    return admitted + completed;
}
}

bool JointSimulationCoordinator::advance_cx(const Candidates& candidates,
        const std::string& chosen, uint64_t first) {
    if (config_.value("cx_advance_mode", std::string("step")) != "safe-before-v1" ||
        chosen.rfind("2/", 0) != 0 || awaiting_ || handoff_ || draining_ || config_.contains("hold") ||
        !status_.at("ready").get<bool>()) return false;
    const auto endpoint = chosen.substr(2);
    const auto& endpoints = status_.at("endpoints");
    const auto current = std::find_if(endpoints.begin(), endpoints.end(),
        [&](const auto& row) { return row.at("id") == endpoint; });
    if (current == endpoints.end() || current->at("state") != "READY") return false;
    uint64_t horizon = limit("horizon_fs");
    for (const auto& [id, candidate] : candidates) {
        if (id == chosen) continue;
        if (candidate.first == first) return false;
        horizon = std::min(horizon, candidate.first);
    }
    if (horizon <= first) return false;
    // safe_point exhausted supply to idle/blocked. No asynchronous producer can
    // enter this synchronous owner before its next ASTRA/MW/card/control edge.
    const auto before_progress = business_progress(status_);
    if (before_progress < progress_) throw std::runtime_error("joint progress regressed");
    const auto reserve_idle = before_progress > progress_ ? kMaxBeforeBatches - 1 : kMaxBeforeBatches;
    const auto idle_base = before_progress > progress_ ? 0 : idle_events_;
    if (first == frontier_fs_ && same_time_ >= limit("same_time_events"))
        throw std::runtime_error("joint event budget exhausted before CX grant");
    const auto first_same = first == frontier_fs_ ? same_time_ + 1 : 1;
    if (!admits_events(kMaxBeforeBatches) ||
        idle_base > limit("no_progress_events") || limit("no_progress_events") - idle_base < reserve_idle ||
        first_same > limit("same_time_events")) return false;

    Json body{{"engine", "cx"}, {"id", endpoint}, {"horizon_fs", std::to_string(horizon)}};
    uint64_t batches = 0;
    call("advance_before", body, [&](const Json& reply) {
        const auto& advanced = reply.at("advanced");
        AstraSim::ServiceBindingJson::fields(advanced,
            {"engine", "id", "horizon_fs", "kernel_fs", "processed_fs", "batch_count"});
        for (const auto& key : {"engine", "id", "horizon_fs"})
            if (advanced.at(key) != body.at(key)) throw std::invalid_argument("CX advance identity differs");
        const auto kernel = JointRuntimeClient::decimal(advanced.at("kernel_fs"));
        const auto processed = JointRuntimeClient::decimal(advanced.at("processed_fs"));
        batches = JointRuntimeClient::decimal(advanced.at("batch_count"));
        if (!batches || batches > kMaxBeforeBatches || processed < first || processed >= horizon ||
            kernel < processed || kernel > horizon || (kernel < horizon && kernel != processed) ||
            (batches == 1 ? processed != first : processed == first))
            throw std::invalid_argument("invalid CX actual stop or batch count");
        const auto& after = reply.at("endpoints");
        const auto found = std::find_if(after.begin(), after.end(),
            [&](const auto& row) { return row.at("id") == endpoint; });
        if (found == after.end()) throw std::invalid_argument("CX advance endpoint disappeared");
        if (!found->at("next_fs").is_null()) {
            const auto next = JointRuntimeClient::decimal(found->at("next_fs"));
            if (next < kernel || next <= processed) throw std::invalid_argument("CX advance next edge regressed");
        }
        if (business_progress(reply) < before_progress) throw std::invalid_argument("CX advance progress regressed");
        // Validate all new stop facts before the existing consume() schedules
        // completions. The actual kernel cut, not the requested H, owns time.
        if (kernel != frontier_fs_ && kernel) closed_ns_ = (kernel - 1) / 1000000;
        frontier_fs_ = kernel;
        events_ += batches;
        same_time_ = kernel > processed ? 0 : (batches == 1 ? first_same : 1);
        tie_ = kernel > processed ? std::string() : chosen;
    });
    const auto after_progress = business_progress(status_);
    idle_events_ = after_progress > before_progress ? 0 :
        (before_progress > progress_ ? batches - 1 : idle_events_ + batches);
    progress_ = after_progress;
    rethrow_failure();
    return true;
}
}
