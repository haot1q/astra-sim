#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <stdexcept>

namespace AstraSimAnalytical {
bool JointSimulationCoordinator::advance_mw(const Candidates& candidates, const std::string& chosen, uint64_t first) {
    auto fallback = [&](const char* reason) { ++mw_fallbacks_[reason]; return false; };
    if (mw_retry_step_) {
        mw_retry_step_ = false;
        return fallback("zero_progress");
    }
    if (awaiting_ || handoff_ || draining_ || !status_.at("ready").get<bool>() ||
        status_.at("native_state") != "READY" || config_.contains("hold") ||
        (config_.contains("software_controls") && !config_["software_controls"].empty()))
        return fallback("phase_or_control");
    for (const auto& row : status_.at("domains"))
        if (!status_.at("supply_idle").at(row.at("id").get<std::string>()).get<bool>())
            return fallback("pending_input");
    for (const auto& row : status_.at("endpoints"))
        if (row.at("state") != "READY") return fallback("unknown_edge");

    // The v1 reply gives an event count and the final timestamp. It cannot
    // reconstruct the same-time budget across multiple distinct timestamps.
    // Integer femtoseconds make [first, first+1) an exact single-timestamp grant.
    // advance() already checked first < the global horizon (so +1 fits).
    uint64_t horizon = std::min(limit("horizon_fs"), first + 1);
    for (const auto& [id, edge] : candidates)
        if (id != chosen) horizon = std::min(horizon, edge.first);
    if (horizon <= first) return fallback("tie");
    const auto remaining = [](uint64_t used, uint64_t cap, uint64_t need) {
        return used <= cap && need <= cap - used;
    };
    const auto before = JointRuntimeClient::decimal(status_.at("completed_nodes")) +
                        JointRuntimeClient::decimal(status_.at("admitted_nodes"));
    const auto same = first == frontier_fs_ ? same_time_ : 0;
    if (!admits_events(mw_per_call_) ||
        !remaining(same, limit("same_time_events"), mw_per_call_) ||
        !remaining(before == progress_ ? idle_events_ : 0, limit("no_progress_events"), mw_per_call_))
        return fallback("budget");

    Json body{{"engine","mw"}, {"id",chosen.substr(2)}, {"horizon_fs",std::to_string(horizon)}};
    uint64_t count = 0;
    call("advance_before", body, [&](const Json& reply) {
        const auto& facts = reply.at("advanced");
        AstraSim::ServiceBindingJson::fields(facts, {"engine","id","horizon_fs","processed_fs",
            "closed_before_fs","next_event_fs","reason","event_count"});
        for (const auto& key : {"engine","id","horizon_fs"})
            if (facts.at(key) != body.at(key)) throw std::invalid_argument("Akita grant identity differs");
        count = JointRuntimeClient::decimal(facts.at("event_count"));
        const auto closed = JointRuntimeClient::decimal(facts.at("closed_before_fs"));
        const auto& last = facts.at("processed_fs");
        const auto& next = facts.at("next_event_fs");
        const auto reason = facts.at("reason").get<std::string>();
        if (count > mw_per_call_ || closed > horizon ||
            (count && (last.is_null() || JointRuntimeClient::decimal(last) != first)) ||
            (!last.is_null() && JointRuntimeClient::decimal(last) > closed) ||
            (!next.is_null() && JointRuntimeClient::decimal(next) < closed) ||
            ((reason == "EMPTY" || reason == "BOUNDARY") && closed != horizon) ||
            (reason == "BOUNDARY" && next.is_null()) ||
            ((reason == "EMPTY" || reason == "WAIT_INPUT") && !next.is_null()) ||
            (reason == "YIELD_BUDGET" && !count) ||
            (reason != "EMPTY" && reason != "BOUNDARY" && reason != "WAIT_INPUT" &&
             reason != "YIELD_BUDGET" && reason != "OUTBOUND"))
            throw std::invalid_argument("invalid Akita grant stop facts");
        if (!count) {
            // A zero grant can still deliver an output. Re-select with the new
            // status, then step individually before attempting another MW grant.
            mw_retry_step_ = true;
            return;
        }
        if (first != frontier_fs_) closed_ns_ = (first - 1) / 1000000;
        frontier_fs_ = first;
        tie_ = chosen;
        events_ += count;
        same_time_ = same + count;
    });
    if (count) {
        const auto after = JointRuntimeClient::decimal(status_.at("completed_nodes")) +
                           JointRuntimeClient::decimal(status_.at("admitted_nodes"));
        if (after < before || before < progress_) throw std::runtime_error("joint progress regressed");
        idle_events_ = after > before ? 0 : (before > progress_ ? count - 1 : idle_events_ + count);
        progress_ = after;
    }
    rethrow_failure();
    return true;
}
}
