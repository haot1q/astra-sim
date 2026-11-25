#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <stdexcept>

namespace AstraSimAnalytical {
namespace {
using Json = JointRuntimeClient::Json;

uint64_t business(const Json& state) {
    const auto admitted = JointRuntimeClient::decimal(state.at("admitted_nodes"));
    const auto completed = JointRuntimeClient::decimal(state.at("completed_nodes"));
    if (completed > UINT64_MAX - admitted) throw std::overflow_error("joint business count overflow");
    return admitted + completed;
}

bool has_key(const Json& status, const std::string& key) {
    for (const auto& row : status.at("domains"))
        if (key == "1/" + row.at("id").get<std::string>()) return true;
    for (const auto& row : status.at("endpoints"))
        if (key == "2/" + row.at("id").get<std::string>()) return true;
    return false;
}

// A zero-step reply cannot consume or reschedule any known memory event.
std::map<std::string, uint64_t> pending_heads(const Json& status) {
    std::map<std::string, uint64_t> heads;
    for (const auto& row : status.at("domains")) {
        if (!row.at("next_fs").is_null())
            heads.emplace("1/" + row.at("id").get<std::string>(), JointRuntimeClient::decimal(row.at("next_fs")));
    }
    for (const auto& row : status.at("endpoints")) {
        if (row.at("state") != "DRAINED" && row.at("state") != "CLOSED" && !row.at("next_fs").is_null())
            heads.emplace("2/" + row.at("id").get<std::string>(), JointRuntimeClient::decimal(row.at("next_fs")));
    }
    return heads;
}

void check_frontier(const Json& reply, uint64_t cut) {
    for (const auto* collection : {"domains", "endpoints"}) {
        for (const auto& row : reply.at(collection)) {
            if (row.value("state", "") == "CLOSED" || row.value("state", "") == "DRAINED") continue;
            if (!row.at("next_fs").is_null() && JointRuntimeClient::decimal(row.at("next_fs")) < cut)
                throw std::invalid_argument("memory grant closed over a pending event");
        }
    }
}
}

bool JointSimulationCoordinator::waiting_for_supply(const std::string& domain) const {
    const auto& waits = status_.at("supply_wait");
    const auto& wakes = status_.at("supply_wake");
    if (!waits.is_object() || !wakes.is_array())
        throw std::invalid_argument("missing authoritative supply state");
    if (!waits.contains(domain)) return false;
    const auto& proof = waits.at(domain);
    AstraSim::ServiceBindingJson::fields(proof, {"view","head","capacity","used"});
    const auto capacity = JointRuntimeClient::decimal(proof.at("capacity"));
    const auto view = proof.at("view").get<std::string>();
    if (!capacity || capacity != JointRuntimeClient::decimal(proof.at("used")) ||
        view.size() != 64 || view.find_first_not_of("0123456789abcdef") != std::string::npos ||
        (!view_identity_.is_null() && view_identity_.at("manifest_sha256") != view) ||
        proof.at("head").get<std::string>().empty() ||
        std::find(wakes.begin(), wakes.end(), Json(domain)) != wakes.end())
        throw std::invalid_argument("invalid logical-capacity wait receipt");
    return true;
}

void JointSimulationCoordinator::advance_memory(const Candidates& candidates,
                                                const std::string& chosen, uint64_t first) {
    const auto hard = limit("horizon_fs");
    auto horizon = candidates.count("0") ? std::min(hard, candidates.at("0").first) : hard;
    bool exact = exact_memory_retry_;
    if (memory_controls_fs_) {
        const auto control = memory_controls_fs_->lower_bound(first);
        if (control != memory_controls_fs_->end()) {
            horizon = std::min(horizon, *control);
            if (*control == first) exact = true;
        }
    }
    for (const auto& row : status_.at("domains")) {
        const auto domain = row.at("id").get<std::string>();
        if (!status_.at("supply_idle").at(domain).get<bool>() && !waiting_for_supply(domain))
            exact = true;
    }
    if (config_.contains("hold") && !hold_released_ && status_.at("ready") == true) {
        const auto& hold = config_.at("hold");
        const auto release = JointRuntimeClient::decimal(hold.at("release_fs"));
        if (!hold_set_ || release <= first) exact = true;
        if (release > first) horizon = std::min(horizon, release);
        if (chosen == "2/" + hold.at("endpoint").get<std::string>() &&
            (!hold_set_ || first >= release)) {
            if (hold_set_ && first > release) throw std::runtime_error("missed original hold release edge");
            hold_released_ = first >= release;
            call("cx_control", {{"id",hold.at("endpoint")}, {"values",{
                {"hold_rsp",hold_released_ ? 0 : hold.at("value").get<int>()}}}});
            hold_set_ = true;
        }
    }
    exact = exact || first == horizon;
    const auto same_limit = limit("same_time_events");
    const auto same = first == frontier_fs_ ? same_time_ : 0;
    if (!admits_events(1) || same >= same_limit)
        throw std::runtime_error("joint event budget exhausted before memory grant");
    const auto before = business(status_);
    if (before < progress_) throw std::invalid_argument("joint business progress regressed");
    const auto events_left = whole_job_ ? std::min(memory_events_per_call_, UINT64_MAX - events_) :
        limit("events") - events_;
    const Json grant{{"schema","joint-memory-grant-v1"}, {"first_key",chosen},
        {"first_fs",std::to_string(first)}, {"frontier_fs",std::to_string(frontier_fs_)},
        {"cursor",first == frontier_fs_ ? tie_ : ""}, {"horizon_fs",std::to_string(horizon)},
        {"exact",exact}, {"events_left",std::to_string(events_left)},
        {"same_time_limit",std::to_string(same_limit)}, {"same_time_used",std::to_string(same)},
        {"idle_limit",std::to_string(limit("no_progress_events"))}, {"idle_used",std::to_string(idle_events_)},
        {"enforce_idle",!awaiting_}, {"progress_pending",before > progress_}};
    call("advance_memory", grant, [&](const Json& reply) { install_memory_grant(grant, reply, before); });
    rethrow_failure();
}

void JointSimulationCoordinator::install_memory_grant(const Json& grant, const Json& reply, uint64_t before) {
    const auto first = JointRuntimeClient::decimal(grant.at("first_fs"));
    const auto horizon = JointRuntimeClient::decimal(grant.at("horizon_fs"));
    const auto same = JointRuntimeClient::decimal(grant.at("same_time_used"));
    const auto same_limit = limit("same_time_events"), hard = limit("horizon_fs");
    const auto& chosen = grant.at("first_key");
    const bool exact = grant.at("exact");
    const auto& facts = reply.at("advanced");
    AstraSim::ServiceBindingJson::fields(facts, {"schema","first_fs","first_key","horizon_fs","exact",
        "events","last_fs","last_key","closed_before_fs","same_time_used","idle_used","reason"});
    for (const auto* key : {"schema","first_fs","first_key","horizon_fs","exact"})
        if (facts.at(key) != grant.at(key)) throw std::invalid_argument("memory grant identity differs");
    const auto count = JointRuntimeClient::decimal(facts.at("events"));
    const auto cut = JointRuntimeClient::decimal(facts.at("closed_before_fs"));
    const auto tail = JointRuntimeClient::decimal(facts.at("same_time_used"));
    const auto idle = JointRuntimeClient::decimal(facts.at("idle_used"));
    const auto reason = facts.at("reason").get<std::string>();
    static const std::set<std::string> reasons{"BOUNDARY","EXACT","NODE_COMPLETE","SUPPLY_WAKE",
        "UPPER_OUTPUT","CONTROL","LIFECYCLE","BUDGET","WAIT_INPUT"};
    const auto& wakes = reply.at("supply_wake");
    if (!reasons.count(reason) || !wakes.is_array() || count > JointRuntimeClient::decimal(grant.at("events_left")) ||
        cut < frontier_fs_ || cut > horizon || (exact && count > 1) ||
        (reason == "EXACT" && (!exact || count != 1)) || (reason == "BOUNDARY" && cut != horizon) ||
        (reason == "SUPPLY_WAKE" && wakes.empty()) ||
        (reason == "NODE_COMPLETE" && reply.at("completions").empty()) ||
        tail > same_limit || (!awaiting_ && idle > limit("no_progress_events")) || business(reply) < before)
        throw std::invalid_argument("invalid memory grant outcome");
    const auto after = business(reply);
    if (count && after == before) {
        const auto idle_start = before > progress_ ? 0 : idle_events_;
        const auto idle_steps = before > progress_ ? count - 1 : count;
        if (idle_steps > UINT64_MAX - idle_start || idle != idle_start + idle_steps)
            throw std::invalid_argument("memory grant idle accounting differs from actual steps");
    } else if (count && idle != 0) {
        throw std::invalid_argument("memory grant crossed the first business completion");
    }
    check_frontier(reply, cut);
    const auto previous = frontier_fs_;
    if (count) {
        const auto last = JointRuntimeClient::decimal(facts.at("last_fs"));
        const auto key = facts.at("last_key").get<std::string>();
        if (!has_key(reply,key) || last < first || last > cut || last >= hard ||
            (!exact && last >= horizon) || !tail ||
            (count == 1 && (key != chosen || last != first)) ||
            (last == first && (count > same_limit - same || tail != same + count)) || (last > first && tail >= count) ||
            ((!wakes.empty() || !reply.at("completions").empty()) && cut != last))
            throw std::invalid_argument("invalid memory grant executed prefix");
        tie_ = cut == last ? key : "";
        same_time_ = cut == last ? tail : 0;
    } else {
        if (!facts.at("last_key").is_null() || !facts.at("last_fs").is_null() ||
            tail != same || idle != idle_events_ || cut > first ||
            pending_heads(reply) != pending_heads(status_))
            throw std::invalid_argument("zero-step grant changed a pending event or its accounting");
        if (reason == "BUDGET" || (exact && cut == previous && wakes.empty() &&
            reply.at("completions").empty()))
            throw std::runtime_error("memory grant cannot make authorized progress");
        if (cut > previous) { tie_.clear(); same_time_ = 0; }
    }
    exact_memory_retry_ = !count && cut == previous;
    if (cut > previous) closed_ns_ = (cut - 1) / 1000000;
    frontier_fs_ = cut;
    events_ += count;
    idle_events_ = idle;
    if (count) progress_ = after;
}
}
