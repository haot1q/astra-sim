#include "common/JointSimulationCoordinator.hh"
#include "ExternalMemoryFixture.hh"
#include <algorithm>
#include <deque>
#include <string>
#include <vector>

namespace {
using Owner = AstraSimAnalytical::JointSimulationCoordinator;
using Json = Owner::Json;
using ExternalMemoryTests::require;
struct Result {
    std::vector<std::pair<std::string, uint64_t>> visits;
    unsigned grants = 0, multi = 0, empty = 0;
};

Result simulate(bool batch, const std::string& domain, bool tight, bool zero_first, unsigned seed) {
    auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
    Json cfg{{"run_id","mw"}, {"session_id","mw"}, {"mw_advance_mode",batch ? "safe-before-v1" : "step"},
        {"open", {{"engine_budget", {{"events_per_call", 4u}}}}},
        {"limits", {{"pending_nodes","16"}, {"descriptor_bytes","4096"}, {"callbacks","32"},
            {"events","120"}, {"same_time_events","12"}, {"no_progress_events","120"},
            {"horizon_fs","20000000"}, {"supply_frames","1"}}}};
    if (tight) cfg["limits"]["no_progress_events"] = "17";
    std::map<std::string,std::deque<uint64_t>> events{
        {domain,{1000000,1000000,1000000,2100000,2100000,3000000,5000000}},
        {"second",{2100000,3000000}}, {"card",{3000000,4000000}}};
    // Different legal domain schedules, including ties, vary independently of names.
    const uint64_t extra = 7100000 + 100000 * (seed % 4);
    events.at(domain).push_back(extra);
    events.at(domain).push_back(extra);
    events.at("second").push_back(extra + 100000 * ((seed / 4) % 3));
    Result result;
    bool sent = false, blocked = (seed % 3 == 2);
    auto insert_card = [&](uint64_t time) {
        auto& card = events.at("card");
        card.insert(std::upper_bound(card.begin(), card.end(), time), time);
    };
    auto peek = [&](const std::string& id) -> Json {
        const auto& list = events.at(id);
        return list.empty() ? Json(nullptr) : Json(std::to_string(list.front()));
    };
    auto step = [&](const std::string& id) {
        const auto time = events.at(id).front();
        events.at(id).pop_front();
        result.visits.emplace_back(id, time);
        // A visible message generated at 1 ns must stop the current grant.
        if (!sent && id == domain && time == 1000000) {
            sent = true;
            insert_card(time);
            return true;
        }
        return false;
    };
    auto exchange = [&](const std::string& command, const Json& body) {
        Json advanced = nullptr;
        if (command == "advance_before") {
            ++result.grants;
            const auto id = body.at("id").get<std::string>();
            const auto horizon = std::stoull(body.at("horizon_fs").get<std::string>());
            const auto start = events.at(id).front();
            for (const auto& [other, rows] : events)
                if (other != id && !rows.empty()) require(horizon <= rows.front(), "grant crossed a competing edge");
            require(horizon > start && horizon <= start + 1, "grant lost exact same-time budget");
            unsigned count = 0;
            bool outbound = false;
            if (!zero_first || result.grants != 1)
                while (!events.at(id).empty() && events.at(id).front() < horizon && count < 4) {
                    outbound = step(id);
                    ++count;
                    if (outbound) break;
                }
            result.multi += count >= 2;
            result.empty += !count;
            advanced = body;
            advanced["closed_before_fs"] = std::to_string(count && !outbound ? horizon : start);
            advanced["processed_fs"] = count ? Json(std::to_string(start)) : Json(nullptr);
            advanced["next_event_fs"] = peek(id);
            advanced["reason"] = count && !outbound ? (peek(id).is_null() ? "EMPTY" : "BOUNDARY") : "OUTBOUND";
            advanced["event_count"] = std::to_string(count);
        } else if (command == "supply") {
            // Backpressure is retried by the original safe_point logic.
        } else if (command == "step_at") {
            blocked = false;
            step(body.at("id").get<std::string>());
        } else require(command == "open" || command == "abort", "unexpected fake exchange");
        Json reply{{"ready",true}, {"native_state","READY"}, {"completions",Json::array()},
            {"completed_nodes","0"}, {"admitted_nodes","0"}, {"business_not_before_fs","0"},
            {"supply_idle",{{domain,!blocked},{"second",true}}}, {"domains",Json::array({
                {{"id",domain},{"next_fs",peek(domain)}}, {{"id","second"},{"next_fs",peek("second")}}})},
            {"endpoints",Json::array({{{"id","card"},{"state","READY"},{"next_fs",peek("card")}}})}};
        if (command == "supply") reply["supply_reason"] = blocked ? "blocked" : "idle";
        if (!advanced.is_null()) reply["advanced"] = advanced;
        return reply;
    };
    auto owner = std::make_shared<Owner>(queue, cfg, exchange);
    queue->schedule_event(2, [](void* ptr) {
        static_cast<Result*>(ptr)->visits.emplace_back("callback", 2000000);
    }, &result);
    for (unsigned i=0; result.visits.size() < 16; ++i) {
        require(i < 24,"bounded grant zero-progress loop");
        owner->advance();
    }
    owner->abort();
    return result;
}
}

void joint_mw_advance_contract() {
    for (const std::string name : {"alpha","renamed"}) {
        for (const bool tight : {false,true}) {
            for (unsigned seed = 0; seed < 12; ++seed) {
                const auto ref = simulate(false, name, tight, false, seed);
                const auto actual = simulate(true, name, tight, false, seed);
                const auto zero = simulate(true, name, tight, true, seed);
                require(ref.visits == actual.visits && ref.visits == zero.visits,
                        "batched trace differs from reference order or simulated time");
                require(actual.multi && zero.empty == 1, "real batching or zero recovery not exercised");
            }
        }
    }
}
