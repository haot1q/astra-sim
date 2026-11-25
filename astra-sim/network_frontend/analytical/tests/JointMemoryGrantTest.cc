#include "common/JointSimulationCoordinator.hh"
#include "ExternalMemoryFixture.hh"
#include <cstdlib>
#include <deque>

namespace {
using Owner = AstraSimAnalytical::JointSimulationCoordinator;
using Json = Owner::Json;
using ExternalMemoryTests::require;

struct Trace {
    std::deque<std::pair<std::string, uint64_t>> events{
        {"1/d", 10}, {"2/c", 20}, {"1/d", 30}, {"1/d", 1000000}};
    std::vector<std::string> visits;
    std::vector<Json> authorizations;
    size_t grants = 0, supplies = 0;
    bool wake, waiting;

    explicit Trace(bool blocked) : wake(blocked), waiting(blocked) {}

    Json next(const std::string& key) const {
        for (const auto& row : events) {
            if (row.first == key) return std::to_string(row.second);
        }
        return nullptr;
    }

    Json status() const {
        Json waits = Json::object();
        if (waiting) waits["d"] = {{"view", std::string(64, 'c')}, {"head", "logical-0"},
                                   {"capacity","1"}, {"used","1"}};
        return {{"ready",true}, {"native_state","READY"}, {"completions",Json::array()},
            {"completed_nodes","0"}, {"admitted_nodes","0"}, {"business_not_before_fs","0"},
            {"supply_idle",{{"d",!wake}}}, {"supply_wait",waits},
            {"supply_wake",Json::array()},
            {"domains",Json::array({{{"id","d"},{"next_fs",next("1/d")}}})},
            {"endpoints",Json::array({{{"id","c"},{"state","READY"},{"next_fs",next("2/c")}}})}};
    }

    Json exchange(const std::string& op, const Json& grant) {
        if (op == "open" || op == "abort") return status();
        if (op == "supply") {
            require(!waiting && wake, "unnecessary external supply poll");
            wake = false;
            ++supplies;
            auto reply = status();
            reply["supply_reason"] = "idle";
            return reply;
        }
        require(op == "advance_memory", "expected one cross-component grant");
        ++grants;
        authorizations.push_back(grant);
        require(grant.at("first_key") == events.front().first &&
                grant.at("first_fs") == std::to_string(events.front().second), "incorrect first key/time");
        const bool exact = grant.at("exact");
        const auto horizon = std::stoull(grant.at("horizon_fs").get<std::string>());
        size_t count = 0;
        std::pair<std::string, uint64_t> last;
        bool notify = false;
        while (!events.empty() && (exact ? count == 0 : events.front().second < horizon)) {
            last = events.front();
            events.pop_front();
            visits.push_back(last.first);
            ++count;
            if (waiting && last.second == 20) {
                waiting = false;
                notify = true;
                break;
            }
        }
        auto reply = status();
        if (notify) reply["supply_wake"].push_back("d");
        const auto cut = notify || exact ? last.second : horizon;
        reply["advanced"] = {{"schema","joint-memory-grant-v1"}, {"first_fs",grant.at("first_fs")},
            {"first_key",grant.at("first_key")}, {"horizon_fs",grant.at("horizon_fs")}, {"exact",exact},
            {"events",std::to_string(count)}, {"last_fs",std::to_string(last.second)}, {"last_key",last.first},
            {"closed_before_fs",std::to_string(cut)}, {"same_time_used",count == 1
                ? std::to_string(std::stoull(grant.at("same_time_used").get<std::string>()) + 1) : "1"},
            {"idle_used",std::to_string(std::stoull(grant.at("idle_used").get<std::string>()) + count)},
            {"reason",notify ? "SUPPLY_WAKE" : exact ? "EXACT" : "BOUNDARY"}};
        return reply;
    }
};
}

void joint_memory_grant_contract() {
    if (std::getenv("CX_EXPERIMENT_MEMORY_SCOPE")) {
        Trace trace(false);
        auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
        queue->schedule_event(1, [](void*) {}, nullptr);
        Json cfg{{"memory_engine","in-process"}, {"host_budget_policy","cgroup-v2-whole-job-v1"},
            {"open",{{"engine_budget",{{"events_per_call",3u}}}}},
            {"limits",{{"pending_nodes","16"},{"descriptor_bytes","4096"},{"callbacks","32"},
                       {"events","1"},{"same_time_events","12"},{"no_progress_events","12"},
                       {"horizon_fs","2000000"},{"supply_frames","1"}}}};
        Owner owner(queue, cfg, [&](const auto& op, const auto& body) { return trace.exchange(op, body); });
        owner.advance();
        require(trace.visits.size() == 3 && trace.authorizations.at(0).at("events_left") == "3",
                "scoped memory grant did not use the native per-call budget");
        owner.advance();
        owner.advance();
        require(trace.visits.size() == 4 && trace.authorizations.at(1).at("events_left") == "3",
                "next grant failed after reaching the old cumulative limit");
        owner.abort();
    }
    for (const bool wake : {false, true}) {
        Trace trace(wake);
        auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
        const auto callback = [](void* ptr) {
            static_cast<Trace*>(ptr)->visits.push_back("0");
        };
        queue->schedule_event(1, callback, &trace);
        queue->schedule_event(1, callback, &trace);
        Json cfg{{"memory_engine","in-process"}, {"limits",{{"pending_nodes","16"},
            {"descriptor_bytes","4096"}, {"callbacks","32"}, {"events","12"},
            {"same_time_events","12"}, {"no_progress_events","12"}, {"horizon_fs","2000000"},
            {"supply_frames","1"}}}};
        Owner owner(queue, cfg, [&](const auto& op, const auto& body) { return trace.exchange(op, body); });
        owner.advance();
        require(trace.grants == 1 && trace.supplies == 0, "initial grant crossed a real wake or repolled");
        if (wake) {
            require(owner.frontier_fs() == 20, "late delivery of newly available slot");
            owner.advance();
            require(trace.supplies == 1 && trace.grants == 2, "first retry did not precede more work");
        }
        require(owner.frontier_fs() == 1000000, "ordinary window did not reach the external boundary");
        owner.advance();
        owner.advance();
        owner.advance();
        require(trace.visits == std::vector<std::string>({"1/d","2/c","1/d","0","1/d","0"}),
                "external tie or cross-component work changed order");
        owner.abort();
    }
    // A real local event at 20 ns separates a memory request at 10 ns from the
    // returning event at 30 ns. The second local callback forces a real tie.
    {
        constexpr uint64_t ns = 1000000;
        Trace trace(false);
        trace.events = {{"1/d", 10 * ns}, {"2/c", 20 * ns}, {"1/d", 30 * ns}};
        auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
        const auto notify = [](void* context) { static_cast<Trace*>(context)->visits.push_back("0"); };
        queue->schedule_event(20, notify, &trace);
        queue->schedule_event(20, notify, &trace);
        queue->schedule_event(100, notify, &trace);
        Json cfg{{"memory_engine","in-process"}, {"limits",{{"pending_nodes","16"},
            {"descriptor_bytes","4096"}, {"callbacks","32"}, {"events","12"}, {"same_time_events","12"},
            {"no_progress_events","12"}, {"horizon_fs","110000000"}, {"supply_frames","1"}}}};
        Owner owner(queue, cfg, [&](const auto& op, const auto& body) { return trace.exchange(op, body); });
        owner.advance();
        require(trace.authorizations.size() == 1 &&
                trace.authorizations.front().at("horizon_fs") == "20000000" &&
                trace.visits == std::vector<std::string>{"1/d"} && owner.frontier_fs() == 20 * ns,
                "10 ns request advanced through the upper callback at 20 ns");
        owner.advance();  // First ASTRA callback at 20 ns.
        owner.advance();  // Same-timestamp CX grant, strictly one event.
        require(trace.authorizations.size() == 2 && trace.authorizations[1].at("exact") == true &&
                trace.authorizations[1].at("first_fs") == "20000000" &&
                trace.authorizations[1].at("horizon_fs") == "20000000",
                "CX did not re-enter arbitration against the second callback at 20 ns");
        owner.advance();  // Second ASTRA callback at 20 ns.
        owner.advance();  // Now the 30 ns return can advance.
        require(trace.visits == std::vector<std::string>({"1/d","0","2/c","0","1/d"}),
                "local and CX callbacks at 20 ns changed order");
        owner.abort();
    }
    // A zero-step window may yield before the still-promised head. It cannot
    // consume it, change another component's queue, or close over either.
    for (const std::string change : {"none","drop_chosen","drop_peer","cross"}) {
        Trace trace(false);
        auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
        queue->schedule_event(1, [](void*) {}, nullptr);
        Json cfg{{"memory_engine","in-process"}, {"limits",{{"pending_nodes","16"},
            {"descriptor_bytes","4096"}, {"callbacks","32"}, {"events","12"}, {"same_time_events","12"},
            {"no_progress_events","12"}, {"horizon_fs","2000000"}, {"supply_frames","1"}}}};
        bool returned = false;
        Owner owner(queue, cfg, [&](const auto& op, const auto& body) {
            if (op != "advance_memory" || returned) return trace.exchange(op,body);
            returned = true;
            auto reply = trace.status();
            if (change == "drop_chosen") reply["domains"][0]["next_fs"] = nullptr;
            if (change == "drop_peer") reply["endpoints"][0]["next_fs"] = nullptr;
            const std::string cut = change == "cross" ? "1000000" : "0";
            reply["advanced"] = {{"schema","joint-memory-grant-v1"},{"first_fs",body.at("first_fs")},
                {"first_key",body.at("first_key")},{"horizon_fs",body.at("horizon_fs")},{"exact",body.at("exact")},
                {"events","0"},{"last_fs",nullptr},{"last_key",nullptr},{"closed_before_fs",cut},
                {"same_time_used",body.at("same_time_used")},{"idle_used",body.at("idle_used")},
                {"reason","WAIT_INPUT"}};
            return reply;
        });
        bool failed = false;
        try { owner.advance(); } catch (const std::exception&) { failed = true; }
        require(failed == (change != "none") && owner.frontier_fs() == 0, "invalid zero-step frontier");
        if (!failed) {
            owner.advance();
            require(trace.authorizations.size() == 1 && trace.authorizations[0].at("exact") == true &&
                    trace.visits == std::vector<std::string>{"1/d"}, "legal zero-step retry skipped its original head");
        }
        owner.abort();
    }
    // An incorrect cut, stop fact or capacity proof cannot affect the receive clock.
    for (const std::string damage : {"identity","horizon","last_key","idle","tail","wake","closed","wait"}) {
        Trace trace(damage == "wait");
        auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
        queue->schedule_event(1, [](void*) {}, nullptr);
        Json cfg{{"memory_engine","in-process"}, {"limits",{{"pending_nodes","16"},
            {"descriptor_bytes","4096"}, {"callbacks","32"}, {"events","12"}, {"same_time_events","12"},
            {"no_progress_events","12"}, {"horizon_fs","2000000"}, {"supply_frames","1"}}}};
        Owner owner(queue, cfg, [&](const auto& op, const auto& body) {
            auto reply = trace.exchange(op,body);
            if (op == "open" && damage == "wait") reply["supply_wait"]["d"]["used"] = "0";
            if (op != "advance_memory") return reply;
            auto& facts = reply["advanced"];
            if (damage == "identity") facts["first_key"] = "2/other";
            if (damage == "horizon") facts["last_fs"] = facts["horizon_fs"];
            if (damage == "last_key") facts["last_key"] = "2/other";
            if (damage == "idle") facts["idle_used"] = "0";
            if (damage == "tail") facts["same_time_used"] = facts["events"];
            if (damage == "wake") facts["reason"] = "SUPPLY_WAKE";
            if (damage == "closed") reply["domains"][0]["next_fs"] = "999999";
            return reply;
        });
        bool failed = false;
        try { owner.advance(); } catch (const std::exception&) { failed = true; }
        require(failed && owner.frontier_fs() == 0, "invalid grant installed unproved frontier");
        owner.abort();
    }
}
