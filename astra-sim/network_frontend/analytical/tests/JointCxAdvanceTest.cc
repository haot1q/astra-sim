#include "common/JointSimulationCoordinator.hh"
#include "ExternalMemoryFixture.hh"
#include <string>
#include <vector>

namespace {
using Owner = AstraSimAnalytical::JointSimulationCoordinator;
using Json = Owner::Json;
using ExternalMemoryTests::require;
using ExternalMemoryTests::rejects;
Json config() {
    return {{"run_id", "before"}, {"session_id", "before"}, {"cx_advance_mode", "safe-before-v1"},
        {"limits", {{"pending_nodes", "256"}, {"descriptor_bytes", "1048576"}, {"callbacks", "100"},
        {"events", "200000"}, {"same_time_events", "100"}, {"no_progress_events", "200000"},
        {"horizon_fs", "1000000000"}, {"supply_frames", "1"}}}};
}
Json status() {
    return {{"ready", true}, {"completions", Json::array()},
        {"domains", Json::array({{{"id", "d"}, {"next_fs", "100"}}})},
        {"endpoints", Json::array({{{"id", "e"}, {"state", "READY"}, {"next_fs", "20"}}})},
        {"completed_nodes", "0"}, {"admitted_nodes", "0"}, {"accepted_prefix", "0"},
        {"native_state", "READY"}, {"business_not_before_fs", "0"}, {"supply_idle", {{"d", true}}}};
}
}

void joint_cx_advance_contract(const std::string& scenario) {
    auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
    auto cfg = config(), state = status();
    std::vector<std::string> commands;
    uint64_t expected_horizon = 100;
    if (scenario == "old") cfg.erase("cx_advance_mode");
    if (scenario == "invalid-mode") cfg["cx_advance_mode"] = "unknown";
    if (scenario == "tie") state["domains"][0]["next_fs"] = "20";
    if (scenario == "bootstrap") state["endpoints"][0]["state"] = "BOOTSTRAP";
    if (scenario == "hold") cfg["hold"] = {{"endpoint", "e"}, {"release_fs", "20"}, {"value", 1}};
    if (scenario == "reserve-events") cfg["limits"]["events"] = "65535";
    if (scenario == "reserve-idle") cfg["limits"]["no_progress_events"] = "65535";
    if (scenario == "count-events") cfg["limits"]["events"] = "65537";
    if (scenario == "count-idle" || scenario == "admission") cfg["limits"]["no_progress_events"] = "65537";
    if (scenario == "completion") cfg["limits"]["no_progress_events"] = "65536";
    if (scenario == "admission" || scenario == "completion") state["admitted_nodes"] = "1";
    if (scenario == "same-first") {
        cfg["limits"]["same_time_events"] = "1";
        state["endpoints"][0]["next_fs"] = "0";
    }
    if (scenario == "global" || scenario == "astra" || scenario == "same-first" || scenario == "drain") {
        state["domains"] = Json::array();
        expected_horizon = scenario == "astra" ? 1000000 : 1000000000;
    }
    if (scenario == "card") {
        state["domains"] = Json::array();
        state["endpoints"].push_back({{"id", "f"}, {"state", "READY"}, {"next_fs", "70"}});
        expected_horizon = 70;
    }
    auto exchange = [&](const std::string& command, const Json& body) {
        commands.push_back(command);
        auto reply = state;
        if (command == "advance_before") {
            require(body.at("engine") == "cx" && body.at("id") == "e" &&
                body.at("horizon_fs") == std::to_string(expected_horizon), "external cut was not preserved");
            const auto first = std::stoull(state["endpoints"][0]["next_fs"].get<std::string>());
            const auto processed = first + 20;
            reply["advanced"] = {{"engine", "cx"}, {"id", "e"}, {"horizon_fs", body["horizon_fs"]},
                {"kernel_fs", std::to_string(scenario == "empty" ? expected_horizon : processed)},
                {"processed_fs", std::to_string(processed)}, {"batch_count", "2"}};
            reply["endpoints"][0]["next_fs"] = std::to_string(scenario == "empty" ? expected_horizon + 20 : processed + 20);
            if (scenario == "astra") reply["endpoints"][0]["next_fs"] = nullptr;
            if (scenario == "completion") reply["completed_nodes"] = "1";
            auto& facts = reply["advanced"];
            if (scenario == "bad-id") facts["id"] = "other";
            if (scenario == "bad-horizon") facts["horizon_fs"] = "99";
            if (scenario == "bad-kernel") facts["kernel_fs"] = "101";
            if (scenario == "bad-processed") facts["processed_fs"] = "100";
            if (scenario == "bad-null") facts["processed_fs"] = nullptr;
            if (scenario == "bad-count") facts["batch_count"] = "0";
            if (scenario == "bad-large") facts["batch_count"] = "65537";
            if (scenario == "bad-next") reply["endpoints"][0]["next_fs"] = "40";
        } else if (command == "step_at") {
            if (body["engine"] == "cx") {
                reply["endpoints"][0]["next_fs"] = nullptr;
                if (scenario == "drain") reply["endpoints"][0]["state"] = "DRAINED";
            } else reply["domains"][0]["next_fs"] = nullptr;
        } else if (command == "drain") {
            reply["native_state"] = "COMPLETE";
            reply["endpoints"][0]["state"] = "DRAINING";
        }
        state = reply;
        state.erase("advanced");
        return reply;
    };
    if (scenario == "invalid-mode") {
        rejects([&] { Owner invalid(queue, cfg, exchange); }, "mode");
        require(commands.empty(), "invalid mode contacted runtime");
        return;
    }
    auto owner = std::make_shared<Owner>(queue, cfg, exchange);
    if (scenario == "astra") owner->deadline(0, 1, 0);
    if (scenario == "drain") {
        owner->finish();
        require(commands == std::vector<std::string>{"open", "drain", "step_at", "close"}, "drain used BEFORE");
        return;
    }
    if (scenario == "same-first") {
        owner->enqueue_receive(0, [](auto) {});
        owner->advance();
        rejects([&] { owner->advance(); }, "event budget");
        require(commands == std::vector<std::string>{"open"}, "same-time budget checked after CX execution");
        return;
    }
    if (scenario.rfind("bad-", 0) == 0) {
        rejects([&] { owner->advance(); });
        require(owner->frontier_fs() == 0 && commands.size() == 2, "invalid reply committed time or retried");
        rejects([&] { owner->advance(); });
        require(commands.size() == 2, "failed BEFORE was retried");
        return;
    }
    owner->advance();
    const bool fallback = scenario == "old" || scenario == "tie" || scenario == "hold" ||
        scenario == "bootstrap" || scenario == "reserve-events" || scenario == "reserve-idle";
    require(commands.back() == (fallback ? "step_at" : "advance_before"), "incorrect STEP/BEFORE selection");
    require(owner->frontier_fs() == (fallback ? 20 : scenario == "empty" ? expected_horizon : 40), "actual kernel cut lost");
    if (scenario == "count-events" || scenario == "count-idle" || scenario == "admission" || scenario == "completion") {
        owner->advance();
        const bool renewed = scenario == "admission" || scenario == "completion";
        require(commands.back() == (renewed ? "advance_before" : "step_at"), "actual batch or progress budget differs");
    }
    if (scenario == "astra") {
        bool received = false;
        owner->enqueue_receive(40, [&](auto edge) { received = edge.visible_ns == 1; });
        owner->advance();
        owner->advance();
        require(received && queue->get_current_time() == 1, "external callback was skipped by BEFORE");
    }
}

void joint_cx_advance_workload(const std::filesystem::path& root) {
    ExternalMemoryTests::Fixture fixture(root, {256, 1048576, 4096}, false);
    auto cfg = config(), state = status();
    cfg["run_id"] = fixture.context->identity().at("run_id");
    cfg["session_id"] = "fixture-session";
    state["source_context"] = fixture.context->identity();
    state["domains"] = Json::array();
    state["endpoints"][0]["next_fs"] = nullptr;
    Json staged = Json::array();
    unsigned before_calls = 0, received = 0;
    auto exchange = [&](const std::string& command, const Json& body) {
        auto reply = state;
        if (command == "admit_nodes") {
            for (const auto& row : body.at("nodes")) staged.push_back(row);
            reply["accepted_prefix"] = std::to_string(body.at("nodes").size());
            reply["admitted_nodes"] = std::to_string(std::stoull(state["admitted_nodes"].get<std::string>()) + body.at("nodes").size());
            const auto proposed = fixture.events->get_current_time() * 1000000 + 1;
            const auto& old = state["endpoints"][0]["next_fs"];
            reply["endpoints"][0]["next_fs"] = std::to_string(old.is_null() ? proposed :
                std::min(proposed, static_cast<uint64_t>(std::stoull(old.get<std::string>()))));
        }
        if (command == "advance_before" || (command == "step_at" && body["engine"] == "cx")) {
            const auto first = std::stoull(state["endpoints"][0]["next_fs"].get<std::string>());
            const auto when = first + (command == "advance_before" ? 1 : 0);
            if (command == "advance_before") {
                ++before_calls;
                reply["advanced"] = {{"engine", "cx"}, {"id", "e"}, {"horizon_fs", body["horizon_fs"]},
                    {"kernel_fs", std::to_string(when)}, {"processed_fs", std::to_string(when)}, {"batch_count", "2"}};
            }
            for (const auto& row : staged) reply["completions"].push_back({{"rank", row["rank"]},
                {"iteration", row["iteration"]}, {"node", row["node"]}, {"completed_fs", std::to_string(when)}});
            reply["completed_nodes"] = reply["admitted_nodes"];
            reply["endpoints"][0]["next_fs"] = nullptr;
            staged.clear();
        }
        state = reply;
        state.erase("advanced");
        state["completions"] = Json::array();
        return reply;
    };
    auto owner = std::make_shared<Owner>(fixture.events, cfg, exchange);
    owner->bind(fixture.context, {&fixture.work(0), &fixture.work(1)});
    owner->observe_receives([&](const auto&, uint64_t fs, auto edge, uint64_t actual) {
        require(actual == edge.visible_ns && actual >= (fs + 999999) / 1000000, "BEFORE completion was consumed before its cut");
        ++received;
    });
    fixture.fire();
    for (unsigned step = 0; !fixture.work(0).is_finished || !fixture.work(1).is_finished; ++step) {
        require(step < 100, "BEFORE Workload failed to retire");
        owner->advance();
    }
    require(before_calls > 0 && received == 4 && owner->pending_nodes() == 0 &&
        fixture.work(0).is_finished && fixture.work(1).is_finished, "BEFORE did not deliver all original node callbacks");
    owner->abort();
}
