// Real Workloads, ETs and queue; only whole-memory-node service is a test peer.
#include "common/JointSimulationCoordinator.hh"
#include "ExternalMemoryFixture.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <fstream>
#include <map>

namespace {
using Coordinator = AstraSimAnalytical::JointSimulationCoordinator;
using Json = Coordinator::Json;
using ExternalMemoryTests::require;
using ExternalMemoryTests::rejects;
Json save(const std::filesystem::path& path, const Json& value) {
    const auto raw = value.dump();
    std::ofstream out(path); out << raw; out.close();
    require(bool(out), "successor fixture write failed");
    return {{"path", path.string()}, {"size_bytes", std::to_string(raw.size())},
            {"sha256", AstraSim::ServiceBindingJson::digest_bytes(raw).substr(7)}};
}

struct Run {
    ExternalMemoryTests::Fixture fixture;
    Json status, input;
    std::shared_ptr<Coordinator> owner;
    unsigned activations = 0, successors = 0;
    Json progress = Json::array();
    AstraSimAnalytical::JointProgress::Time host_time{};
    std::string scenario;
    explicit Run(const std::filesystem::path& root, std::string test)
        : fixture(root, {256, 1048576, 4096}, false), scenario(std::move(test)) {
        std::ifstream file(root / "successor-input.json"); file >> input;
        Json config{{"run_id", fixture.context->identity().at("run_id")}, {"session_id", "successor-test"},
            {"activation", {{"schema", "simulator-first-activation-v1"}, {"manifest_path", (root/"activation.json").string()}}},
            {"successor", {{"schema", "simulator-successive-views-v1"}, {"manifest_root", root.string()}}},
            {"limits", {{"pending_nodes", "256"}, {"descriptor_bytes", "1048576"}, {"callbacks", "100"},
                {"events", "10000"}, {"same_time_events", "1000"}, {"no_progress_events", "1000"},
                {"horizon_fs", "1000000000000"}, {"supply_frames", "1"}}}};
        status = {{"ready", false}, {"completions", Json::array()}, {"domains", Json::array()},
            {"endpoints", Json::array()}, {"admitted_nodes", "0"}, {"completed_nodes", "0"},
            {"business_not_before_fs", "0"}, {"supply_idle", Json::object()},
            {"source_context", fixture.context->identity()}};
        owner = std::make_shared<Coordinator>(fixture.events, config,
            [this](const auto& cmd, const auto& body) { return exchange(cmd, body); });
        owner->bind(fixture.context, {&fixture.work(0), &fixture.work(1)});
        if (scenario == "progress") owner->observe_progress(4, [this](const auto& line) {
            progress.push_back(AstraSim::ServiceBindingJson::parse(line.substr(18)));
            std::cout << line << std::endl;
        }, [this] { return host_time; });
    }
    Json exchange(const std::string& command, const Json& body) {
        status["completions"] = Json::array(); status["accepted_prefix"] = "0";
        if (command == "activate") {
            ++activations; status["ready"] = true;
            status["business_not_before_fs"] = body.at("epoch_fs");
            status["view_identity"] = {{"session_id", "successor-test"},
                {"manifest_sha256", body["manifest"]["sha256"]}, {"view_sequence", "0"}, {"execution_iteration", "2"}};
        } else if (command == "admit_nodes") {
            for (const auto& row : body.at("nodes")) {
                status["completions"].push_back({{"rank", row["rank"]}, {"iteration", row["iteration"]},
                    {"node", row["node"]}, {"completed_fs", row["release_fs"]}});
            }
            status["accepted_prefix"] = std::to_string(body.at("nodes").size());
            const auto total = std::stoull(status["admitted_nodes"].get<std::string>()) + body.at("nodes").size();
            status["admitted_nodes"] = status["completed_nodes"] = std::to_string(total);
        } else if (command == "successor") {
            ++successors;
            const auto value = AstraSim::ServiceBindingJson::parse(
                AstraSimAnalytical::JointRuntimeClient::original(body.at("manifest"), 1U << 20, true));
            const auto& facts = value.at("successor");
            status["successor_capability"] = "simulator-successive-views-v1";
            status["view_identity"] = {{"session_id", "successor-test"}, {"manifest_sha256", body["manifest"]["sha256"]},
                {"view_sequence", facts.at("view_sequence")}, {"execution_iteration", facts.at("execution_iteration")}};
            status["view_ready"] = scenario != "no-event";
            if (scenario == "stale-ready") status["view_identity"]["view_sequence"] = "0";
            status["business_not_before_fs"] = body.at("cut_fs");
        }
        const auto total = std::stoull(status["completed_nodes"].get<std::string>());
        status["bridge"] = {{"issued", total}, {"completed", total}, {"failed", false}};
        return status;
    }
    Json finish() {
        std::map<unsigned, uint64_t> times;
        for (unsigned steps = 0; times.size() != 2; ++steps) {
            require(steps < 1000, "actual Workloads failed to finish");
            owner->advance();
            for (unsigned rank = 0; rank < 2; ++rank)
                if (fixture.work(rank).is_finished && !times.count(rank)) times[rank] = fixture.events->get_current_time();
        }
        Json acks = Json::array();
        for (const auto& [rank, time] : times) {
            acks.push_back({{"rank", std::to_string(rank)}, {"iteration", std::to_string(fixture.work(rank).iteration)},
                {"finish_ns", std::to_string(time)}});
        }
        return acks;
    }
    void load(const Json& graph, bool checked = true) {
        const auto prefix = graph.at("workload_prefix").get<std::string>();
        if (checked) owner->check_workload(prefix, {&fixture.work(0), &fixture.work(1)});
        if (scenario == "changed-cohort-file") {
            // Exercise the existing managed-rank loader, including its controller rank.
            fixture.work(0).add_workload(prefix, {fixture.systems.at(1).get()});
        } else {
            for (unsigned rank = 0; rank < 2; ++rank) fixture.work(rank).add_workload(prefix, {});
        }
    }
};
}

void joint_successor_contract(const std::filesystem::path& root, const std::string& scenario) {
    Run run(root, scenario);
    run.fixture.fire(); run.finish();
    run.load(run.input.at("forward1"), false);
    const auto forward = run.finish();
    auto first = run.input.at("business2");
    auto facts = run.fixture.context->identity();
    facts.update(Json{{"prepared_at_ns", std::to_string(run.fixture.events->get_current_time())},
        {"model_sequence", "0"}, {"model_rounds_before", "0"}, {"execution_iteration", "2"},
        {"control", {{"kind", "forward"}, {"iteration", "1"}, {"acks", forward}}}});
    first["first_business"] = facts;
    const auto initial_ref = save(root/"activation.json", first);
    const auto first_ready = run.owner->activate(initial_ref);
    if (scenario == "progress") std::cout << "JOINT_ACTIVATION_READY_V1 " << first_ready.dump() << std::endl;
    require(run.progress.empty(), "activation emitted progress before business");
    run.load(first);
    const auto model = run.finish();
    const auto retirement = run.fixture.events->get_current_time();
    const auto prefix = run.input["forward3"]["workload_prefix"].get<std::string>();
    const auto charge = run.fixture.context->live_charge_bytes();
    if (scenario == "control-business") {
        rejects([&] { run.owner->check_workload(run.input["business3"]["workload_prefix"], {}); }, "non-memory");
    } else if (scenario == "late-control-file") {
        const auto path = run.input["forward3"]["graphs"][1]["et"]["path"].get<std::string>();
        std::ifstream in(path, std::ios::binary); std::string original((std::istreambuf_iterator<char>(in)), {});
        { std::ofstream bad(path); bad << "broken"; }
        rejects([&] { run.owner->check_workload(prefix, {&run.fixture.work(0), &run.fixture.work(1)}); });
        { std::ofstream restored(path, std::ios::binary); restored << original; }
    }
    require(run.fixture.context->live_charge_bytes() == charge, "temporary invalid control indexes leaked charge");
    run.owner->check_workload(prefix, {});
    require(run.fixture.context->live_charge_bytes() == charge, "control preflight indexes were retained");
    if (scenario == "partial-cohort") {
        run.owner->check_workload(prefix, {&run.fixture.work(0)});
        rejects([&] { run.owner->check_workload(run.input["business4"]["workload_prefix"], {}); }, "retains ranks");
        run.owner->check_workload(prefix, {&run.fixture.work(1)});
        run.load(run.input.at("forward3"), false);
    } else {
        if (scenario == "changed-cohort-file") {
            const auto path = run.input["forward3"]["graphs"][1]["companion"]["path"].get<std::string>();
            std::ifstream in(path); std::string original((std::istreambuf_iterator<char>(in)), {});
            { std::ofstream bad(path); bad << "{}"; }
            rejects([&] { run.owner->check_workload(prefix, {&run.fixture.work(0), &run.fixture.work(1)}); });
            { std::ofstream restored(path); restored << original; }
        }
        run.load(run.input.at("forward3"));
    }
    const auto controls = run.finish();
    auto next = run.input.at("business4"); next["schema"] = "simulator-joint-successor-v1";
    Json predecessor{{"session_id", "successor-test"}, {"manifest_sha256", initial_ref.at("sha256")},
        {"view_sequence", "0"}, {"execution_iteration", "2"}, {"acks", model},
        {"resources_retired", true}, {"observed_at_ns", std::to_string(retirement)}};
    auto next_facts = facts;
    next_facts.erase("mode"); next_facts.erase("limits"); next_facts.erase("source_profile");
    next_facts.update(Json{{"schema", "simulator-successor-facts-v1"}, {"session_id", "successor-test"},
        {"view_sequence", "1"}, {"execution_iteration", "4"}, {"model_sequence", "1"}, {"model_rounds_before", "1"},
        {"prepared_at_ns", std::to_string(run.fixture.events->get_current_time())}, {"predecessor", predecessor},
        {"control", {{"kind", "forward"}, {"iteration", "3"}, {"acks", controls}}},
        {"preparations", Json::array()}, {"source_inventory_sha256", std::string(64, 'a')}});
    if (scenario == "old-model-ack") next_facts["predecessor"]["acks"][1]["iteration"] = "1";
    if (scenario == "old-manifest") next_facts["predecessor"]["manifest_sha256"] = std::string(64, 'f');
    next["successor"] = next_facts;
    const auto ref = save(root/"view-1.json", next);
    if (scenario == "old-model-ack" || scenario == "old-manifest" || scenario == "stale-ready" || scenario == "no-event") {
        rejects([&] { run.owner->successor(ref); });
        require(run.fixture.work(0).iteration == 3 && run.fixture.work(1).iteration == 3,
                "invalid successor installed a later Workload");
    } else {
        const auto ready = run.owner->successor(ref);
        if (scenario == "progress") std::cout << "JOINT_SUCCESSOR_READY_V1 " << ready.dump() << std::endl;
        require(ready.at("view_sequence") == "1" && run.activations == 1 && run.successors == 1,
                "successor restarted the original session");
        if (scenario == "progress") {
            require(!run.progress.empty() && run.progress.back().at("view_sequence") == "0" &&
                run.progress.back().at("completed_nodes") == "4",
                "handoff failed to flush genuine old-view completion");
            run.host_time += std::chrono::seconds(1);
        }
        run.load(next); run.finish();
        if (scenario == "progress") require(run.progress.back().at("view_sequence") == "1" &&
            std::stoull(run.progress.back().at("completed_nodes").get<std::string>()) > 4,
            "successor did not report strictly newer actual completions");
        for (unsigned rank = 0; rank < 2; ++rank)
            require(run.fixture.work(rank).iteration == 4 && run.fixture.work(rank).rank_completion_count == 5,
                    "successor did not finish through original Workload ACKs");
        require(run.owner->status().at("admitted_nodes") == "8" && run.owner->status().at("completed_nodes") == "8",
                "successor reset cumulative whole-node counts");
        rejects([&] { run.owner->successor(ref); });
    }
    std::cout << Json{{"scope", "original-workload-contract-peer"}, {"scenario", scenario},
        {"activations", run.activations}, {"successors", run.successors},
        {"rank_acks", {run.fixture.work(0).rank_completion_count, run.fixture.work(1).rank_completion_count}},
        {"admitted_nodes", run.owner->status().at("admitted_nodes")},
        {"completed_nodes", run.owner->status().at("completed_nodes")},
        {"astra_ns", std::to_string(run.fixture.events->get_current_time())}}.dump() << std::endl;
    run.owner->abort();
}
