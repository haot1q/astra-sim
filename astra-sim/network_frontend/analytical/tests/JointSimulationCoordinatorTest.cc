#include "common/JointSimulationCoordinator.hh"
#include "ExternalMemoryFixture.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <fstream>
#include <iomanip>
#include <map>
#include <openssl/sha.h>
#include <sstream>
#include <set>
#include <sys/prctl.h>
#include <unistd.h>

using Coordinator = AstraSimAnalytical::JointSimulationCoordinator;
using Json = Coordinator::Json;
namespace AstraSimAnalytical {
struct JointCoordinatorTestAccess {
    static Json start(const std::shared_ptr<JointSimulationCoordinator>& owner, const Json& ref,
                      const Json& facts, uint64_t epoch) {
        return owner->start_service(ref, facts, epoch);
    }
};
}
namespace {
struct ExpectedBoundedStop {};
void observe_prefix(const std::shared_ptr<Coordinator>& owner, const std::vector<AstraSim::Sys*>& systems,
                    const std::shared_ptr<NetworkAnalytical::EventQueue>& events) {
    static bool bound = false;
    static Json receives = Json::array(), observed = Json::array();
    static std::set<Coordinator::Service::NodeKey> seen;
    static size_t max_inflight = 0;
    if (!bound) {
        owner->observe_receives([](const auto& id, uint64_t completed, auto edge, uint64_t actual) {
            ExternalMemoryTests::require(receives.size() < 8, "finite prefix receipt limit");
            receives.push_back({{"rank", id.rank}, {"node", std::to_string(id.node)},
                {"completed_fs", std::to_string(completed)}, {"visible_ns", std::to_string(edge.visible_ns)},
                {"actual_ns", std::to_string(actual)}});
        });
        bound = true;
    }
    std::set<uint32_t> ranks;
    Json promises = Json::array();
    max_inflight = std::max(max_inflight, owner->pending_nodes());
    for (const auto& [key, request] : owner->promises()) {
        Json row{{"rank", request.identity.rank}, {"node", std::to_string(request.identity.node)},
            {"submitted_ns", std::to_string(request.submitted_ns)}};
        promises.push_back(row);
        if (request.identity.iteration == 1 && (request.identity.node == 3 || request.identity.node == 5) && seen.insert(key).second)
            observed.push_back(row);
    }
    for (const auto& key : seen) if (std::get<2>(key) == 5) ranks.insert(std::get<0>(key));
    if (ranks != std::set<uint32_t>{0, 1}) return;
    ExternalMemoryTests::require(max_inflight >= 2 && receives.size() >= 2, "prefix did not observe multiple promises and actual callbacks");
    Json unfinished = Json::array();
    for (const auto* system : systems) {
        ExternalMemoryTests::require(!system->workload->is_finished, "prefix falsely completed a Workload");
        unfinished.push_back(system->id);
    }
    const auto* path = std::getenv("JOINT_TEST_RECEIPT");
    ExternalMemoryTests::require(path != nullptr, "test receipt path missing");
    std::ofstream output(path);
    output << Json{{"status", "EXPECTED_BOUNDED_STOP"}, {"source_complete", false},
        {"unfinished_ranks", unfinished}, {"pending_nodes", promises}, {"runtime", owner->status()},
        {"observed_promises", observed}, {"receives", receives}, {"max_inflight", max_inflight},
        {"astra_ns", std::to_string(events->get_current_time())}}.dump() << std::endl;
    ExternalMemoryTests::require(bool(output), "test receipt write failed");
    throw ExpectedBoundedStop{};
}
}

// Reuse the actual frontend assembly and Controller protocol, with no product stop selector.
#define JOINT_RUNTIME_TEST_OBSERVE observe_prefix
#define main joint_original_frontend_main
#include "../congestion_unaware/main.cc"
#undef main
#undef JOINT_RUNTIME_TEST_OBSERVE

namespace {
using ExternalMemoryTests::require;
using ExternalMemoryTests::rejects;
Json configuration() {
    return Json{{"run_id", "unit"}, {"session_id", "unit"}, {"limits", {
        {"pending_nodes", "256"}, {"descriptor_bytes", "1048576"}, {"callbacks", "100"},
        {"events", "1000"}, {"same_time_events", "100"}, {"no_progress_events", "1000"},
        {"horizon_fs", "1000000000"}, {"supply_frames", "1"}}}};
}
Json empty_status() {
    return Json{{"ready", false}, {"completions", Json::array()}, {"domains", Json::array()},
        {"endpoints", Json::array()}, {"completed_nodes", "0"}, {"admitted_nodes", "0"},
        {"accepted_prefix", "0"}, {"native_state", "NEW"}, {"business_not_before_fs", "0"}, {"supply_idle", Json::object()}};
}

void activation_contract() {
    auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
    auto config = configuration();
    config["activation"] = {{"schema", "simulator-first-activation-v1"}, {"manifest_path", "/unpublished"}};
    config["limits"]["no_progress_events"] = "1";
    auto status = empty_status();
    std::vector<std::string> calls;
    auto owner = std::make_shared<Coordinator>(queue, config, [&](const auto& command, const auto& body) {
        calls.push_back(command);
        if (command == "activate") {
            require(body.at("epoch_fs") == "100000000", "epoch differs from original queue cut");
            status["endpoints"] = Json::array({{{"id", "e"}, {"state", "BOOTSTRAP"}, {"next_fs", "107000000"}}});
        } else if (command == "step_at") {
            require(body.at("time_fs") == "107000000", "global READY lost local initialization cost");
            status["ready"] = true;
            status["business_not_before_fs"] = "107000000";
            status["endpoints"][0]["next_fs"] = nullptr;
        }
        return status;
    });
    require(calls.empty(), "awaiting constructor opened a lower service");
    rejects([&] { owner->try_submit(Coordinator::Service::NodeRequest{}); }, "invalid or repeated");
    rejects([&] { owner->activate(Json::object()); }, "activation requires");
    for (auto when : {1, 2, 100}) owner->deadline(0, when, when);
    for (int i = 0; i < 3; ++i) owner->advance();
    require(queue->get_current_time() == 100 && calls.empty(), "control phase advanced a lower clock");
    const auto ready = AstraSimAnalytical::JointCoordinatorTestAccess::start(owner,
        {{"sha256", std::string(64, 'a')}}, {{"execution_iteration", "5"}}, 100000000);
    require(ready.at("anchor_ns") == "107" && queue->get_current_time() == 107 &&
            ready.at("business_not_before_fs") == "107000000" && calls == std::vector<std::string>{"activate", "step_at"},
            "activation marker did not execute on the original receive queue");
    rejects([&] { owner->activate(Json::object()); }, "activation requires");
    owner->abort();
}

Json manifest_ref(const std::filesystem::path& path, const Json& value) {
    const auto raw = value.dump();
    std::ofstream output(path);
    output << raw;
    output.close();
    require(bool(output), "activation test manifest write failed");
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(raw.data()), raw.size(), digest);
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (auto byte : digest) hex << std::setw(2) << static_cast<unsigned>(byte);
    return {{"path", path.string()}, {"size_bytes", std::to_string(raw.size())}, {"sha256", hex.str()}};
}

void bound_activation(const std::filesystem::path& root, const std::string& scenario) {
    ExternalMemoryTests::Fixture fixture(root, {256, 1048576, 4096}, false);
    auto config = configuration();
    config["run_id"] = fixture.context->identity().at("run_id");
    config["activation"] = {{"schema", "simulator-first-activation-v1"},
        {"manifest_path", (root / "activation.json").string()}};
    auto status = empty_status();
    status["source_context"] = fixture.context->identity();
    unsigned calls = 0;
    uint64_t ready_fs = 0;
    auto owner = std::make_shared<Coordinator>(fixture.events, config, [&](const auto& command, const auto& body) {
        ++calls;
        if (command == "activate") {
            ready_fs = std::stoull(body.at("epoch_fs").template get<std::string>()) + 7000000;
            status["endpoints"] = Json::array({{{"id", "e"}, {"state", "BOOTSTRAP"},
                {"next_fs", std::to_string(ready_fs)}}});
        } else if (command == "step_at") {
            require(body.at("time_fs") == std::to_string(ready_fs), "activation skipped original READY cost");
            status["ready"] = true;
            status["business_not_before_fs"] = std::to_string(ready_fs);
            status["endpoints"][0]["next_fs"] = nullptr;
        }
        return status;
    });
    owner->bind(fixture.context, {&fixture.work(0), &fixture.work(1)});
    fixture.fire();
    for (unsigned step = 0; !fixture.work(0).is_finished || !fixture.work(1).is_finished; ++step) {
        require(step < 100, "original bootstrap did not drain");
        owner->advance();
    }
    std::ifstream input(root / "activation-input.json");
    Json manifest;
    input >> manifest;
    for (unsigned rank = 0; rank != 2; ++rank) {
        require(fixture.work(rank).rank_completion_count == 1, "bootstrap did not complete exactly once");
        fixture.work(rank).add_workload(manifest.at("control_prefix").get<std::string>(), {});
    }
    manifest.erase("control_prefix");
    Json acks = Json::array();
    std::map<unsigned, uint64_t> finished;
    for (unsigned step = 0; finished.size() != 2; ++step) {
        require(step < 100, "original forward controls did not drain");
        owner->advance();
        for (unsigned rank = 0; rank != 2; ++rank)
            if (fixture.work(rank).rank_completion_count == 2 && !finished.count(rank))
                finished[rank] = fixture.events->get_current_time();
    }
    require(calls == 0, "awaiting control phase contacted lower service");
    for (const auto& [rank, when] : finished)
        acks.push_back({{"rank", std::to_string(rank)}, {"iteration", std::to_string(fixture.work(rank).iteration)},
            {"finish_ns", std::to_string(when)}});
    const auto iteration = fixture.work(0).iteration;
    auto facts = fixture.context->identity();
    facts.update(Json{{"prepared_at_ns", std::to_string(fixture.events->get_current_time())},
        {"model_sequence", "0"}, {"model_rounds_before", "0"}, {"execution_iteration", std::to_string(iteration + 1)},
        {"control", {{"kind", "forward"}, {"iteration", std::to_string(iteration)}, {"acks", acks}}}});
    if (scenario == "missing-ack") facts["control"]["acks"].erase(1);
    if (scenario == "iteration") facts["control"]["acks"][1]["iteration"] = std::to_string(iteration + 1);
    manifest["first_business"] = facts;
    const auto ref = manifest_ref(root / "activation.json", manifest);
    if (scenario == "missing-ack" || scenario == "iteration") {
        rejects([&] { owner->activate(ref); }, scenario == "missing-ack" ? "complete original ranks" : "actual drained rank");
        require(calls == 0, "invalid original ACK reached lower service");
    } else {
        const auto ready = owner->activate(ref);
        require(ready.at("anchor_ns") == std::to_string(fixture.events->get_current_time()) &&
                calls == 2 && ready_fs == fixture.events->get_current_time() * 1000000,
                "public activation did not execute its actual READY callback");
        const auto prefix = manifest.at("workload_prefix").get<std::string>();
        rejects([&] { owner->check_workload(prefix + "-wrong", {}); }, "prefix changed");
        if (scenario == "et" || scenario == "companion") {
            std::ofstream changed(manifest["graphs"][0][scenario]["path"].get<std::string>(), std::ios::app);
            changed << 'x';
            changed.close();
            rejects([&] { owner->check_workload(prefix, {&fixture.work(0)}); }, "original byte limit");
        } else {
            owner->check_workload(prefix, {&fixture.work(0)});
            rejects([&] { owner->check_workload(prefix, {&fixture.work(0)}); }, "later business view");
            owner->check_workload(prefix, {&fixture.work(1)});
        }
        rejects([&] { owner->activate(ref); }, "activation requires");
    }
    owner->abort();
}

void queue_contract(const std::string& scenario) {
    auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
    auto status = empty_status();
    auto config = configuration();
    std::vector<std::string> order;
    size_t supplies = 0;
    auto exchange = [&](const std::string& command, const Json& body) {
        if (command == "supply") {
            require(body.at("arrival_fs") == "0", "quota changed original arrival");
            order.push_back("supply");
            status["supply_reason"] = ++supplies < 3 ? "quota" : "idle";
            status["supply_idle"]["d"] = supplies >= 3;
        }
        if (command == "step_at") {
            order.push_back(body.at("engine"));
            status[body.at("engine") == "mw" ? "domains" : "endpoints"][0]["next_fs"] = nullptr;
            if (scenario == "drain") status["endpoints"][0]["state"] = "DRAINED";
        }
        if (command == "drain") {
            status["native_state"] = "COMPLETE";
            status["endpoints"] = Json::array({{{"id", "e"}, {"next_fs", "0"}, {"state", "DRAINING"}}});
        }
        return status;
    };
    if (scenario == "tie") {
        status["domains"] = Json::array({{{"id", "d"}, {"next_fs", "0"}}});
        status["endpoints"] = Json::array({{{"id", "e"}, {"next_fs", "0"}, {"state", "BOOTSTRAP"}}});
    } else if (scenario == "quota") {
        status["ready"] = true;
        status["domains"] = Json::array({{{"id", "d"}, {"next_fs", "0"}}});
        status["supply_idle"]["d"] = false;
    } else if (scenario == "ceil" || scenario == "closed") {
        status["endpoints"] = Json::array({{{"id", "e"}, {"next_fs", "1"}, {"state", "BOOTSTRAP"}}});
    }
    auto owner = std::make_shared<Coordinator>(queue, config, exchange);
    if (scenario == "tie") {
        owner->enqueue_receive(0, [&](auto) { order.push_back("a0"); });
        owner->enqueue_receive(0, [&](auto) { order.push_back("a1"); });
        for (int i = 0; i != 4; ++i) owner->advance();
        require(order == std::vector<std::string>{"a0", "mw", "cx", "a1"}, "same-time owner order differs");
    } else if (scenario == "quota") {
        owner->advance();
        owner->safe_point();
        owner->safe_point();
        require(order == std::vector<std::string>{"supply", "supply", "supply", "mw"},
                "host quota changed simulated time or idle repeated Source supply");
    } else if (scenario == "ceil" || scenario == "closed") {
        owner->advance();
        owner->enqueue_receive(scenario == "ceil" ? 1 : 0, [&](auto edge) {
            require(edge.visible_ns == 1 && queue->get_current_time() == 1, "receive rounding/closed edge differs");
            order.push_back("receive");
        });
        owner->advance();
        require(order.back() == "receive", "receive callback did not execute");
    } else if (scenario == "failure") {
        owner->enqueue_receive(0, [](auto) { throw std::runtime_error("sticky original callback"); });
        rejects([&] { owner->advance(); }, "sticky original callback");
        rejects([&] { owner->advance(); }, "sticky original callback");
    } else if (scenario == "deadline") {
        owner->deadline(0, 10, 0); owner->deadline(0, 10, 0); owner->deadline(0, 5, 1);
        owner->advance(); require(queue->get_current_time() == 5, "new control wake missed");
        owner->advance(); require(queue->get_current_time() == 10 && queue->finished(), "stale wake not retired");
    } else if (scenario == "drain") {
        owner->finish();
        require(order == std::vector<std::string>{"cx"}, "drain bypassed the original CX event");
    } else {
        rejects([&] { owner->enqueue_receive(1, [](auto) {}); }, "frontier");
        rejects([&] { owner->deadline(0, UINT64_MAX, 0); }, "overflow");
        rejects([&] { owner->advance(); }, "no authorized");
    }
}

void global_capacity(const std::filesystem::path& root) {
    ExternalMemoryTests::Fixture fixture(root, {256, 1048576, 4096}, false);
    auto config = configuration();
    config["run_id"] = fixture.context->identity().at("run_id");
    config["session_id"] = "fixture-session";
    config["limits"]["pending_nodes"] = "2";
    config["limits"]["descriptor_bytes"] = std::to_string(2 * (sizeof(Coordinator::Service::NodeRequest) + 512) - 1);
    auto status = empty_status();
    status["ready"] = true;
    status["source_context"] = fixture.context->identity();
    std::set<unsigned> ranks;
    auto exchange = [&](const std::string& command, const Json& body) {
        auto reply = status;
        if (command == "admit_nodes") {
            reply["accepted_prefix"] = std::to_string(body.at("nodes").size());
            for (const auto& row : body.at("nodes")) {
                ranks.insert(std::stoul(row.at("rank").get<std::string>()));
                reply["completions"].push_back({{"rank", row["rank"]}, {"iteration", row["iteration"]},
                    {"node", row["node"]}, {"completed_fs", std::to_string(fixture.events->get_current_time() * 1000000)}});
            }
        }
        return reply;
    };
    auto owner = std::make_shared<Coordinator>(fixture.events, config, exchange);
    owner->bind(fixture.context, {&fixture.work(0), &fixture.work(1)});
    fixture.fire();
    for (unsigned step = 0; !fixture.work(0).is_finished || !fixture.work(1).is_finished; ++step) {
        require(step < 100, "a globally rejected ingress never woke");
        owner->advance();
    }
    require(ranks == std::set<unsigned>{0, 1}, "cross-ingress capacity wake lost a rank");
    owner->abort();
}

void real_component(const std::string& config_path) {
    using AstraSimAnalytical::JointRuntimeClient;
    auto client = std::make_shared<JointRuntimeClient>(config_path);
    auto queue = std::make_shared<NetworkAnalytical::EventQueue>();
    std::shared_ptr<Coordinator> owner;
    uint64_t outer = 0, observation = 0, offset = 0;
    std::optional<uint64_t> native_frame;
    std::set<std::string> scheduled, completed;
    Json receives = Json::array();
    const std::filesystem::path output = client->config().at("output").get<std::string>();
    const auto journal = output / "test-observations.jsonl";
    const auto read_observations = [&] {
        if (!std::filesystem::exists(journal)) return;
        require(std::filesystem::file_size(journal) <= (1U << 20) && std::filesystem::file_size(journal) >= offset,
                "test journal exceeds finite budget or was truncated");
        std::ifstream stream(journal);
        stream.seekg(offset);
        std::string line;
        while (std::getline(stream, line)) {
            require(!stream.eof(), "test journal contains an incomplete observation frame");
            offset += line.size() + 1;
            const auto row = AstraSim::ServiceBindingJson::parse(line);
            require(row.size() == 5 && JointRuntimeClient::decimal(row.at("observation_seq")) == observation++ &&
                JointRuntimeClient::decimal(row.at("outer_seq")) == outer, "test observation sequence differs");
            const auto command = AstraSim::ServiceBindingJson::parse(row.at("native_command_utf8").get<std::string>());
            const auto reply = AstraSim::ServiceBindingJson::parse(row.at("native_reply_utf8").get<std::string>());
            const auto frame = JointRuntimeClient::decimal(reply.at("frame_seq"));
            require(command.at("frame_seq") == reply.at("frame_seq") && command.at("schema") == reply.at("schema") &&
                reply.at("version") == 1 && command.at("version") == 1 && reply.at("session_serial") == "1" && command.at("session_serial") == "1" &&
                command.at("run_id") == client->config().at("run_id") && reply.at("run_id") == command.at("run_id") &&
                (!native_frame || frame > *native_frame), "actual native observation identity differs");
            native_frame = frame;
            size_t count = 0;
            for (const auto& event : reply.at("events")) {
                if (event.at("kind") != "logical_completed") continue;
                const auto identity = event.at("logical_id").get<std::string>();
                const auto local = JointRuntimeClient::decimal(event.at("time_fs"));
                const auto epoch = JointRuntimeClient::decimal(client->config().value("fixture_epoch_fs", Json("0")));
                const auto when = JointRuntimeClient::decimal(row.at("global_completions").at(identity));
                require(local <= UINT64_MAX - epoch && when == local + epoch,
                        "diagnostic observer differs from the runtime epoch boundary");
                require(owner && event.size() == 4 && client->config().at("binding").contains(event.at("domain_id").get<std::string>()) &&
                        scheduled.size() < 10 && scheduled.insert(identity).second,
                        "unknown or duplicate finite diagnostic completion");
                owner->enqueue_receive(when, [&, identity, when](auto edge) {
                    require(completed.insert(identity).second, "diagnostic callback executed twice");
                    receives.push_back({{"logical_id", identity}, {"completed_fs", std::to_string(when)},
                        {"visible_ns", std::to_string(edge.visible_ns)}, {"actual_ns", std::to_string(queue->get_current_time())}});
                });
                ++count;
            }
            require(count != 0, "journal contained a frame without actual logical completion");
        }
        require(stream.eof(), "test journal read failed");
    };
    owner = std::make_shared<Coordinator>(queue, client->config(), [&](const auto& command, const auto& body) {
        auto reply = client->exchange(command, body);
        read_observations();
        ++outer;
        return reply;
    });
    if (client->config().contains("fixture_epoch_fs")) {
        const auto epoch = JointRuntimeClient::decimal(client->config().at("fixture_epoch_fs"));
        require(epoch && epoch % 1000000 == 0, "diagnostic epoch must be a nonzero original ns edge");
        owner->deadline(0, epoch / 1000000, 0);
        owner->advance();
        require(outer == 0 && queue->get_current_time() == epoch / 1000000,
                "diagnostic lower service started during the original control interval");
        AstraSimAnalytical::JointCoordinatorTestAccess::start(owner,
            {{"sha256", std::string(64, 'd')}}, {{"execution_iteration", "0"}}, epoch);
    }
    while (completed.size() != 10 || !queue->finished()) owner->advance();
    const std::set<std::string> expected{"cold", "hit", "masked", "masked-hit", "evict", "reread",
        "warm-other-set", "lpddr", "slow-miss", "hit-during-miss"};
    require(completed == expected && scheduled == expected, "finite component did not consume its complete original set");
    owner->finish();
    client->close();
    const auto status = owner->status();
    require(status.at("bridge").at("issued") == 7 && status.at("bridge").at("completed") == 7,
            "original normal diagnostic lower census differs");
    std::ofstream receipt(output / "component-receipt.json");
    receipt << Json{{"status", "COMPONENT_COMPLETE"}, {"source_complete", false},
        {"logical_callbacks", receives}, {"runtime", status}}.dump() << std::endl;
    require(bool(receipt), "component receipt write failed");
}
}

int main(int argc, char** argv) {
    if (argc < 3) return 2;
    const std::string mode = argv[1];
    if (mode == "prefix") {
        int original_subreaper = -1;
        require(prctl(PR_GET_CHILD_SUBREAPER, &original_subreaper) == 0, "cannot read original subreaper");
        try { return joint_original_frontend_main(argc - 1, argv + 1); }
        catch (const ExpectedBoundedStop&) {
            std::ifstream children("/proc/self/task/" + std::to_string(getpid()) + "/children");
            std::string child;
            children >> child;
            int current_subreaper = -1;
            require(children.eof() && child.empty() && prctl(PR_GET_CHILD_SUBREAPER, &current_subreaper) == 0 &&
                    current_subreaper == original_subreaper, "bounded stop did not release its owned processes");
            const auto* path = std::getenv("JOINT_TEST_RECEIPT");
            std::ifstream input(path);
            Json receipt;
            input >> receipt;
            input.close();
            receipt["owned_stop_verified"] = true;
            receipt["owned_children"] = 0;
            std::ofstream output(path);
            output << receipt.dump() << std::endl;
            require(bool(output), "bounded stop receipt write failed");
            AstraSim::LoggerFactory::shutdown();
            std::cout << "joint-coordinator EXPECTED_BOUNDED_STOP source_complete=false" << std::endl;
            return 0;
        }
    }
    if (mode == "unit" && std::string(argv[2]) == "activation") activation_contract();
    else if (mode == "unit") queue_contract(argv[2]);
    else if (mode == "component") real_component(argv[2]);
    else if (mode == "activation-bound" && argc == 4) {
        AstraSim::LoggerFactory::init("empty");
        bound_activation(argv[2], argv[3]);
    }
    else if (mode == "capacity") {
        AstraSim::LoggerFactory::init("empty");
        global_capacity(argv[2]);
    }
    else throw std::invalid_argument("unknown coordinator test mode");
    AstraSim::LoggerFactory::shutdown();
    std::cout << "joint-coordinator PASS " << argv[2] << std::endl;
}
