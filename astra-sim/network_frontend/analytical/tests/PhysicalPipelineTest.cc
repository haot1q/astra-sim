/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PhysicalPathFixture.hh"
#include "astra-sim/system/memory/TransportServiceAdapter.hh"

namespace {
std::vector<std::pair<int, uint64_t>> arrivals;
bool throw_arrival = false;

void arrived(void* value) {
    if (throw_arrival) throw std::runtime_error("deliberate message arrival failure");
    const auto& key = *static_cast<Paths::Key*>(value);
    arrivals.emplace_back(std::get<0>(key), Sys::boostedTick());
}

Json limits(uint64_t block = 4) {
    return {{"block_bytes", block}, {"max_blocks_per_message", 64u},
        {"max_block_hops_per_message", 1024u}, {"max_inflight_blocks", 512u},
        {"max_inflight_block_hops", 8192u}};
}

Customize setup(std::string mode) {
    return [mode](Json& transport, Json& routes) {
        routes["timing_profile"] = "message_block_pipeline_v1";
        routes["pipeline"] = limits();
        routes["routes"][0]["service_link_ids"] = {"a", "b"};
        for (auto& row : transport["resources"]) {
            row["bandwidth_bytes_per_second"] = 2'000'000'000ULL;
            row["latency_ns"] = 3u;
        }
        if (mode == "one-hop" || mode == "pending" || mode == "recycle") {
            routes["routes"][0]["service_link_ids"] = {"a"};
        }
        if (mode == "slow-fast" || mode == "fast-slow" || mode == "ceil" || mode == "shared") {
            for (auto& row : transport["resources"]) row["latency_ns"] = 0u;
            transport["resources"][0]["bandwidth_bytes_per_second"] = mode == "fast-slow" ? 4'000'000'000ULL : 1'000'000'000ULL;
            transport["resources"][1]["bandwidth_bytes_per_second"] = mode == "slow-fast" ? 4'000'000'000ULL : 1'000'000'000ULL;
            if (mode == "ceil") for (auto& row : transport["resources"]) row["bandwidth_bytes_per_second"] = 3'000'000'000ULL;
        }
        if (mode == "shared") {
            transport["resources"][0]["bandwidth_bytes_per_second"] = 2'000'000'000ULL;
            transport["resources"][2]["bandwidth_bytes_per_second"] = 1'000'000'000ULL;
            routes["routes"].push_back({{"source_rank", 1u}, {"target_rank", 0u}, {"service_link_ids", {"c", "b"}}});
        }
        if (mode == "one-block") routes["pipeline"]["block_bytes"] = 16u;
        if (mode == "empty") routes["routes"][0]["service_link_ids"] = Json::array();
        if (mode == "pending" || mode == "recycle") {
            routes["pipeline"]["max_inflight_blocks"] = 2u;
            for (auto& row : transport["resources"]) row["latency_ns"] = 100u;
        }
        if (mode == "bounded") {
            transport["resources"] = Json::array();
            transport["links"] = Json::array();
            routes["routes"][0]["service_link_ids"] = Json::array();
            routes["pipeline"] = limits(1);
            for (int i = 0; i < 16; ++i) {
                const auto id = "resource-" + std::to_string(i);
                transport["resources"].push_back({{"id", id}, {"bandwidth_bytes_per_second", 1'000'000'000ULL}, {"latency_ns", 1u}});
                transport["links"].push_back({{"id", id}, {"source", "from"}, {"target", "to"}, {"resource", id}});
                routes["routes"][0]["service_link_ids"].push_back(id);
            }
        }
    };
}

Json summary(Fixture& fixture) {
    fixture.paths->write_summary();
    return Wire::read((fixture.root / "summary.json").string());
}

void run_case(const Json& golden, const std::string& mode) {
    Fixture fixture(golden, true, setup(mode));
    const bool pending = mode == "pending", recycle = mode == "recycle", bounded = mode == "bounded";
    const uint64_t bytes = pending || recycle ? 8 : bounded ? 64 : 10;
    const unsigned messages = bounded ? 8 : 1;
    for (unsigned i = 0; i < messages; ++i) fixture.paths->submit({int(i), 0, 1, bytes, 0}, arrived);
    auto probe = [&] {
        require(arrivals.empty() && !fixture.paths->drained(), "propagation completed early");
        bool rejected = false;
        try { fixture.paths->submit({99, 0, 1, 1, 0}, arrived); }
        catch (const std::invalid_argument&) { rejected = true; }
        require(rejected, "propagation released the active-block budget");
        if (pending) {
            const auto body = summary(fixture);
            require(body["status"] == "incomplete" && body["completed_messages"] == 0 &&
                    body["pending_messages"] == 1 && body["pipeline_counters"]["inflight_blocks"] == 2,
                    "pending summary lost live blocks");
        }
    };
    if (pending || recycle) fixture.events->schedule_event(5, [](void* data) {
        (*static_cast<decltype(probe)*>(data))();
    }, &probe);
    auto second = [&] { fixture.paths->submit({1, 1, 0, 6, 0}, arrived); };
    if (mode == "shared") fixture.events->schedule_event(3, [](void* data) {
        (*static_cast<decltype(second)*>(data))();
    }, &second);
    if (mode == "failure") throw_arrival = true;
    bool failed = false;
    try { fixture.drain(50'000); } catch (const std::runtime_error&) { failed = true; }
    if (mode == "failure") {
        const auto body = summary(fixture);
        require(failed && arrivals.empty() && !fixture.paths->drained() && body["status"] == "failed" &&
                body["completed_messages"] == 0 && body["pending_messages"] == 1 &&
                body["pipeline_counters"]["inflight_blocks"] == 1, "failed last block pretended complete");
        return;
    }
    require(!failed && fixture.paths->drained(), "pipeline did not drain");
    if (recycle) {
        fixture.paths->submit({1, 0, 1, bytes, 0}, arrived);
        fixture.drain();
        require(arrivals == std::vector<std::pair<int, uint64_t>>{{0, 104}, {1, 208}}, "retired budget was not reusable");
    } else if (mode == "shared") {
        require(arrivals == std::vector<std::pair<int, uint64_t>>{{0, 12}, {1, 18}}, "shared-middle FIFO violated actual arrivals");
    } else if (bounded) {
        require(arrivals.size() == 8 && arrivals.back().second == 543, "bounded pipeline did not conserve work");
    } else {
        const uint64_t expected = pending ? 104 : mode == "empty" ? 0 : mode == "one-block" ? 16 :
            mode == "one-hop" ? 8 : mode == "ceil" ? 7 : mode == "slow-fast" || mode == "fast-slow" ? 11 : 13;
        require(arrivals == std::vector<std::pair<int, uint64_t>>{{0, expected}}, "single-flow recurrence mismatch");
    }
    if (pending) return;  // Original summary publishes once; its incomplete snapshot is the evidence.
    const auto body = summary(fixture);
    const auto& counters = body["pipeline_counters"];
    require(body["status"] == "completed" && body["pending_messages"] == 0 &&
            counters["inflight_blocks"] == 0 && counters["reserved_block_hops"] == 0,
            "terminal state retained active budget");
    uint64_t blocks = mode == "empty" || mode == "one-block" ? 1 : recycle ? 4 : mode == "shared" ? 5 : bounded ? 512 : 3;
    uint64_t hops = mode == "empty" ? 0 : recycle ? 4 : mode == "one-hop" ? 3 : bounded ? 8192 : blocks * 2;
    require(counters["submitted_blocks"] == blocks && counters["completed_blocks"] == blocks &&
            counters["completed_block_hops"] == hops && counters["continuation_callbacks"] == blocks + hops,
            "actual owner callbacks differ from bounded block-hop work");
    if (mode == "shared") {
        const auto& resources = body["resources"];
        for (const auto& [id, bytes, busy, wait] : std::vector<std::tuple<std::string, uint64_t, uint64_t, uint64_t>>{
                {"a", 10, 5, 6}, {"b", 16, 16, 19}, {"c", 6, 6, 4}}) {
            const auto& row = resources[id];
            require(row["completed_payload_bytes"] == bytes && row["busy_ns"] == busy &&
                    row["queue_wait_ns"] == wait && row["propagation_ns"] == 0,
                    "automatic blocks changed shared-resource work");
        }
    }
    if (bounded) require(counters["peak_inflight_blocks"] == 512 && counters["peak_reserved_block_hops"] == 8192,
                         "bounded peak context counts differ");
    std::cout << "PIPELINE " << body.dump() << std::endl;
}

void rejected_budgets(const Json& golden, const std::string& mode) {
    auto configure = setup("single");
    Fixture fixture(golden, true, [&](Json& transport, Json& routes) {
        configure(transport, routes);
        auto& budget = routes["pipeline"];
        if (mode == "message-blocks") budget["max_blocks_per_message"] = 2u;
        if (mode == "message-hops") budget["max_block_hops_per_message"] = 5u;
        if (mode == "active-blocks") budget["max_inflight_blocks"] = 2u;
        if (mode == "active-hops") budget["max_inflight_block_hops"] = 5u;
        if (mode == "overflow") {
            for (auto& item : budget.items()) item.value() = UINT64_MAX;
            budget["block_bytes"] = 1u;
        }
    });
    bool rejected = false;
    try { fixture.paths->submit({0, 0, 1, mode == "overflow" ? UINT64_MAX : 10, 0}, arrived); }
    catch (const std::invalid_argument&) { rejected = true; }
    catch (const std::overflow_error&) { rejected = true; }
    const auto body = summary(fixture);
    require(rejected && arrivals.empty() && fixture.paths->drained() &&
            body["submitted_messages"] == 0 && body["pipeline_counters"]["submitted_blocks"] == 0,
            "budget rejection partially published a message");
}
void observed_shared(const Json& golden) {
    Fixture fixture(golden, true, setup("shared"));
    std::vector<std::vector<TransportReceipt>> observed(5);
    std::map<std::string, uint64_t> released, bytes;
    unsigned completed = 0;
    auto submit = [&](size_t index, uint64_t size, std::vector<std::string> links, uint64_t ready) {
        fixture.paths->submit_path({index < 3 ? 0u : 1u, size, std::move(links),
            [&](const auto&) { ++completed; },
            [&, index, size, ready](const TransportReceipt& row) {
                auto& history = observed[index];
                require(row.payload_bytes == size && row.ready_ns == (history.empty() ? ready : history.back().finish_ns),
                        "next hop consumed bytes before actual previous arrival");
                const auto rate = row.resource_id == "a" ? 2u : 1u;
                require(row.start_ns >= row.ready_ns && row.start_ns >= released[row.resource_id] &&
                        row.release_ns - row.start_ns == (size + rate - 1) / rate && row.finish_ns == row.release_ns,
                        "shared service exceeded capacity or per-block ceil");
                released[row.resource_id] = row.release_ns;
                bytes[row.resource_id] += row.payload_bytes;
                history.push_back(row);
            }});
    };
    submit(0, 4, {"a", "b"}, 0);
    submit(1, 4, {"a", "b"}, 0);
    submit(2, 2, {"a", "b"}, 0);
    auto second = [&] { submit(3, 4, {"c", "b"}, 3); submit(4, 2, {"c", "b"}, 3); };
    fixture.events->schedule_event(3, [](void* data) { (*static_cast<decltype(second)*>(data))(); }, &second);
    fixture.drain();
    require(completed == 5 && fixture.paths->drained() && bytes == std::map<std::string, uint64_t>{{"a", 10}, {"b", 16}, {"c", 6}},
            "observed original paths lost blocks or bytes");
    const std::vector<uint64_t> finishes{6, 10, 12, 16, 18};
    for (size_t i = 0; i < observed.size(); ++i) require(observed[i].size() == 2 && observed[i].back().finish_ns == finishes[i],
                                                     "observed shared-block FIFO differs");
    std::cout << "OBSERVED 5 blocks 10 hops; nonoverlap and arrival causality PASS" << std::endl;
}

void rejected_config(const Json& golden, const std::string& mode) {
    bool rejected = false;
    auto configure = setup("single");
    try {
        Fixture fixture(golden, true, [&](Json& transport, Json& routes) {
            configure(transport, routes);
            if (mode == "config-bool") routes["pipeline"]["block_bytes"] = true;
            if (mode == "config-missing") routes["pipeline"].erase("block_bytes");
            if (mode == "config-policy") transport["policy"] = "fifo_serialized_latency_v1";
        });
    } catch (const std::exception&) { rejected = true; }
    require(rejected && arrivals.empty(), "raw pipeline config bypassed contract");
}
}  // namespace

int main(int argc, char** argv) {
    try {
        require(argc == 3, "expected golden fixture and pipeline case");
        LoggerFactory::init("empty");
        const std::string mode = argv[2];
        if (mode == "observed-shared") observed_shared(Wire::read(argv[1]));
        else if (mode.rfind("config-", 0) == 0) rejected_config(Wire::read(argv[1]), mode);
        else if (mode == "message-blocks" || mode == "message-hops" || mode == "active-blocks" ||
            mode == "active-hops" || mode == "overflow") rejected_budgets(Wire::read(argv[1]), mode);
        else run_case(Wire::read(argv[1]), mode);
        std::cout << "PASS pipeline " << mode << std::endl;
    } catch (const std::exception& error) {
        std::cerr << "pipeline test failed: " << error.what() << std::endl;
        return 1;
    }
}
