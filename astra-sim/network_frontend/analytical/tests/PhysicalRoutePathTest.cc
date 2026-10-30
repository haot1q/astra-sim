/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PhysicalPathFixture.hh"

namespace {
void timing(const TransportPathTiming& actual, const TransportPathTiming& expected) {
    require(actual.first_ready_ns == expected.first_ready_ns &&
            actual.first_start_ns == expected.first_start_ns &&
            actual.finish_ns == expected.finish_ns &&
            actual.total_wait_ns == expected.total_wait_ns &&
            actual.total_service_ns == expected.total_service_ns,
            "actual path timing differs from expected original-service events");
    require(actual.total_wait_ns + actual.total_service_ns ==
            actual.finish_ns - actual.first_ready_ns, "path timing does not conserve time");
}

void paths_and_messages(Fixture& fixture) {
    std::vector<TransportPathTiming> completed;
    const auto save = [&](const auto& result) { completed.push_back(result); };
    fixture.paths->submit_path({1, 40, {"b"}, save, {}});
    fixture.paths->submit_path({0, 10, {"a", "b"}, save, {}});
    fixture.paths->submit_path({1, 10, {"c"}, save, {}});
    fixture.paths->submit({0, 0, 1, 5, 0}, [](void* key) {
        require(std::get<3>(*static_cast<Paths::Key*>(key)) == 5, "message identity changed");
    });
    require(!fixture.paths->drained(), "pending paths were reported drained");
    fixture.drain();
    require(completed.size() == 3 && fixture.paths->drained(), "completion count mismatch");
    timing(completed[0], {0, 0, 10, 0, 10});
    timing(completed[1], {0, 0, 40, 0, 40});
    timing(completed[2], {0, 0, 50, 30, 20});
    fixture.paths->write_summary();
    const auto summary = Wire::read((fixture.root / "summary.json").string());
    require(summary["completed_messages"] == 1 && summary["pending_messages"] == 0 &&
            summary["completed_payload_bytes"] == 5 && summary["resources"].size() == 1 &&
            summary["resources"]["a"]["completed_payload_bytes"] == 5 &&
            summary["resources"]["a"]["queue_wait_ns"] == 10,
            "generic paths contaminated the original message-only summary");
}

void delayed_and_empty(Fixture& fixture) {
    std::vector<TransportPathTiming> completed;
    auto submit = [&] {
        const auto save = [&](const auto& result) { completed.push_back(result); };
        fixture.paths->submit_path({0, 10, {}, save, {}});
        fixture.paths->submit_path({0, 10, {"a"}, save, {}});
        fixture.paths->submit_path({1, 10, {"back"}, save, {}});
    };
    fixture.events->schedule_event(7, [](void* data) {
        (*static_cast<decltype(submit)*>(data))();
    }, &submit);
    fixture.drain();
    require(completed.size() == 3, "empty/delayed callbacks missing");
    timing(completed[0], {7, 7, 7, 0, 0});
    timing(completed[1], {7, 7, 17, 0, 10});
    timing(completed[2], {7, 17, 27, 10, 10});
}

void propagation_paths(Fixture& fixture, bool snapshot = false) {
    std::vector<TransportPathTiming> completed;
    fixture.paths->submit_path({0, 4, {"a", "b"},
        [&](const auto& row) { completed.push_back(row); }, {}});
    fixture.paths->submit({0, 0, 1, 4, 0}, [](void*) {});
    auto pending = [&] {
        require(!fixture.paths->drained() && completed.empty(), "propagating path drained early");
        if (!snapshot) return;
        fixture.paths->write_summary();
        const auto summary = Wire::read((fixture.root / "summary.json").string());
        require(summary["completed_messages"] == 0 && summary["pending_messages"] == 1 &&
                summary["resources"].empty(), "propagation counted as completed bytes");
    };
    fixture.events->schedule_event(9, [](void* data) {
        (*static_cast<decltype(pending)*>(data))();
    }, &pending);
    fixture.drain();
    require(fixture.paths->drained() && completed.size() == 1, "propagation did not drain");
    timing(completed[0], {0, 0, 208, 0, 208});  // Whole-message arrival precedes the second hop.
    if (snapshot) return;  // The original summary owner publishes only once.
    fixture.paths->write_summary();
    const auto summary = Wire::read((fixture.root / "summary.json").string());
    const auto& resource = summary["resources"]["a"];
    require(summary["completed_messages"] == 1 && resource["queue_wait_ns"] == 4 &&
            resource["service_ns"] == 104 && resource["busy_ns"] == 4 &&
            resource["propagation_ns"] == 100 && resource["completed_payload_bytes"] == 4,
            "propagation summary conflated occupancy and arrival");
}

void invalid_paths(Fixture& fixture) {
    unsigned calls = 0;
    const auto save = [&](const auto&) { ++calls; };
    for (const auto& request : std::vector<TransportPathRequest>{
            {2, 1, {"a"}, save, {}}, {0, 0, {"a"}, save, {}},
            {0, 1, {"missing"}, save, {}}, {0, 1, {"a", "back"}, save, {}},
            {0, 1, {"a"}, {}, {}}, {0, UINT64_MAX, {"a", "b"}, save, {}}}) {
        bool rejected = false;
        try { fixture.paths->submit_path(request); }
        catch (const std::invalid_argument&) { rejected = true; }
        catch (const std::overflow_error&) { rejected = true; }
        require(rejected, "invalid path was accepted");
    }
    fixture.drain();
    require(calls == 0 && fixture.paths->drained(), "invalid submission left live work");
}

void failed_callback(Fixture& fixture, bool observer) {
    unsigned completed = 0, observed = 0;
    TransportPathRequest request{0, 10, {"a"}, [&](const auto&) {
        ++completed;
        if (!observer) throw std::runtime_error("deliberate completion failure");
    }, [&](const auto&) {
        ++observed;
        if (observer) throw std::runtime_error("deliberate observer failure");
    }};
    fixture.paths->submit_path(request);
    bool failed = false;
    try { fixture.drain(); } catch (const std::runtime_error&) { failed = true; }
    require(failed && observed == 1 && completed == (observer ? 0u : 1u) &&
            !fixture.paths->drained(), "failed callback was reported as successful drainage");
    const auto summary = Wire::read((fixture.root / "summary.json").string());
    require(summary["status"] == "failed" && summary["completed_messages"] == 0 &&
            summary["pending_messages"] == 0, "generic failure polluted message completion");
}
}  // namespace

int main(int argc, char** argv) {
    try {
        require(argc == 3, "expected golden fixture and test mode");
        LoggerFactory::init("empty");
        const std::string mode = argv[2];
        Fixture fixture(Wire::read(argv[1]), mode.find("propagation") == 0);
        if (mode == "paths") paths_and_messages(fixture);
        else if (mode == "delayed") delayed_and_empty(fixture);
        else if (mode == "propagation") propagation_paths(fixture);
        else if (mode == "propagation-pending") propagation_paths(fixture, true);
        else if (mode == "propagation-failure") failed_callback(fixture, false);
        else if (mode == "invalid") invalid_paths(fixture);
        else if (mode == "observer-failure") failed_callback(fixture, true);
        else if (mode == "completion-failure") failed_callback(fixture, false);
        else throw std::invalid_argument("unknown path test mode");
        std::cout << "PASS physical path " << mode << " evidence=" << fixture.root << std::endl;
    } catch (const std::exception& error) {
        std::cerr << "physical path test failed: " << error.what() << std::endl;
        return 1;
    }
    return 0;
}
