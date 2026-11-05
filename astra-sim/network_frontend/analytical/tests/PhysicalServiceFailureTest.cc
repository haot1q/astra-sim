/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PhysicalPathFixture.hh"
#include "extern/memory_backend/analytical/AnalyticalMemory.hh"

namespace {
template <class Error, class Function>
void expect_failure(Function function, const std::string& message) {
    try {
        function();
    } catch (const Error& error) {
        require(error.what() == message, "saved failure message changed");
        return;
    }
    throw std::runtime_error("saved failure was not rethrown");
}

void adapter_failure_and_isolation(const Json& golden) {
    Fixture fixture(golden);
    const auto memory = load_memory_tier_config((fixture.root / "memory.json").string());
    PhysicalServiceFactory healthy(memory, (fixture.root / "binding.json").string(), 2);
    fixture.services->rethrow_failure();
    auto* api = fixture.services->at(0).memory.at(0).api;
    WorkloadLayerHandlerData handler;
    handler.sys_id = 0;
    expect_failure<std::invalid_argument>([&] {
        api->issue({1, static_cast<MemoryOperation>(0)}, &handler);
    }, "invalid physical memory operation");
    for (unsigned check = 0; check < 2; ++check) {
        expect_failure<std::invalid_argument>([&] { fixture.services->rethrow_failure(); },
                                              "invalid physical memory operation");
        healthy.rethrow_failure();
    }

    // Independent callers retain local failure storage without a factory target.
    PhysicalServiceAdapter standalone(0, {healthy.at(0).memory.at(0).api});
    const std::string invalid = "invalid rank/device or unsupported native PIM service request";
    expect_failure<std::invalid_argument>([&] { standalone.issue({1, MemoryOperation::Read}, &handler); }, invalid);
    expect_failure<std::invalid_argument>([&] { standalone.rethrow_failure(); }, invalid);
    healthy.rethrow_failure();
}

struct FailedCompletion : Callable {
    unsigned rank;
    std::vector<unsigned>& order;
    FailedCompletion(unsigned rank, std::vector<unsigned>& order) : rank(rank), order(order) {}
    void call(EventType, CallData*) override {
        order.push_back(rank);
        throw std::runtime_error("completion rank " + std::to_string(rank));
    }
};

void transport_scan_priority(const Json& golden) {
    Fixture fixture(golden);
    std::vector<unsigned> order;
    FailedCompletion completion0(0, order), completion1(1, order);
    TransportRequest request0(0, "later-rank-zero", 2, completion0);
    TransportRequest request1(1, "earlier-rank-one", 1, completion1);
    auto& transport1 = fixture.services->transport(1, "c");
    auto& transport0 = fixture.services->transport(0, "a");
    transport1.set_sys(fixture.systems[1]);
    transport0.set_sys(fixture.systems[0]);
    transport1.issue(request1);
    transport0.issue(request0);
    WorkloadLayerHandlerData invalid;
    invalid.sys_id = 0;
    expect_failure<std::invalid_argument>([&] {
        fixture.services->at(0).memory.at(0).api->issue(
            {1, static_cast<MemoryOperation>(0)}, &invalid);
    }, "invalid physical memory operation");
    // Accumulate real callback errors before checking, to distinguish scan order
    // from first-error arrival order. Production checks still run at every step.
    for (unsigned step = 0; !fixture.events->finished(); ++step) {
        require(step < 20, "failure callbacks did not drain");
        fixture.events->proceed();
    }
    require(order == std::vector<unsigned>({1, 0}), "callback failure order changed");
    for (unsigned check = 0; check < 2; ++check) {
        expect_failure<std::runtime_error>([&] { fixture.services->rethrow_failure(); }, "completion rank 0");
        expect_failure<std::runtime_error>([&] { transport1.rethrow_failure(); }, "completion rank 1");
        expect_failure<std::logic_error>([&] { request0.receipt(); }, "transport has no completed receipt");
        expect_failure<std::logic_error>([&] { request1.receipt(); }, "transport has no completed receipt");
    }
}

void standalone_adapters(const Json& golden) {
    Fixture fixture(golden);
    auto* api = fixture.services->at(0).memory.at(0).api;
    const MemoryTransportConfig memory_config;
    MemoryTransportAdapter memory(0, "standalone", *api, memory_config);
    expect_failure<std::invalid_argument>([&] { memory.issue({1, MemoryOperation::Read}, nullptr); },
                                          "invalid memory transport request ownership");
    expect_failure<std::invalid_argument>([&] { memory.rethrow_failure(); },
                                          "invalid memory transport request ownership");
    PhysicalServiceAdapter adapter(0, {api});
    const auto& config = fixture.services->transport_configuration();
    TransportServiceAdapter transport(0, config.links.at("a"), config.resources.at("a"),
                                      adapter, config.policy);
    transport.set_sys(fixture.systems[0]);
    std::vector<unsigned> order;
    FailedCompletion completion(0, order);
    TransportRequest request(0, "standalone", 1, completion);
    transport.issue(request);
    fixture.drain();
    expect_failure<std::runtime_error>([&] { transport.rethrow_failure(); }, "completion rank 0");
    require(order == std::vector<unsigned>({0}), "standalone completion count changed");
    fixture.services->rethrow_failure();
}

void standalone_memory_failures(const Json& golden, bool physical) {
    Fixture fixture(golden);
    Json config = {{"memory-type", "MEMORY_POOL"}, {"memory-location", "REMOTE_MEMORY"},
                   {"mem-bw", 1u}, {"num-devices", 1u}};
    if (physical) config["physical-service-mode"] = "native-single-device-v1";
    const auto path = fixture.root / "standalone-memory.json";
    write_json(path, config);
    Analytical::AnalyticalMemory memory(path.string());
    WorkloadLayerHandlerData first, second;
    first.sys_id = second.sys_id = 0;
    first.device_id = 9;
    if (physical) first.service_device_id = 9;
    else second.service_device_id = 0;
    fixture.systems[0]->register_event(&memory, EventType::General, &first, 1);
    fixture.systems[0]->register_event(&memory, EventType::General, &second, 2);
    fixture.drain();
    if (physical) {
        for (unsigned check = 0; check < 2; ++check) {
            expect_failure<std::out_of_range>([&] { memory.rethrow_failure(); },
                                              "memory device_id 9 is out of range [0,1)");
        }
    } else {
        memory.rethrow_failure();  // Legacy callbacks never retained native failures.
    }
}
}  // namespace

void run_physical_service_failure_tests(const nlohmann::json& golden) {
    adapter_failure_and_isolation(golden);
    transport_scan_priority(golden);
    standalone_adapters(golden);
    standalone_memory_failures(golden, true);
    standalone_memory_failures(golden, false);
    std::cout << "PASS physical service saved-failure contracts" << std::endl;
}
