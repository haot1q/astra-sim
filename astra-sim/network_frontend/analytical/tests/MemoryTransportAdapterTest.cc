#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>
#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/MemoryCompletionTiming.hh"
#include "common/PhysicalRouteTransport.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>

namespace {
using namespace AstraSim;
using Json = nlohmann::json;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;

struct Tagged : WorkloadLayerHandlerData { unsigned marker = 595; };

struct Consumer : Callable {
    AstraMemoryAPI& link;
    Json rows = Json::array();
    std::set<CallData*> identities;
    explicit Consumer(AstraMemoryAPI& input) : link(input) {}
    void issue(uint64_t bytes, uint64_t id) {
        auto handler = std::make_unique<Tagged>();
        handler->sys_id = 0;
        handler->node_id = id;
        handler->completion_target = this;
        handler->memory_total_wait_ns = 999;
        handler->memory_total_service_ns = 999;
        link.issue({bytes, MemoryOperation::Read}, handler.get());
        identities.insert(handler.get());
        handler.release();
    }
    void call(EventType type, CallData* data) override {
        (void)type;
        if (identities.erase(data) != 1) throw std::logic_error("handler replaced or repeated callback");
        auto* tagged = static_cast<Tagged*>(data);
        if (tagged->marker != 595 || tagged->device_id != 0) {
            throw std::logic_error("dynamic handler or logical device changed");
        }
        std::unique_ptr<Tagged> owned(tagged);
        const auto [wait, service] = memory_completion_timing(*tagged, Sys::boostedTick(), true);
        rows.push_back({{"id", tagged->node_id}, {"ready", tagged->memory_ready_ns},
            {"start", tagged->memory_start_ns}, {"finish", tagged->memory_finish_ns},
            {"wait", wait}, {"service", service}});
        const bool again = tagged->node_id == 1;
        owned.reset();
        if (again) issue(5, 3);  // The original target can release and immediately resubmit.
    }
};
}

int main(int argc, char** argv) {
    if (argc != 3) throw std::invalid_argument("expected fresh input directory and mode");
    const std::string root = argv[1], mode = argv[2];
    AstraSim::LoggerFactory::init("empty");
    auto events = std::make_shared<NetworkAnalytical::EventQueue>();
    auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(
        NetworkAnalytical::NetworkParser(root + "/network.yml"));
    Network::set_event_queue(events);
    Network::set_topology(topology);
    const auto memory = load_memory_tier_config(root + "/manifest.json");
    PhysicalServiceFactory factory(memory, root + "/binding.json", 2, "",
        root + "/physical/transport.json", root + "/physical/memory-routes.json");
    std::vector<std::unique_ptr<Network>> networks;
    std::vector<std::unique_ptr<Sys>> owned;
    std::vector<Sys*> systems;
    for (int rank = 0; rank < 2; ++rank) {
        networks.push_back(std::make_unique<Network>(rank));
        owned.push_back(std::make_unique<Sys>(rank, root + "/llm", "empty", root + "/system.json",
            factory.at(rank).memory, memory.manifest_digest, networks.back().get(),
            topology->get_npus_count_per_dim(), std::vector<int>{1}, 1, 1, false,
            factory.at(rank).ucie, factory.at(rank).movement));
        systems.push_back(owned.back().get());
    }
    AstraSimAnalytical::PhysicalRouteTransport paths(root + "/physical/routes.json",
        root + "/physical/summary.json", factory, systems, events);
    factory.attach_transport_path_api(paths);
    Consumer consumer(*factory.at(0).ucie.at("ucie").api);
    if (mode == "invalid") {
        try { consumer.issue(0, 1); throw std::runtime_error("zero bytes accepted"); }
        catch (const std::invalid_argument&) {}
        if (!consumer.rows.empty()) throw std::runtime_error("invalid request completed");
        factory.write_memory_transport_summary(root + "/physical/memory-summary.json", true);
        return 0;
    }
    if (mode == "second-hop" || mode == "overflow") {
        paths.submit({0, 0, 1, mode == "overflow" ? uint64_t{1} << 63 : 100, 0}, [](void*) {});
    }
    consumer.issue(mode == "overflow" ? uint64_t{1} << 63 : 30, 1);
    if (mode != "overflow") consumer.issue(20, 2);
    bool failed = false;
    try {
        for (unsigned count = 0; !events->finished(); ++count) {
            if (count > 200) throw std::runtime_error("memory transport did not drain");
            events->proceed();
            paths.rethrow_failure();
        }
    } catch (const std::overflow_error&) {
        if (mode != "overflow") throw;
        failed = true;
    }
    if (mode == "overflow") {
        if (!failed || !consumer.rows.empty()) throw std::runtime_error("overflow produced a completion");
        factory.write_memory_transport_summary(root + "/physical/memory-summary.json", true);
        return 0;
    }
    if (consumer.rows.size() != 3 || !factory.memory_transport_drained()) {
        throw std::runtime_error("missing or repeated completion");
    }
    WorkloadLayerHandlerData endpoint;
    endpoint.memory_ready_ns = endpoint.memory_start_ns = endpoint.memory_finish_ns = Sys::boostedTick();
    endpoint.memory_total_wait_ns = endpoint.memory_total_service_ns = 0;
    try { memory_completion_timing(endpoint, Sys::boostedTick(), false); throw std::runtime_error("endpoint accepted aggregate"); }
    catch (const std::logic_error&) {}
    auto reset = std::make_unique<Tagged>();
    reset->sys_id = 0;
    reset->node_id = 4;
    reset->completion_target = &consumer;
    reset->memory_total_wait_ns = reset->memory_total_service_ns = 999;
    consumer.identities.insert(reset.get());
    factory.at(0).memory.at(0).api->issue({1, MemoryOperation::Read}, reset.get());
    if (reset->memory_total_wait_ns || reset->memory_total_service_ns) {
        throw std::runtime_error("original endpoint retained previous atomic totals");
    }
    reset.release();
    while (!events->finished()) { events->proceed(); paths.rethrow_failure(); }
    std::cout << "MEMORY_ADAPTER_ROWS " << consumer.rows.dump() << std::endl;
    factory.write_memory_transport_summary(root + "/physical/memory-summary.json", false);
    AstraSim::LoggerFactory::shutdown();
}
