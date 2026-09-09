/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include <filesystem>
#include <iostream>
#include <memory>
#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/PdKvTransferExecutor.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "common/PdNetworkProjection.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>
#include "protoio.hh"
#include "NativeTagEvents.hh"

namespace {
using namespace AstraSim;
using Json = nlohmann::json;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;

struct CancelEvent : Callable {
    PdKvTransferExecutor& executor;
    std::string transfer;
    CancelEvent(PdKvTransferExecutor& owner, std::string id)
        : executor(owner), transfer(std::move(id)) {}
    void call(EventType, CallData*) override { executor.cancel_path(transfer); }
};

void run_path_collision(const std::vector<Sys*>& systems, PhysicalServiceFactory& services,
        const Json& network, const std::shared_ptr<NetworkAnalytical::EventQueue>& events,
        const std::string& path, uint64_t tick) {
    NativeTagRegistry tags;
    for (auto* sys : systems) sys->bind_native_tags(&tags);
    tags.acquire(0, 1, 8, 0);
    PdPathExecutor executor(systems, services, tags, network);
    NativeTagEventsTest::ConflictingEvent conflict(systems[0]);
    // The conflicting original Sys event precedes the sink callback at the same tick.
    systems[1]->register_event(&conflict, EventType::General, nullptr, tick);
    executor.submit(path, 0);
    while (!events->finished()) {
        events->proceed();
        tags.rethrow_failure();
        executor.rethrow_failure();
    }
    throw std::runtime_error("same-tick conflict did not fail closed");
}

void empty_et(const std::string& prefix, const std::string& manifest,
              const ServiceBindingIdentity& services, uint32_t rank) {
    ChakraProtoMsg::GlobalMetadata metadata;
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>>{
            {"tier_manifest_digest", manifest}, {"service_binding_digest", services.binding_digest},
            {"service_activation_id", services.activation_id}}) {
        auto* attr = metadata.add_attr();
        attr->set_name(name);
        attr->set_string_val(value);
    }
    auto* attr = metadata.add_attr();
    attr->set_name("service_rank");
    attr->set_uint64_val(rank);
    ProtoOutputStream stream(prefix + "." + std::to_string(rank) + ".et");
    stream.write(metadata);
}
}

int main(int argc, char** argv) {
    try {
        if (argc != 3 && argc != 4) throw std::invalid_argument("expected fixture directory/cancel count/optional tag case");
        LoggerFactory::init("empty");
        const std::filesystem::path root(argv[1]);
        const int cancel_after = std::stoi(argv[2]);
        const auto network_parser = NetworkAnalytical::NetworkParser((root / "network.yml").string());
        auto events = std::make_shared<NetworkAnalytical::EventQueue>();
        const auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(network_parser);
        Network::set_event_queue(events);
        Network::set_topology(topology);
        const auto memory = load_memory_tier_config((root / "manifest.json").string());
        const auto ranks = topology->get_npus_count();
        PhysicalServiceFactory services(memory, (root / "services.json").string(), ranks,
                                         (root / "interfaces.json").string());
        std::vector<std::unique_ptr<Network>> networks;
        std::vector<std::unique_ptr<Sys>> owned;
        std::vector<Sys*> systems;
        for (int rank = 0; rank < ranks; ++rank) {
            const auto prefix = (root / "empty").string();
            empty_et(prefix, memory.manifest_digest, services.identity(), rank);
            networks.push_back(std::make_unique<Network>(rank));
            owned.push_back(std::make_unique<Sys>(rank, prefix, "empty", (root / "system.json").string(),
                services.at(rank).memory, memory.manifest_digest, networks.back().get(),
                topology->get_npus_count_per_dim(), std::vector<int>{1}, 1.0, 1.0,
                std::filesystem::exists(root / "rendezvous"), services.at(rank).ucie, services.at(rank).movement));
            systems.push_back(owned.back().get());
        }
        const std::string mode = argc == 4 ? argv[3] : "";
        const std::string cancel_prefix = "cancel_at_";
        if (argc == 4 && mode.rfind(cancel_prefix, 0) != 0) {
            const std::string mode(argv[3]);
            const std::string prefix = "collision_at_";
            if (mode.rfind(prefix, 0) == 0) {
                run_path_collision(systems, services,
                    pd_network_projection(network_parser), events,
                    (root / "work.json").string(), std::stoull(mode.substr(prefix.size())));
            }
            NativeTagEventsTest::run(argv[3], systems, events);
            std::cout << "PASS native-tag-events" << std::endl;
            return 0;
        }
        PdKvTransferExecutor executor(systems, &services, pd_network_projection(network_parser));
        executor.submit((root / "work.json").string(), 0);
        std::unique_ptr<CancelEvent> cancellation;
        if (mode.rfind(cancel_prefix, 0) == 0) {
            const auto wire = ServiceBindingJson::read((root / "work.json").string());
            const auto descriptor = ServiceBindingJson::parse(wire.at("descriptor_json").get<std::string>());
            cancellation = std::make_unique<CancelEvent>(executor, descriptor.at("transfer_id"));
            systems[0]->register_event(cancellation.get(), EventType::General, nullptr,
                                       std::stoull(mode.substr(cancel_prefix.size())));
        }
        for (int steps = 0; !events->finished(); ++steps) {
            if (steps > 10000) throw std::runtime_error("P/D path events did not drain");
            if (steps == cancel_after) {
                const auto wire = ServiceBindingJson::read((root / "work.json").string());
                const auto descriptor = ServiceBindingJson::parse(wire.at("descriptor_json").get<std::string>());
                executor.cancel_path(descriptor.at("transfer_id").get<std::string>());
            }
            events->proceed();
            services.rethrow_failure();
            executor.rethrow_failure();
        }
        if (!executor.drained()) throw std::runtime_error("P/D path remains pending without events");
        std::cout << "PASS pd-path-events" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
