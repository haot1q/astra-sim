/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#pragma once

#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/memory/ExternalMemoryService.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "astra-sim/workload/Workload.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>

namespace ExternalMemoryTests {
using namespace AstraSim;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;
using Bridge = ExternalMemoryService;

inline void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

template<class Function> void rejects(Function function, const char* diagnostic = nullptr) {
    try { function(); } catch (const std::exception& error) {
        if (diagnostic) require(std::string(error.what()).find(diagnostic) != std::string::npos,
                                "unexpected failure diagnostic");
        return;
    }
    throw std::runtime_error("expected explicit rejection");
}

struct Provider final : Bridge::Provider {
    std::vector<Bridge::NodeRequest> accepted;
    std::size_t attempts = 0;
    std::vector<Bridge::Identity> observed;
    std::vector<Bridge::AdmissionKey> observed_ingress;
    std::function<Bridge::Admission(const Bridge::NodeRequest&)> behavior;

    Bridge::Admission try_submit(const Bridge::NodeRequest& request) override {
        ++attempts;
        observed.push_back(request.identity);
        observed_ingress.push_back(request.admission_key);
        const auto result = behavior ? behavior(request) : Bridge::Admission::Accepted;
        if (result == Bridge::Admission::Accepted) accepted.push_back(request);
        return result;
    }

    Bridge::AdmissionKey ingress(uint32_t rank) const {
        for (const auto& key : observed_ingress) if (std::get<0>(key) == rank) return key;
        throw std::runtime_error("test has not observed this rank's admission key");
    }
};

struct MemorySpy final : AstraMemoryAPI {
    AstraMemoryAPI* delegate;
    std::vector<uint64_t> nodes;
    explicit MemorySpy(AstraMemoryAPI* api) : delegate(api) {}
    void set_sys(int id, Sys* sys) override { delegate->set_sys(id, sys); }
    MemoryLocationType get_memory_location_type() const override { return delegate->get_memory_location_type(); }
    void issue(const MemoryRequest& request, WorkloadLayerHandlerData* handler) override {
        nodes.push_back(handler->node_id);
        delegate->issue(request, handler);
    }
};

// Inputs are produced by Python's actual converter/finalizer, not a second schema writer.
struct Fixture {
    std::shared_ptr<NetworkAnalytical::EventQueue> events;
    std::shared_ptr<JointSourceContext> context;
    std::shared_ptr<Provider> provider = std::make_shared<Provider>();
    std::shared_ptr<Bridge> bridge;
    std::unique_ptr<PhysicalServiceFactory> services;
    std::vector<std::unique_ptr<MemorySpy>> memories;
    std::vector<std::unique_ptr<Network>> networks;
    std::vector<std::unique_ptr<Sys>> systems;

    explicit Fixture(const std::filesystem::path& root,
                     Bridge::Limits limits = {256, 1048576, 4096}, bool bind = true) {
        context = JointSourceContext::load((root / "joint-context.json").string());
        events = std::make_shared<NetworkAnalytical::EventQueue>();
        const auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(
            NetworkAnalytical::NetworkParser((root / "network.yml").string()));
        Network::set_event_queue(events);
        Network::set_topology(topology);
        const auto memory = load_memory_tier_config((root / "manifest.json").string());
        services = std::make_unique<PhysicalServiceFactory>(memory,
            (root / "binding.json").string(), 2);
        bridge = std::make_shared<Bridge>(context, "fixture-session", provider, limits);
        for (int rank = 0; rank != 2; ++rank) {
            auto bindings = services->at(rank).memory;
            for (auto& binding : bindings) {
                memories.push_back(std::make_unique<MemorySpy>(binding.api));
                binding.api = memories.back().get();
            }
            networks.push_back(std::make_unique<Network>(rank));
            systems.push_back(std::make_unique<Sys>(rank, (root / "llm").string(), "empty",
                (root / "system.json").string(), bindings, memory.manifest_digest,
                networks.back().get(), topology->get_npus_count_per_dim(), std::vector<int>{1},
                1.0, 1.0, false, services->at(rank).ucie, services->at(rank).movement, context));
            systems.back()->workload->emit_rank_completions = true;
            if (bind) systems.back()->workload->bind_external_service(bridge);
        }
    }

    Workload& work(unsigned rank = 0) { return *systems.at(rank)->workload; }
    Bridge::ReceiveEdge edge() const { return {Sys::boostedTick(), std::nullopt}; }

    void fire() { for (auto& system : systems) system->workload->fire(); }

    void reach_submit() {
        fire();
        for (unsigned count = 0; !provider->attempts; ++count) {
            require(count < 50 && !events->finished(), "original graph never submitted a MEM");
            events->proceed();
            bridge->rethrow_failure();
        }
    }

    void advance(uint64_t time) {
        require(time >= Sys::boostedTick(), "test tried to reverse time");
        events->schedule_event(time, [](void*) {}, nullptr);
        drain_events();
        require(Sys::boostedTick() == time, "test edge did not reach the real event clock");
    }

    void drain_events() {
        for (unsigned count = 0; !events->finished(); ++count) {
            require(count < 500, "finite fixture did not drain");
            events->proceed();
            bridge->rethrow_failure();
        }
    }

    void complete(std::size_t index, std::optional<uint64_t> actual_fs = std::nullopt,
                  std::optional<uint64_t> closed_ns = std::nullopt) {
        const auto identity = provider->accepted.at(index).identity;
        bridge->complete({identity, actual_fs.value_or(Sys::boostedTick() * 1000000),
                          Bridge::Outcome::Success}, {Sys::boostedTick(), closed_ns});
    }

    void finish(std::size_t completed = 0) {
        for (unsigned step = 0; step != 100; ++step) {
            drain_events();
            bridge->resume_ready(edge());
            if (completed < provider->accepted.size()) complete(completed++);
            else if (work(0).is_finished && work(1).is_finished) {
                bridge->close();
                require(work(0).rank_completion_count == 1 && work(1).rank_completion_count == 1,
                        "original rank owner did not complete exactly once");
                return;
            }
        }
        throw std::runtime_error("fixture left unresolved work");
    }

    ~Fixture() {
        bridge->abort_local();
        // Stop admission, then retire original event envelopes while Sys owners remain alive.
        for (unsigned count = 0; !events->finished() && count != 500; ++count) events->proceed();
        provider->accepted.clear();
    }
};
}  // namespace ExternalMemoryTests
