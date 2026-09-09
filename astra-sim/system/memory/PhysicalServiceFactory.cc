/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "PhysicalServiceFactory.hh"

#include <cstdio>
#include <stdexcept>
#include <utility>

#include "extern/memory_backend/analytical/AnalyticalMemory.hh"

namespace AstraSim {

PhysicalServiceFactory::PhysicalServiceFactory(
    const MemoryTierConfigSet& memory, const std::string& binding_path, uint32_t rank_count,
    const std::string& pd_path_binding_path)
    : native_(memory.native) {
    if (native_) {
        config_ = load_physical_service_config(binding_path, memory, rank_count);
        pd_config_ = load_pd_path_service_config(pd_path_binding_path, config_);
        for (const auto& [ref, backend] : config_.backends) {
            physical_.emplace(ref, own(backend, true));
        }
        for (const auto& [ref, backend] : pd_config_.declared_backends) {
            physical_.emplace(ref, own(backend, true));
        }
    } else {
        if (!binding_path.empty() || !pd_path_binding_path.empty()) {
            throw std::invalid_argument("legacy memory cannot use physical service bindings");
        }
        legacy_backends(memory);
    }
    for (uint32_t rank = 0; rank < rank_count; ++rank) {
        ranks_.push_back(rank_bindings(memory, rank));
    }
    for (const auto& [key, ref] : pd_config_.bindings) {
        adapters_.push_back(std::make_unique<PhysicalServiceAdapter>(key.first,
            std::vector<AstraMemoryAPI*>{physical_.at(ref)}, pd_config_.rounding.at(ref)));
        interfaces_.emplace(key, adapters_.back().get());
    }
}

PhysicalServiceFactory::~PhysicalServiceFactory() = default;

AstraMemoryAPI* PhysicalServiceFactory::own(nlohmann::json backend, bool physical) {
    if (physical) backend["physical-service-mode"] = "native-single-device-v1";
    const auto path = write_temporary_memory_backend_config(backend);
    try {
        memories_.push_back(std::make_unique<Analytical::AnalyticalMemory>(path));
    } catch (...) {
        std::remove(path.c_str());
        throw;
    }
    std::remove(path.c_str());
    return memories_.back().get();
}

void PhysicalServiceFactory::legacy_backends(const MemoryTierConfigSet& memory) {
    for (const auto& tier : memory.tiers) {
        legacy_.emplace(ServiceEndpoint{"memory", tier.tier_name, 0}, own(tier.backend_config, false));
    }
    // Kept explicitly identical to the old frontend construction. The legacy
    // parser rejects native UCIe/movement declarations before this point.
    for (const auto& link : memory.ucie_links) {
        legacy_.emplace(ServiceEndpoint{"ucie", link.id, 0}, own(link.backend_config, false));
    }
    for (const auto& resource : memory.movement_bandwidth_resources) {
        legacy_.emplace(ServiceEndpoint{"movement", resource.id, 0}, own(resource.backend_config, false));
    }
}

AstraMemoryAPI* PhysicalServiceFactory::route(
    uint32_t rank, const std::string& kind, const std::string& ref, uint32_t devices) {
    if (!native_) return legacy_.at({kind, ref, 0});
    std::vector<AstraMemoryAPI*> endpoints;
    for (uint32_t device = 0; device < devices; ++device) {
        endpoints.push_back(physical_.at(config_.bindings.at({rank, kind, ref, device})));
    }
    adapters_.push_back(std::make_unique<PhysicalServiceAdapter>(rank, std::move(endpoints)));
    return adapters_.back().get();
}

RankServiceBindings PhysicalServiceFactory::rank_bindings(
    const MemoryTierConfigSet& memory, uint32_t rank) {
    RankServiceBindings result;
    for (const auto& tier : memory.tiers) {
        result.memory.push_back({tier.tier_id, tier.tier_name, tier.num_devices,
            route(rank, "memory", tier.tier_name, tier.num_devices), config_.identity});
    }
    std::vector<UcieLinkBinding> links;
    for (const auto& link : memory.ucie_links) {
        links.push_back({link.id, link.stack_count, link.header_bytes, link.latency_ns,
            route(rank, "ucie", link.id, link.stack_count)});
    }
    result.ucie = UcieLinkRegistry(std::move(links));
    if (memory.has_movement_paths) {
        std::vector<MovementBandwidthBinding> resources;
        for (const auto& resource : memory.movement_bandwidth_resources) {
            resources.push_back({resource.id, resource.stack_count, resource.latency_ns,
                route(rank, "movement", resource.id, resource.stack_count)});
        }
        result.movement = MovementPathRegistry(memory.selected_movement_path_id,
            memory.movement_path_capabilities, std::move(resources));
    }
    return result;
}

const RankServiceBindings& PhysicalServiceFactory::at(uint32_t rank) const {
    return ranks_.at(rank);
}

const ServiceBindingIdentity& PhysicalServiceFactory::identity() const {
    return config_.identity;
}

AstraMemoryAPI* PhysicalServiceFactory::pd_interface(
    uint32_t rank, const std::string& interface_id) const {
    const auto found = interfaces_.find({rank, interface_id});
    if (found == interfaces_.end()) throw std::invalid_argument("unbound P/D physical interface");
    return found->second;
}

const PdPathServiceConfig& PhysicalServiceFactory::pd_path_config() const {
    return pd_config_;
}

void PhysicalServiceFactory::rethrow_failure() const {
    for (const auto& adapter : adapters_) adapter->rethrow_failure();
    for (const auto& memory : memories_) memory->rethrow_failure();
}

}  // namespace AstraSim
