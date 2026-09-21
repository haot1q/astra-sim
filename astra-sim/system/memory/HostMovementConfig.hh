#ifndef __HOST_MOVEMENT_CONFIG_HH__
#define __HOST_MOVEMENT_CONFIG_HH__

#include <stdexcept>
#include <string>

#include "astra-sim/system/MemoryTierConfig.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim {

// Validate the bounded shared-cold contract independently of Python.
inline void configure_host_movement(
    const nlohmann::json& payload, MovementPathCapabilityConfig& capability) {
    const auto& topology = payload.at("home_domain_topology");
    ServiceBindingJson::fields(topology,
        {"schema_version", "cold_endpoint_policy", "domains", "home_mapping"});
    if (topology.value("schema_version", "") != "home-domain-topology-v2" ||
        topology.value("cold_endpoint_policy", "") != "shared_host_pool_v1") {
        throw std::invalid_argument("host_pcie requires shared Host topology v2");
    }
    const auto& domains = topology.at("domains");
    const std::string cold = domains.at(0).at("cold");
    const std::string hot = domains.at(0).at("hot");
    const auto hot_name = hot.substr(0, hot.find(':'));
    const auto cold_name = cold.substr(0, cold.find(':'));
    if (hot_name == cold_name || cold != cold_name + ":0") {
        throw std::invalid_argument("shared Host must be a distinct device 0");
    }
    for (std::size_t i = 0; i < domains.size(); ++i) {
        const auto& domain = domains[i];
        ServiceBindingJson::fields(domain, {"home_domain_id", "hot", "cold"});
        if (!domain.at("home_domain_id").is_number_unsigned() ||
            domain.at("home_domain_id") != i || domain.at("cold") != cold ||
            domain.at("hot") != hot_name + ":" + std::to_string(i)) {
            throw std::invalid_argument("invalid shared Host domain endpoints");
        }
    }
    for (const auto& tier : payload.at("tiers")) {
        if (tier.at("tier_name") == hot_name) {
            if (tier.at("num_devices") != domains.size()) {
                throw std::invalid_argument("Host hot tier must cover all domains");
            }
            capability.host_hot_tier_id = tier.at("tier_id");
        }
        if (tier.at("tier_name") == cold_name) {
            if (tier.at("scope") != "node" || tier.at("num_devices") != 1) {
                throw std::invalid_argument("Host cold tier requires node scope and one device");
            }
            capability.host_cold_tier_id = tier.at("tier_id");
        }
    }
    if (!capability.host_hot_tier_id || !capability.host_cold_tier_id) {
        throw std::invalid_argument("Host path references an unknown tier");
    }
    capability.host_domain_count = domains.size();
}

}  // namespace AstraSim
#endif
