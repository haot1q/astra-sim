#ifndef __PAIRED_COLD_COPY_CONFIG_HH__
#define __PAIRED_COLD_COPY_CONFIG_HH__

#include "astra-sim/system/MemoryTierConfig.hh"

namespace AstraSim {

// Derive cold-copy eligibility without tightening existing Page-path parsing.
// A zero/unavailable pair is rejected when a new local cold copy consumes it.
inline void configure_paired_cold_copy(
    const nlohmann::json& payload, MovementPathCapabilityConfig& capability) {
    const auto& topology = payload.at("home_domain_topology");
    if (topology.value("schema_version", "") != "home-domain-topology-v1" ||
        topology.contains("cold_endpoint_policy")) return;
    const auto& domains = topology.at("domains");
    capability.paired_cold_tiers.resize(domains.size());
    for (std::size_t i = 0; i < domains.size(); ++i) {
        const auto& domain = domains[i];
        if (!domain.is_object() || !domain.contains("home_domain_id") ||
            domain.at("home_domain_id") != i || !domain.contains("hot") ||
            !domain.contains("cold")) continue;
        auto& pair = capability.paired_cold_tiers[i];
        for (const auto& tier : payload.at("tiers")) {
            if (tier.at("num_devices") <= i) continue;
            const auto endpoint = tier.at("tier_name").get<std::string>() +
                                  ":" + std::to_string(i);
            if (domain.at("hot") == endpoint) pair.first = tier.at("tier_id");
            if (domain.at("cold") == endpoint) pair.second = tier.at("tier_id");
        }
    }
}

}  // namespace AstraSim
#endif
