/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <cstdint>
#include <stdexcept>

#include "astra-sim/system/MemoryTierConfig.hh"
#include "astra-sim/system/memory/BandwidthResource.hh"
#include "astra-sim/system/memory/UcieTransport.hh"

namespace {

using json = nlohmann::json;

template <typename Error, typename Fn>
bool throws(Fn fn) {
    try {
        fn();
    } catch (const Error&) {
        return true;
    }
    return false;
}

json ucie_link(uint64_t read_bps, uint64_t write_bps, uint64_t shared_bps) {
    return {
        {"id", "ucie-frontside"},
        {"endpoints", json::array({"compute", "hbm"})},
        {"stack_count", 1u},
        {"header_bytes", 64u},
        {"latency_ns", 20u},
        {"bandwidth_resource",
         {
             {"schema_version", "bandwidth-resource-v1"},
             {"read_bytes_per_second", read_bps},
             {"write_bytes_per_second", write_bps},
             {"shared_bytes_per_second", shared_bps},
             {"concurrency", "simultaneous"},
             {"turnaround_ns", 0u},
         }},
    };
}

json native_hbm_tier() {
    return {
        {"tier_id", 16u},
        {"tier_name", "hbm"},
        {"backend_kind", "analytical"},
        {"scope", "instance"},
        {"pool_key", "hbm"},
        {"devices", json::array({json{
                        {"device_id", 0u},
                        {"capacity_bytes", 1024u},
                    }})},
        {"num_devices", 1u},
        {"mem_bw_gbps", 1000},
        {"mem_latency_ns", 100},
    };
}

json movement_resource(const std::string& id) {
    return {
        {"id", id},
        {"stack_count", 1u},
        {"latency_ns", 20u},
        {"bandwidth_resource",
         {
             {"schema_version", "bandwidth-resource-v1"},
             {"read_bytes_per_second", 500'000'000ULL},
             {"write_bytes_per_second", 500'000'000ULL},
             {"shared_bytes_per_second", 500'000'000ULL},
             {"concurrency", "serialized"},
             {"turnaround_ns", 0u},
         }},
    };
}

json movement_paths() {
    return {
        {"schema_version", "movement-path-v1"},
        {"selection_mode", "fixed"},
        {"selected_capability_id", "gpu_routed"},
        {"capabilities",
         json::array(
             {{{"id", "gpu_routed"},
               {"engine_location", "gpu"},
               {"engine_count", 1u},
               {"max_priority_burst", 4u},
               {"max_in_flight_page_movements", 1u},
               {"timing_provenance", "estimated"},
               {"segments",
                json::array(
                    {{{"id", "base-to-gpu"},
                      {"kind", "ucie_transaction"},
                      {"resource_ref", "ucie-frontside"},
                      {"operation", "read"},
                      {"byte_rule", "payload"}},
                     {{"id", "gpu-dma"},
                      {"kind", "bandwidth_resource"},
                      {"resource_ref", "gpu-dma"},
                      {"operation", "write"},
                      {"byte_rule", "payload"}},
                     {{"id", "gpu-to-base"},
                      {"kind", "ucie_transaction"},
                      {"resource_ref", "ucie-frontside"},
                      {"operation", "write"},
                      {"byte_rule", "payload"}}})}}})},
        {"bandwidth_resources", json::array({movement_resource("gpu-dma")})},
    };
}

bool contract_holds() {
    using AstraSim::BandwidthResource;
    using AstraSim::MemoryOperation;
    using AstraSim::parse_memory_tier_config;
    using AstraSim::ucie_transaction_hops;

    const auto read_hops =
        ucie_transaction_hops(MemoryOperation::Read, 1024, 64);
    if (read_hops.size() != 2 ||
        read_hops[0].operation != MemoryOperation::Write ||
        read_hops[0].bytes != 64 ||
        read_hops[1].operation != MemoryOperation::Read ||
        read_hops[1].bytes != 1024) {
        return false;
    }
    const auto write_hops =
        ucie_transaction_hops(MemoryOperation::Write, 1024, 64);
    if (write_hops.size() != 1 ||
        write_hops[0].operation != MemoryOperation::Write ||
        write_hops[0].bytes != 1088) {
        return false;
    }
    if (!throws<std::invalid_argument>([]() {
            ucie_transaction_hops(MemoryOperation::Read, 0, 0);
        })) {
        return false;
    }

    const BandwidthResource slow({
        500'000'000ULL,
        500'000'000ULL,
        1'000'000'000ULL,
        AstraSim::BandwidthConcurrency::Simultaneous,
        0,
    });
    const BandwidthResource fast({
        1'000'000'000ULL,
        1'000'000'000ULL,
        2'000'000'000ULL,
        AstraSim::BandwidthConcurrency::Simultaneous,
        0,
    });
    const uint64_t slow_read =
        slow.service_time_ns(64, MemoryOperation::Write, 20) +
        slow.service_time_ns(1024, MemoryOperation::Read, 20);
    const uint64_t fast_read =
        fast.service_time_ns(64, MemoryOperation::Write, 20) +
        fast.service_time_ns(1024, MemoryOperation::Read, 20);
    if (fast_read >= slow_read) {
        return false;
    }

    json payload = {
        {"schema_version", "memory-tier-runtime-v1"},
        {"id_mode", "native"},
        {"manifest_digest", "sha256:" + std::string(64, 'c')},
        {"tiers", json::array({native_hbm_tier()})},
        {"home_domain_topology",
         {{"schema_version", "home-domain-topology-v1"},
          {"domains",
           json::array({{{"home_domain_id", 0u},
                         {"hot", "hbm:0"},
                         {"cold", "lpddr:0"}}})},
          {"home_mapping",
           {{"policy", "stable_hash_v1"}, {"epoch", 0u}, {"seed", 7u}}}}},
        {"ucie_links", json::array({ucie_link(
                           500'000'000ULL, 500'000'000ULL, 1'000'000'000ULL)})},
        {"movement_paths", movement_paths()},
    };
    AstraSim::MemoryTierConfigSet parsed;
    try {
        parsed = parse_memory_tier_config(payload);
    } catch (const std::exception&) {
        return false;
    }
    if (parsed.ucie_links.size() != 1 ||
        parsed.ucie_links[0].id != "ucie-frontside" ||
        parsed.ucie_links[0].header_bytes != 64 ||
        parsed.ucie_links[0].latency_ns != 20 ||
        !parsed.has_movement_paths ||
        parsed.selected_movement_path_id != "gpu_routed" ||
        parsed.movement_path_capabilities.size() != 1 ||
        parsed.movement_path_capabilities[0].segments.size() != 3 ||
        parsed.movement_bandwidth_resources.size() != 1) {
        return false;
    }

    auto unknown = payload;
    unknown["ucie_links"][0]["memory_type"] = "UCIE_MEMORY";
    auto empty = payload;
    empty["ucie_links"] = nlohmann::json::array();
    auto collide = payload;
    collide["ucie_links"][0]["id"] = "hbm";
    auto no_compute = payload;
    no_compute["ucie_links"][0]["endpoints"] =
        json::array({"hbm", "remote"});
    auto unknown_peer = payload;
    unknown_peer["ucie_links"][0]["endpoints"] =
        json::array({"compute", "lpddr"});
    auto stack_mismatch = payload;
    stack_mismatch["ucie_links"][0]["stack_count"] = 2u;
    auto unsupported_selection = payload;
    unsupported_selection["movement_paths"]["selected_capability_id"] =
        "base_die_local";
    auto missing_round_trip = payload;
    missing_round_trip["movement_paths"]["capabilities"][0]["segments"].erase(2);
    auto unknown_resource = payload;
    unknown_resource["movement_paths"]["capabilities"][0]["segments"][1]
                    ["resource_ref"] = "missing";
    return throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(unknown); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(empty); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(collide); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(no_compute); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(unknown_peer); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(stack_mismatch); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(unsupported_selection); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(missing_round_trip); }) &&
        throws<std::invalid_argument>(
               [&]() { parse_memory_tier_config(unknown_resource); });
}

}  // namespace

int main() {
    return contract_holds() ? 0 : 1;
}
