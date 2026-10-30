/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#pragma once

#include "astra-sim/system/memory/TransportPathAPI.hh"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "common/PhysicalRouteTransport.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>
#include "protoio.hh"

namespace {
using namespace AstraSim;
using Json = nlohmann::json;
namespace Wire = ServiceBindingJson;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;
using Paths = AstraSimAnalytical::PhysicalRouteTransport;

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

void write_json(const std::filesystem::path& path, const Json& value) {
    std::ofstream stream(path);
    stream << value.dump();
    require(stream.good(), "path fixture write failed");
}

void empty_et(const std::filesystem::path& root, const Json& fixture, int rank) {
    ChakraProtoMsg::GlobalMetadata metadata;
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>>{
             {"tier_manifest_digest", fixture["manifest"]["manifest_digest"]},
             {"service_binding_digest", fixture["binding"]["binding_digest"]},
             {"service_activation_id", fixture["binding"]["activation_id"]}}) {
        auto* attr = metadata.add_attr();
        attr->set_name(name);
        attr->set_string_val(value);
    }
    auto* attr = metadata.add_attr();
    attr->set_name("service_rank");
    attr->set_uint64_val(rank);
    ProtoOutputStream stream((root / ("empty." + std::to_string(rank) + ".et")).string());
    stream.write(metadata);
}

using Customize = std::function<void(Json&, Json&)>;

void write_transport(const std::filesystem::path& root, const Json& binding, bool propagation,
                     const Customize& customize) {
    Json transport = {{"schema_version", "transport-service-v1"},
        {"physical_network_sha256", std::string(64, 'a')},
        {"service_binding_digest", binding["binding_digest"]},
        {"service_activation_id", binding["activation_id"]},
        {"policy", propagation ? "fifo_service_then_propagate_v1" : "fifo_serialized_latency_v1"}, {"rounding_mode", "ceil_ns_v1"},
        {"rank_count", 2u}, {"resources", Json::array()}, {"links", Json::array()}};
    for (const auto& id : {"a", "b", "c"}) {
        transport["resources"].push_back({{"id", id},
            {"bandwidth_bytes_per_second", 1'000'000'000ULL}, {"latency_ns", propagation ? 100u : 0u}});
        transport["links"].push_back({{"id", id}, {"source", "from"},
            {"target", "to"}, {"resource", id}});
    }
    transport["links"].push_back({{"id", "back"}, {"source", "to"},
        {"target", "from"}, {"resource", "a"}});
    Json routes = {{"schema_version", "physical-message-routes-v1"},
        {"timing_profile", propagation ? "message_store_forward_propagation_v1" : "message_store_forward_v1"},
        {"physical_network_sha256", transport["physical_network_sha256"]},
        {"transport_projection_digest", ""},
        {"service_binding_digest", binding["binding_digest"]},
        {"service_activation_id", binding["activation_id"]}, {"rank_count", 2u},
        {"routes", {{{"source_rank", 0u}, {"target_rank", 1u},
                     {"service_link_ids", {"a"}}}}}};
    if (customize) customize(transport, routes);
    transport["projection_digest"] = Wire::digest(transport);
    routes["transport_projection_digest"] = transport["projection_digest"];
    write_json(root / "transport.json", transport);
    routes["routes_digest"] = Wire::digest(routes);
    write_json(root / "routes.json", routes);
}

struct Fixture {
    std::filesystem::path root;
    std::shared_ptr<NetworkAnalytical::EventQueue> events;
    std::unique_ptr<PhysicalServiceFactory> services;
    std::vector<std::unique_ptr<Network>> networks;
    std::vector<std::unique_ptr<Sys>> owned;
    std::vector<Sys*> systems;
    std::unique_ptr<Paths> paths;

    explicit Fixture(const Json& golden, bool propagation = false, const Customize& customize = {}) {
        auto pattern = (std::filesystem::temp_directory_path() / "physical-path-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        require(::mkdtemp(buffer.data()), "mkdtemp failed");
        root = buffer.data();
        write_json(root / "memory.json", golden["manifest"]);
        write_json(root / "binding.json", golden["binding"]);
        write_json(root / "system.json", {
            {"scheduling-policy", "LIFO"}, {"endpoint-delay", 0u},
            {"active-chunks-per-dimension", 1u}, {"preferred-dataset-splits", 1u},
            {"collective-optimization", "localBWAware"}, {"local-mem-bw", 1u},
            {"boost-mode", 0u}, {"all-reduce-implementation", {"ring"}},
            {"all-gather-implementation", {"ring"}}, {"reduce-scatter-implementation", {"ring"}},
            {"all-to-all-implementation", {"ring"}}});
        write_transport(root, golden["binding"], propagation, customize);
        {
            std::ofstream stream(root / "network.yml");
            stream << "topology: [FullyConnected]\nnpus_count: [2]\nbandwidth: [1000.0]\nlatency: [0.0]\n";
        }
        events = std::make_shared<NetworkAnalytical::EventQueue>();
        auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(
            NetworkAnalytical::NetworkParser((root / "network.yml").string()));
        Network::set_event_queue(events);
        Network::set_topology(topology);
        const auto memory = load_memory_tier_config((root / "memory.json").string());
        services = std::make_unique<PhysicalServiceFactory>(memory,
            (root / "binding.json").string(), 2, "", (root / "transport.json").string());
        for (int rank = 0; rank < 2; ++rank) {
            empty_et(root, golden, rank);
            networks.push_back(std::make_unique<Network>(rank));
            owned.push_back(std::make_unique<Sys>(rank, (root / "empty").string(), "empty",
                (root / "system.json").string(), services->at(rank).memory, memory.manifest_digest,
                networks.back().get(), topology->get_npus_count_per_dim(), std::vector<int>{1},
                1.0, 1.0, false, services->at(rank).ucie, services->at(rank).movement));
            systems.push_back(owned.back().get());
        }
        paths = std::make_unique<Paths>((root / "routes.json").string(),
            (root / "summary.json").string(), *services, systems, events);
    }

    void drain(unsigned limit = 200) {
        for (unsigned count = 0; !events->finished(); ++count) {
            require(count < limit, "path callbacks did not drain");
            events->proceed();
            paths->rethrow_failure();
        }
    }
};

}  // namespace
