/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "astra-sim/system/memory/TransportPathAPI.hh"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
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

void write_transport(const std::filesystem::path& root, const Json& binding) {
    Json transport = {{"schema_version", "transport-service-v1"},
        {"physical_network_sha256", std::string(64, 'a')},
        {"service_binding_digest", binding["binding_digest"]},
        {"service_activation_id", binding["activation_id"]},
        {"policy", "fifo_serialized_latency_v1"}, {"rounding_mode", "ceil_ns_v1"},
        {"rank_count", 2u}, {"resources", Json::array()}, {"links", Json::array()}};
    for (const auto& id : {"a", "b", "c"}) {
        transport["resources"].push_back({{"id", id},
            {"bandwidth_bytes_per_second", 1'000'000'000ULL}, {"latency_ns", 0u}});
        transport["links"].push_back({{"id", id}, {"source", "from"},
            {"target", "to"}, {"resource", id}});
    }
    transport["links"].push_back({{"id", "back"}, {"source", "to"},
        {"target", "from"}, {"resource", "a"}});
    transport["projection_digest"] = Wire::digest(transport);
    write_json(root / "transport.json", transport);
    Json routes = {{"schema_version", "physical-message-routes-v1"},
        {"timing_profile", "message_store_forward_v1"},
        {"physical_network_sha256", transport["physical_network_sha256"]},
        {"transport_projection_digest", transport["projection_digest"]},
        {"service_binding_digest", binding["binding_digest"]},
        {"service_activation_id", binding["activation_id"]}, {"rank_count", 2u},
        {"routes", {{{"source_rank", 0u}, {"target_rank", 1u},
                     {"service_link_ids", {"a"}}}}}};
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

    explicit Fixture(const Json& golden) {
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
        write_transport(root, golden["binding"]);
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

    void drain() {
        for (unsigned count = 0; !events->finished(); ++count) {
            require(count < 200, "path callbacks did not drain");
            events->proceed();
            paths->rethrow_failure();
        }
    }
};

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
        Fixture fixture(Wire::read(argv[1]));
        const std::string mode = argv[2];
        if (mode == "paths") paths_and_messages(fixture);
        else if (mode == "delayed") delayed_and_empty(fixture);
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
