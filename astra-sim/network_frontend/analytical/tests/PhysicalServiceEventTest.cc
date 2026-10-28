/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <vector>

#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
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

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

void write_json(const std::filesystem::path& path, const Json& value) {
    std::ofstream stream(path);
    stream << value.dump();
    require(stream.good(), "test fixture write failed");
}

std::filesystem::path temporary_directory() {
    auto pattern = (std::filesystem::temp_directory_path() / "physical-service-events-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end());
    buffer.push_back('\0');
    if (!::mkdtemp(buffer.data())) throw std::runtime_error("mkdtemp failed");
    return std::filesystem::path(buffer.data());
}

void seal(Json& document) {
    auto& manifest = document["manifest"];
    manifest.erase("manifest_digest");
    manifest["manifest_digest"] = Wire::digest(manifest);
    auto& binding = document["binding"];
    binding["tier_manifest_digest"] = manifest["manifest_digest"];
    binding.erase("binding_digest");
    binding.erase("activation_id");
    std::sort(binding["ranks"].begin(), binding["ranks"].end(),
              [](const auto& a, const auto& b) { return a["rank"] < b["rank"]; });
    std::sort(binding["resources"].begin(), binding["resources"].end(),
              [](const auto& a, const auto& b) { return a["id"] < b["id"]; });
    std::sort(binding["bindings"].begin(), binding["bindings"].end(), [](const auto& a, const auto& b) {
        return std::make_tuple(a["rank"], a["kind"], a["logical_ref"], a["logical_device_id"]) <
               std::make_tuple(b["rank"], b["kind"], b["logical_ref"], b["logical_device_id"]);
    });
    binding["binding_digest"] = Wire::digest(binding);
    binding["activation_id"] = "event-test-run";
    if (document.contains("pd_path_services")) {
        auto& path = document["pd_path_services"];
        path["service_binding_digest"] = binding["binding_digest"];
        path["service_activation_id"] = binding["activation_id"];
        path.erase("binding_digest");
        path["binding_digest"] = Wire::digest(path, true);
    }
}

void write_empty_et(const std::filesystem::path& root, const Json& document, uint32_t rank) {
    ChakraProtoMsg::GlobalMetadata metadata;
    for (const auto& [name, value] : std::vector<std::pair<std::string, std::string>>{
            {"tier_manifest_digest", document["manifest"]["manifest_digest"]},
            {"service_binding_digest", document["binding"]["binding_digest"]},
            {"service_activation_id", document["binding"]["activation_id"]}}) {
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

struct TaggedHandler : WorkloadLayerHandlerData { int marker = 42; };

struct Completion : Callable {
    unsigned calls = 0;
    void call(EventType, CallData* data) override {
        auto* handler = static_cast<TaggedHandler*>(data);
        require(handler->marker == 42, "handler dynamic state was lost");
        ++calls;
        LoggerFactory::get_logger("physical-service-test")->info(
            "RECEIPT rank={} logical_device={} ready={} start={} finish={}",
            handler->sys_id, handler->device_id, handler->memory_ready_ns,
            handler->memory_start_ns, handler->memory_finish_ns);
    }
};

struct TaggedTransport : TransportRequest {
    using TransportRequest::TransportRequest;
    int marker = 42;
};

struct TransportCompletion : Callable {
    unsigned calls = 0;
    void call(EventType, CallData* data) override {
        auto* context = static_cast<TaggedTransport*>(data);
        require(context->marker == 42, "transport lost derived handler state");
        const auto result = context->receipt();
        ++calls;
        LoggerFactory::get_logger("physical-service-test")->info(
            "TRANSPORT request={} resource={} link={} bytes={} ready={} start={} finish={}",
            result.request_id, result.resource_id, result.link_id, result.payload_bytes,
            result.ready_ns, result.start_ns, result.finish_ns);
    }
};

struct DelayedTransport : Callable {
    TransportServiceAdapter* api = nullptr;
    TransportRequest* request = nullptr;
    void call(EventType, CallData*) override { api->issue(*request); }
};

struct RequestPattern {
    MemoryOperation second = MemoryOperation::Read;
    uint32_t device = 0;
    uint64_t bytes = 1024;
    uint64_t second_bytes = 0;
    uint32_t second_tier = 16;
    std::string interface_id;
    bool first_memory = false;
    uint64_t first_finish = 0;
    bool transport = false;
    uint64_t second_ready = 0;
    unsigned requests = 2;
};

struct TransportRun {
    TransportCompletion completion;
    std::vector<std::unique_ptr<TaggedTransport>> requests;
    DelayedTransport delayed;

    void issue(PhysicalServiceFactory& services, Sys& sys, unsigned rank,
               const RequestPattern& pattern) {
        auto& api = services.transport(rank, rank == 0 ? "out" : "back");
        api.set_sys(&sys);
        requests.push_back(std::make_unique<TaggedTransport>(rank,
            "request-" + std::to_string(rank), rank == 1 && pattern.second_bytes
                ? pattern.second_bytes : pattern.bytes, completion));
        auto& request = *requests.back();
        if (rank == 1 && pattern.second_ready) {
            delayed.api = &api;
            delayed.request = &request;
            sys.register_event(&delayed, EventType::General, nullptr, pattern.second_ready);
        } else {
            api.issue(request);
        }
        bool rejected = false;
        try { request.receipt(); } catch (const std::logic_error&) { rejected = true; }
        require(rejected, "submission was reported as completed transport");
    }

    void check(const RequestPattern& pattern, uint64_t second_finish) const {
        require(completion.calls == pattern.requests, "transport callback count mismatch");
        const auto first = requests[0]->receipt();
        require(first.finish_ns == pattern.first_finish && first.ready_ns == 0 && first.start_ns == 0,
                "first transport duration changed");
        if (pattern.requests == 2) {
            const auto second = requests[1]->receipt();
            require(second.finish_ns == second_finish && second.ready_ns == pattern.second_ready &&
                    second.start_ns == second_finish - pattern.first_finish,
                    "transport shared queue or actual ready time mismatch");
        }
    }

    void check_failed_last() const {
        bool rejected = false;
        try { requests.back()->receipt(); } catch (const std::logic_error&) { rejected = true; }
        require(rejected, "failed transport has a success receipt");
    }
};

void run_events(Json document, const char* name, uint64_t second_finish,
                RequestPattern pattern = {}) {
    seal(document);
    const auto root = temporary_directory();
    write_json(root / "memory.json", document["manifest"]);
    write_json(root / "binding.json", document["binding"]);
    const Json system = {
        {"scheduling-policy", "LIFO"}, {"endpoint-delay", 0u},
        {"active-chunks-per-dimension", 1u}, {"preferred-dataset-splits", 1u},
        {"collective-optimization", "localBWAware"}, {"local-mem-bw", 1u}, {"boost-mode", 0u},
        {"all-reduce-implementation", {"ring"}}, {"all-gather-implementation", {"ring"}},
        {"reduce-scatter-implementation", {"ring"}}, {"all-to-all-implementation", {"ring"}},
    };
    write_json(root / "system.json", system);
    {
        std::ofstream network(root / "network.yml");
        network << "topology: [FullyConnected]\nnpus_count: [2]\nbandwidth: [1000.0]\nlatency: [0.0]\n";
    }
    auto events = std::make_shared<NetworkAnalytical::EventQueue>();
    auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(
        NetworkAnalytical::NetworkParser((root / "network.yml").string()));
    Network::set_event_queue(events);
    Network::set_topology(topology);
    const auto memory = load_memory_tier_config((root / "memory.json").string());
    std::string pd_path;
    if (document.contains("pd_path_services")) {
        pd_path = (root / "pd-path.json").string();
        write_json(pd_path, document["pd_path_services"]);
    }
    std::string transport_path;
    if (pattern.transport) {
        transport_path = (root / "transport.json").string();
        write_json(transport_path, document["transport"]);
    }
    PhysicalServiceFactory services(memory, (root / "binding.json").string(), 2, pd_path,
                                     transport_path);
    std::vector<std::unique_ptr<Network>> networks;
    std::vector<std::unique_ptr<Sys>> systems;
    for (int rank = 0; rank < 2; ++rank) {
        write_empty_et(root, document, rank);
        networks.push_back(std::make_unique<Network>(rank));
        systems.push_back(std::make_unique<Sys>(rank, (root / "empty").string(), "empty",
            (root / "system.json").string(), services.at(rank).memory, memory.manifest_digest,
            networks.back().get(), topology->get_npus_count_per_dim(), std::vector<int>{1},
            1.0, 1.0, false, services.at(rank).ucie, services.at(rank).movement));
    }
    Completion completion;
    TaggedHandler requests[2];
    TransportRun transport;
    for (unsigned rank = 0; rank < pattern.requests; ++rank) {
        if (pattern.transport) {
            transport.issue(services, *systems[rank], rank, pattern);
            continue;
        }
        requests[rank].sys_id = rank;
        requests[rank].device_id = pattern.device;
        requests[rank].completion_target = &completion;
        auto* api = pattern.interface_id.empty() || (rank == 0 && pattern.first_memory)
            ? systems[rank]->memory_api(rank == 0 ? 16 : pattern.second_tier, pattern.device)
            : services.pd_interface(rank, pattern.interface_id);
        api->set_sys(rank, systems[rank].get());
        api->issue(
            {rank == 1 && pattern.second_bytes != 0 ? pattern.second_bytes : pattern.bytes,
             rank == 0 ? MemoryOperation::Read : pattern.second}, &requests[rank]);
    }
    for (unsigned steps = 0; !events->finished(); ++steps) {
        require(steps < 100, "physical memory callbacks did not drain");
        events->proceed();
        try { services.rethrow_failure(); } catch (...) {
            if (pattern.transport) transport.check_failed_last();
            throw;
        }
    }
    if (pattern.transport) {
        transport.check(pattern, second_finish);
        LoggerFactory::get_logger("physical-service-test")->info("PASS {}", name);
        return;
    }
    require(completion.calls == 2, "missing or duplicate memory callback");
    require(requests[0].memory_finish_ns ==
                (pattern.first_finish ? pattern.first_finish : pattern.bytes),
            "first service runtime changed");
    require(requests[1].memory_finish_ns == second_finish, "incorrect physical resource contention");
    require(requests[0].device_id == pattern.device && requests[1].device_id == pattern.device,
            "logical device ID changed");
    LoggerFactory::get_logger("physical-service-test")->info(
        "PASS {} evidence={}", name, root.string());
}

Json shared(Json input, const char* owner) {
    input["binding"]["resources"].erase(1);
    auto& resource = input["binding"]["resources"][0];
    resource["owner_kind"] = owner;
    resource["owner_id"] = std::string(owner) == "accelerator" ? "gpu-0"
        : std::string(owner) == "node" ? "0" : "global";
    input["binding"]["bindings"][1]["physical_resource_ref"] = "hbm-0";
    if (std::string(owner) == "accelerator") {
        input["binding"]["ranks"][1]["accelerator_id"] = "gpu-0";
    }
    return input;
}

void directional(Json& input, bool simultaneous) {
    auto& tier = input["manifest"]["tiers"][0];
    tier.erase("mem_bw_gbps");
    tier["bandwidth_resource"] = {
        {"schema_version", "bandwidth-resource-v1"},
        {"read_bytes_per_second", 1'000'000'000ULL}, {"write_bytes_per_second", 1'000'000'000ULL},
        {"shared_bytes_per_second", simultaneous ? 2'000'000'000ULL : 1'000'000'000ULL},
        {"concurrency", simultaneous ? "simultaneous" : "serialized"},
        {"turnaround_ns", simultaneous ? 0u : 7u},
    };
}

void nonzero_logical_device(Json input) {
    auto& tier = input["manifest"]["tiers"][0];
    tier["devices"].push_back({{"device_id", 1u}, {"capacity_bytes", 1024u}});
    tier["num_devices"] = 2u;
    for (uint32_t rank = 0; rank < 2; ++rank) {
        auto resource = input["binding"]["resources"][rank];
        resource["id"] = "hbm-" + std::to_string(rank) + "-one";
        resource["physical_device_id"] = 1u;
        input["binding"]["resources"].push_back(resource);
        auto binding = input["binding"]["bindings"][rank];
        binding["logical_device_id"] = 1u;
        binding["physical_resource_ref"] = resource["id"];
        input["binding"]["bindings"].push_back(binding);
    }
    directional(input, false);
    run_events(input, "logical-device-one-internal-zero", 1024,
               RequestPattern{MemoryOperation::Write, 1});
}

void logical_alias(Json input) {
    input = shared(input, "accelerator");
    auto alias = input["manifest"]["tiers"][0];
    alias["tier_id"] = 17u;
    alias["tier_name"] = "hbm-alias";
    alias["pool_key"] = "other-account";
    input["manifest"]["tiers"].push_back(alias);
    for (uint32_t rank = 0; rank < 2; ++rank) {
        auto binding = input["binding"]["bindings"][rank];
        binding["logical_ref"] = "hbm-alias";
        input["binding"]["bindings"].push_back(binding);
    }
    run_events(input, "logical-aliases-one-physical-queue", 2048,
               RequestPattern{MemoryOperation::Read, 0, 1024, 0, 17});
    input["manifest"]["tiers"][1]["mem_bw_gbps"] = 2u;
    bool rejected = false;
    try {
        run_events(input, "conflicting-alias", 0);
    } catch (const std::invalid_argument& error) {
        rejected = std::string(error.what()).find("conflicting physical service parameters") != std::string::npos;
    }
    require(rejected, "conflicting logical alias parameters accepted");
}

Json transport_document(Json input, bool independent = false, uint64_t rate = 1'000'000'000ULL,
                        uint64_t latency = 5) {
    seal(input);
    Json resources = {{{"id", "shared"}, {"bandwidth_bytes_per_second", rate}, {"latency_ns", latency}}};
    if (independent) resources.push_back({{"id", "other"}, {"bandwidth_bytes_per_second", rate},
                                         {"latency_ns", latency}});
    Json wire = {{"schema_version", "transport-service-v1"},
        {"physical_network_sha256", std::string(64, 'a')},
        {"service_binding_digest", input["binding"]["binding_digest"]},
        {"service_activation_id", input["binding"]["activation_id"]},
        {"policy", "fifo_serialized_latency_v1"}, {"rounding_mode", "ceil_ns_v1"},
        {"rank_count", 2u}, {"resources", resources}, {"links", {
            {{"id", "out"}, {"source", "a"}, {"target", "b"}, {"resource", "shared"}},
            {{"id", "back"}, {"source", "b"}, {"target", "a"},
             {"resource", independent ? "other" : "shared"}}}}};
    wire["projection_digest"] = Wire::digest(wire);
    input["transport"] = wire;
    return input;
}

void transport_events(const Json& golden) {
    RequestPattern pattern;
    pattern.transport = true;
    pattern.bytes = 64;
    pattern.first_finish = 69;
    pattern.requests = 1;
    run_events(transport_document(golden), "transport-single", 0, pattern);
    pattern.requests = 2;
    run_events(transport_document(golden), "transport-reverse-shared", 138, pattern);
    run_events(transport_document(golden, true), "transport-independent", 69, pattern);
    pattern.second_ready = 10;
    run_events(transport_document(golden), "transport-delayed-ready", 138, pattern);
    pattern.second_ready = 0;
    pattern.bytes = 1;
    pattern.first_finish = 1;
    run_events(transport_document(golden, false, 2'000'000'000ULL, 0), "transport-ceil", 2, pattern);
    run_events(transport_document(golden, false, 3'000'000'000ULL, 0), "transport-ceil-third", 2, pattern);
    pattern.bytes = 64;
    pattern.first_finish = 69;
    pattern.second_bytes = std::numeric_limits<uint64_t>::max() - 37;
    bool rejected = false;
    try { run_events(transport_document(golden), "transport-queued-overflow", 0, pattern); }
    catch (const std::overflow_error&) { rejected = true; }
    require(rejected, "queued transport overflow was swallowed");
}

template <class Function>
void rejects(Function function, const char* message) {
    bool rejected = false;
    try { function(); } catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, message);
}

void transport_contracts(const Json& golden) {
    auto input = transport_document(golden);
    const auto root = temporary_directory();
    write_json(root / "memory.json", input["manifest"]);
    write_json(root / "binding.json", input["binding"]);
    const auto memory = load_memory_tier_config((root / "memory.json").string());
    const auto physical = load_physical_service_config((root / "binding.json").string(), memory, 2);
    const auto path = (root / "transport.json").string();
    for (const auto& mutate : std::vector<std::function<void(Json&)>>{
        [](Json& j) { j["resources"][0]["bandwidth_bytes_per_second"] = 0u; },
        [](Json& j) { j["resources"][0]["bandwidth_bytes_per_second"] = -1; },
        [](Json& j) { j["resources"][0]["latency_ns"] = true; },
        [](Json& j) { j["resources"][0]["id"] = "hbm-0"; },
        [](Json& j) { j["resources"].push_back(j["resources"][0]); },
        [](Json& j) { j["links"][0]["resource"] = "absent"; },
        [](Json& j) { j["links"][0]["resource"] = nullptr; },
        [](Json& j) { j["links"].push_back(j["links"][0]); },
        [](Json& j) { j["rank_count"] = 3u; },
        [](Json& j) { j["policy"] = "fair"; },
        [](Json& j) { j["rounding_mode"] = "floor_ns_v1"; },
        [](Json& j) { j["service_activation_id"] = "stale"; },
        [](Json& j) { j["extra"] = 1u; }}) {
        auto wire = input["transport"];
        mutate(wire);
        wire.erase("projection_digest");
        wire["projection_digest"] = Wire::digest(wire);
        write_json(path, wire);
        rejects([&] { load_transport_service_config(path, physical); }, "invalid transport wire accepted");
    }
    auto corrupt = input["transport"];
    corrupt["resources"][0]["latency_ns"] = 7u;
    write_json(path, corrupt);
    rejects([&] { load_transport_service_config(path, physical); }, "transport digest mismatch accepted");
    write_json(path, input["transport"]);
    PhysicalServiceFactory services(memory, (root / "binding.json").string(), 2, "", path);
    rejects([&] { services.transport(2, "out"); }, "unknown transport rank accepted");
    rejects([&] { services.transport(0, "absent"); }, "unpriced link accepted");
    TransportCompletion completion;
    TaggedTransport zero(0, "zero", 0, completion), wrong(1, "wrong-rank", 64, completion);
    rejects([&] { services.transport(0, "out").issue(zero); }, "zero payload accepted");
    rejects([&] { services.transport(0, "out").issue(wrong); }, "wrong context rank accepted");
    require(completion.calls == 0, "invalid transport request completed");
}
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2 && argc != 4 && argc != 5) {
            throw std::invalid_argument("expected golden fixture path and optional interface request");
        }
        LoggerFactory::init("empty");
        auto golden = Wire::read(argv[1]);
        if (argc == 5 && std::string(argv[2]) == "--transport") {
            golden["transport"] = Wire::read(argv[3]);
            RequestPattern pattern;
            pattern.transport = true;
            pattern.bytes = 64;
            pattern.first_finish = 69;
            run_events(golden, "transport-python-wire", std::stoull(argv[4]), pattern);
            return 0;
        }
        if (argc >= 4) {
            golden["pd_path_services"] = Wire::read(argv[2]);
            RequestPattern pattern;
            pattern.bytes = 1;
            pattern.interface_id = "pcie";
            if (argc == 5) {
                require(std::string(argv[4]) == "memory-alias", "unknown interface test mode");
                pattern.first_memory = true;
                pattern.bytes = 4;
                pattern.first_finish = 1;
            }
            run_events(golden, "pd-interface", std::stoull(argv[3]), pattern);
            return 0;
        }
        auto manifest = golden["manifest"];
        manifest.erase("manifest_digest");
        require(Wire::digest(manifest) == golden["manifest"]["manifest_digest"], "Python SHA golden mismatch");
        auto binding = golden["binding"];
        binding.erase("binding_digest");
        binding.erase("activation_id");
        require(Wire::digest(binding) == golden["binding"]["binding_digest"], "Python binding SHA golden mismatch");
        run_events(golden, "independent-gpus", 1024);
        auto same_instance = golden;
        same_instance["binding"]["ranks"][1]["instance_id"] = 0u;
        run_events(same_instance, "one-instance-independent-gpus", 1024);
        run_events(shared(same_instance, "accelerator"), "one-instance-shared-gpu", 2048);
        run_events(shared(golden, "accelerator"), "shared-gpu-two-instances", 2048);
        run_events(shared(golden, "node"), "shared-node-pool", 2048);
        auto global = shared(golden, "global");
        global["binding"]["ranks"][1]["node_id"] = 1u;
        run_events(global, "shared-global-across-nodes", 2048);
        auto nodes = golden;
        nodes["binding"]["ranks"][1]["node_id"] = 1u;
        for (uint32_t rank = 0; rank < 2; ++rank) {
            nodes["binding"]["resources"][rank]["owner_kind"] = "node";
            nodes["binding"]["resources"][rank]["owner_id"] = std::to_string(rank);
        }
        run_events(nodes, "separate-node-local-pools", 1024);
        auto serial = shared(golden, "accelerator");
        directional(serial, false);
        run_events(serial, "shared-turnaround", 2055, RequestPattern{MemoryOperation::Write});
        directional(serial, true);
        run_events(serial, "simultaneous-static-caps", 1024, RequestPattern{MemoryOperation::Write});
        bool zero_rejected = false;
        try {
            run_events(shared(golden, "accelerator"), "zero-bytes", 0,
                       RequestPattern{MemoryOperation::Read, 0, 0});
        } catch (const std::invalid_argument& error) {
            zero_rejected = std::string(error.what()).find("bytes must be positive") != std::string::npos;
        }
        require(zero_rejected, "zero-byte access must be rejected");
        bool queued_overflow_rejected = false;
        try {
            run_events(shared(golden, "accelerator"), "queued-overflow", 0,
                       RequestPattern{MemoryOperation::Read, 0, 1024,
                                      std::numeric_limits<uint64_t>::max() - 512});
        } catch (const std::overflow_error&) {
            queued_overflow_rejected = true;
        }
        require(queued_overflow_rejected, "asynchronous queued overflow was swallowed");
        nonzero_logical_device(golden);
        logical_alias(golden);
        transport_events(golden);
        transport_contracts(golden);
    } catch (const std::exception& error) {
        std::cerr << "physical service event test failed: " << error.what() << std::endl;
        return 1;
    }
    return 0;
}
