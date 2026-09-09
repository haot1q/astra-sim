/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
        std::cout << "RECEIPT rank=" << handler->sys_id << " logical_device=" << handler->device_id
                  << " ready=" << handler->memory_ready_ns << " start=" << handler->memory_start_ns
                  << " finish=" << handler->memory_finish_ns << std::endl;
    }
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
    PhysicalServiceFactory services(memory, (root / "binding.json").string(), 2, pd_path);
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
    for (int rank = 0; rank < 2; ++rank) {
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
        services.rethrow_failure();
    }
    require(completion.calls == 2, "missing or duplicate memory callback");
    require(requests[0].memory_finish_ns ==
                (pattern.first_finish ? pattern.first_finish : pattern.bytes),
            "first service runtime changed");
    require(requests[1].memory_finish_ns == second_finish, "incorrect physical resource contention");
    require(requests[0].device_id == pattern.device && requests[1].device_id == pattern.device,
            "logical device ID changed");
    std::cout << "PASS " << name << " evidence=" << root << std::endl;
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
}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc != 2 && argc != 4 && argc != 5) {
            throw std::invalid_argument("expected golden fixture path and optional interface request");
        }
        LoggerFactory::init("empty");
        auto golden = Wire::read(argv[1]);
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
    } catch (const std::exception& error) {
        std::cerr << "physical service event test failed: " << error.what() << std::endl;
        return 1;
    }
    return 0;
}
