/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <vector>
#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/PdKvTransferExecutor.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>
#include "protoio.hh"
#include "NativeTagEvents.hh"

namespace {
using namespace AstraSim;
using Json = nlohmann::json;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

void write_json(const std::filesystem::path& path, const Json& value) {
    std::ofstream stream(path);
    stream << value.dump();
    require(stream.good(), "test fixture write failed");
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

void check_collision(PdKvTransferExecutor& executor, const std::filesystem::path& root) {
    Json descriptor = {
        {"schema_version", "pd-kv-transfer-v1"}, {"transport", "astra_analytical_p2p_v1"},
        {"run_id", "tag-test"}, {"attempt_id", "a"}, {"transfer_id", "first"},
        {"request_id", "r"}, {"source_instance_id", 0}, {"destination_instance_id", 1},
        {"charged_bytes_aggregate", 8u},
        {"rank_pairs", {{{"source_rank", 0}, {"destination_rank", 1},
                         {"charged_bytes", 8u}, {"tag", 1000000000}}}},
    };
    const auto submit = [&]() {
        write_json(root / "transfer.json", {{"descriptor", descriptor},
            {"descriptor_digest", ServiceBindingJson::digest(descriptor)}});
        executor.submit((root / "transfer.json").string(), 0);
    };
    submit();
    descriptor["transfer_id"] = "collision";
    descriptor["rank_pairs"][0]["tag"] = 0;
    bool rejected = false;
    try { submit(); } catch (const std::invalid_argument&) { rejected = true; }
    require(rejected, "normalized in-flight tag collision was accepted");
}
}

int main(int argc, char** argv) {
    try {
        require(argc == 2 || argc == 3, "expected physical-service fixture and optional tag mode");
        LoggerFactory::init("empty");
        auto pattern = (std::filesystem::temp_directory_path() / "native-tag-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        require(::mkdtemp(buffer.data()), "mkdtemp failed");
        const std::filesystem::path root(buffer.data());
        const auto fixture = ServiceBindingJson::read(argv[1]);
        write_json(root / "memory.json", fixture["manifest"]);
        write_json(root / "binding.json", fixture["binding"]);
        write_json(root / "system.json", {
            {"scheduling-policy", "LIFO"}, {"endpoint-delay", 0u},
            {"active-chunks-per-dimension", 1u}, {"preferred-dataset-splits", 1u},
            {"collective-optimization", "localBWAware"}, {"local-mem-bw", 1u},
            {"boost-mode", 0u}, {"all-reduce-implementation", {"ring"}},
            {"all-gather-implementation", {"ring"}}, {"reduce-scatter-implementation", {"ring"}},
            {"all-to-all-implementation", {"ring"}},
        });
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
        PhysicalServiceFactory services(memory, (root / "binding.json").string(), 2);
        std::vector<std::unique_ptr<Network>> networks;
        std::vector<std::unique_ptr<Sys>> owned;
        std::vector<Sys*> systems;
        for (int rank = 0; rank < 2; ++rank) {
            empty_et(root, fixture, rank);
            networks.push_back(std::make_unique<Network>(rank));
            owned.push_back(std::make_unique<Sys>(rank, (root / "empty").string(), "empty",
                (root / "system.json").string(), services.at(rank).memory, memory.manifest_digest,
                networks.back().get(), topology->get_npus_count_per_dim(), std::vector<int>{1},
                1.0, 1.0, false, services.at(rank).ucie, services.at(rank).movement));
            systems.push_back(owned.back().get());
        }
        const bool failure_with_transfer = argc == 3 &&
            std::string(argv[2]) == "callback_failure_with_transfer";
        if (argc == 3 && !failure_with_transfer) {
            NativeTagEventsTest::run(argv[2], systems, events);
            std::cout << "PASS native tag event lifecycle" << std::endl;
            return 0;
        }
        PdKvTransferExecutor executor(systems);
        unsigned ordinary_calls = 0;
        if (failure_with_transfer) {
            sim_request request{};
            systems[0]->front_end_sim_send(0, Sys::dummy_data, 8, UINT8, 1, 42, &request,
                Sys::FrontEndSendRecvType::NATIVE, NativeTagEventsTest::fail_callback, nullptr);
            NativeTagEventsTest::post(systems[1], false, 42, 0, &ordinary_calls);
        }
        check_collision(executor, root);
        for (unsigned steps = 0; !events->finished(); ++steps) {
            require(steps < 100, "native tag callbacks did not drain");
            events->proceed();
            executor.rethrow_failure();
        }
        require(executor.drained(), "original transfer did not drain");
        std::cout << "PASS normalized tag collision and original transfer drain" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
