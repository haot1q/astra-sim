/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>

#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/CommunicatorGroup.hh"
#include "astra-sim/system/DataSet.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>
#include "protoio.hh"

namespace {
void require(bool value) {
    if (!value) throw std::runtime_error("collective scope assertion failed");
}

template <class Function> void rejects(Function function) {
    bool caught = false;
    try { function(); } catch (const std::invalid_argument&) { caught = true; }
    require(caught);
}

void test_identity() {
    using AstraSim::CommunicatorGroup;
    using Identity = CommunicatorGroup::ScopeIdentity;
    std::set<int> tags;
    for (uint64_t group = 0; group < 3; ++group) {
        CommunicatorGroup scope(Identity{group, 3}, {0, 1});
        require(scope.identity_only());
        scope.require_streams(4);
        for (uint64_t sequence = 0; sequence < 4; ++sequence) {
            const auto expected = CommunicatorGroup::scoped_stream_id(
                Identity{group, 3}, sequence);
            require(scope.next_stream_id() == expected);
            require(tags.insert(expected).second);
        }
    }
    constexpr uint64_t width = 500000000;
    require(CommunicatorGroup::scoped_stream_id(Identity{2, 3},
                (width - 3) / 3) == 499999997);
    CommunicatorGroup last(Identity{width - 1, width}, {0});
    last.require_streams(1);
    rejects([&] { last.require_streams(2); });
    require(last.next_stream_id() == width - 1);
    rejects([&] { last.next_stream_id(); });
    rejects([&] { last.require_streams(1); });
    rejects([&] { CommunicatorGroup::scoped_stream_id(Identity{0, 0}, 0); });
    rejects([&] { CommunicatorGroup::scoped_stream_id(Identity{3, 3}, 0); });
    rejects([&] { CommunicatorGroup::scoped_stream_id(Identity{0, width + 1}, 0); });
    rejects([&] { CommunicatorGroup::scoped_stream_id(
        Identity{0, 3}, std::numeric_limits<uint64_t>::max()); });
    rejects([&] { CommunicatorGroup invalid(Identity{0, 1}, {}); });
    rejects([&] { CommunicatorGroup invalid(Identity{0, 1}, {0, 0}); });
    rejects([&] { CommunicatorGroup invalid(Identity{0, 1}, {-1, 0}); });
}

using Json = nlohmann::json;
using Network = AstraSimAnalyticalCongestionUnaware::CongestionUnawareNetworkApi;

void write_json(const std::filesystem::path& path, const Json& value) {
    std::ofstream stream(path);
    stream << value.dump();
    require(stream.good());
}

void write_et(const std::filesystem::path& root, const Json& fixture, int rank) {
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

std::shared_ptr<ChakraProtoMsg::Node> collective() {
    auto node = std::make_shared<ChakraProtoMsg::Node>();
    node->set_id(1);
    node->set_type(ChakraProtoMsg::COMM_COLL_NODE);
    auto* attr = node->add_attr();
    attr->set_name("pg_name"); attr->set_string_val("world");
    attr = node->add_attr(); attr->set_name("involved_dim");
    attr->mutable_bool_list()->add_values(true);
    attr = node->add_attr(); attr->set_name("comm_type");
    attr->set_int64_val(ChakraProtoMsg::ALL_REDUCE);
    attr = node->add_attr(); attr->set_name("comm_size"); attr->set_int64_val(64);
    return node;
}

void reject_before_occupation(AstraSim::Sys& sys) {
    auto& resource = *sys.workload->hw_resource;
    for (int damage = 0; damage < 8; ++damage) {
        auto raw = collective();
        if (damage == 0) raw->mutable_attr(0)->set_string_val("unknown");
        if (damage == 1) raw->mutable_attr(0)->set_int64_val(1);
        if (damage == 2) *raw->add_attr() = raw->attr(0);
        if (damage == 3) raw->mutable_attr()->DeleteSubrange(0, 1);
        if (damage == 4) raw->mutable_attr(1)->set_string_val("wrong");
        if (damage == 5) raw->mutable_attr(1)->mutable_bool_list()->set_values(0, false);
        if (damage == 6) *raw->add_attr() = raw->attr(1);
        if (damage == 7) raw->mutable_attr(3)->set_int64_val(0);
        auto node = std::make_shared<Chakra::ETFeederNode>(raw);
        rejects([&] { sys.workload->issue_comm(node); });
        require(resource.num_in_flight_gpu_comm_ops == 0 && resource.num_gpu_comms == 0);
        require(sys.num_streams == 0 && sys.ready_list.empty() && sys.event_queue.empty());
    }
}

void test_preflight(const char* fixture_path) {
    using namespace AstraSim;
    LoggerFactory::init("empty");
    auto pattern = (std::filesystem::temp_directory_path() / "scope-test-XXXXXX").string();
    std::vector<char> buffer(pattern.begin(), pattern.end()); buffer.push_back('\0');
    require(::mkdtemp(buffer.data()));
    const std::filesystem::path root(buffer.data());
    const auto fixture = ServiceBindingJson::read(fixture_path);
    write_json(root / "memory.json", fixture["manifest"]);
    write_json(root / "binding.json", fixture["binding"]);
    write_json(root / "scopes.json", {{"schema", "collective-scopes-v1"},
        {"groups", {{"world", {0, 1}}}}});
    write_json(root / "system.json", {
        {"scheduling-policy", "LIFO"}, {"endpoint-delay", 0u}, {"active-chunks-per-dimension", 1u},
        {"preferred-dataset-splits", 1u}, {"collective-optimization", "localBWAware"},
        {"local-mem-bw", 1u}, {"boost-mode", 0u}, {"all-reduce-implementation", {"ring"}},
        {"all-gather-implementation", {"ring"}}, {"reduce-scatter-implementation", {"ring"}},
        {"all-to-all-implementation", {"ring"}},
    });
    { std::ofstream network(root / "network.yml");
      network << "topology: [FullyConnected]\nnpus_count: [2]\nbandwidth: [1000.0]\nlatency: [0.0]\n"; }
    auto events = std::make_shared<NetworkAnalytical::EventQueue>();
    auto topology = NetworkAnalyticalCongestionUnaware::construct_topology(
        NetworkAnalytical::NetworkParser((root / "network.yml").string()));
    Network::set_event_queue(events); Network::set_topology(topology);
    const auto memory = load_memory_tier_config((root / "memory.json").string());
    PhysicalServiceFactory services(memory, (root / "binding.json").string(), 2);
    std::vector<std::unique_ptr<Network>> networks;
    std::vector<std::unique_ptr<Sys>> systems;
    for (int rank = 0; rank < 2; ++rank) {
        write_et(root, fixture, rank);
        networks.push_back(std::make_unique<Network>(rank));
        systems.push_back(std::make_unique<Sys>(rank, (root / "empty").string(),
            (root / "scopes.json").string(), (root / "system.json").string(),
            services.at(rank).memory, memory.manifest_digest, networks.back().get(),
            topology->get_npus_count_per_dim(), std::vector<int>{1}, 1.0, 1.0, false,
            services.at(rank).ucie, services.at(rank).movement));
    }
    reject_before_occupation(*systems[0]);
    systems[0]->inter_dimension_scheduling = InterDimensionScheduling::RoundRobin;
    rejects([&] { systems[0]->workload->initialize_comm_group((root / "scopes.json").string()); });
    systems[0]->inter_dimension_scheduling = InterDimensionScheduling::Ascending;
    std::vector<std::unique_ptr<CommunicatorGroup>> scopes;
    std::vector<std::unique_ptr<DataSet>> datasets;
    for (auto& sys : systems) {
        sys->preferred_dataset_splits = 2;
        scopes.push_back(std::make_unique<CommunicatorGroup>(
            CommunicatorGroup::ScopeIdentity{0, 1}, std::vector<int>{0, 1}));
        datasets.emplace_back(sys->generate_all_reduce(64, {true}, scopes.back().get(), 0));
        require(sys->num_streams == 2 && scopes.back()->next_stream_id() == 2);
    }
    for (unsigned step = 0; !events->finished(); ++step) {
        require(step < 1000);
        events->proceed();
    }
    for (const auto& dataset : datasets) require(dataset->is_finished());
}
}  // namespace

int main(int argc, char** argv) {
    try {
        require(argc == 1 || argc == 2);
        test_identity();
        if (argc == 2) test_preflight(argv[1]);
        std::cout << "PASS collective scope identity boundaries" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
