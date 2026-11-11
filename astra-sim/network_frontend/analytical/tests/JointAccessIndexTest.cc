/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/JointAccessIndex.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/system/MemoryTierRegistry.hh"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
#include <sys/stat.h>

namespace {
using AstraSim::JointAccessIndex;
using AstraSim::JointSourceContext;
using Json = nlohmann::json;

void require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

template <class Function>
void rejected(Function operation) {
    bool failed = false;
    try { operation(); } catch (const std::exception&) { failed = true; }
    require(failed, "invalid association accepted");
}

void write(const std::filesystem::path& path, const std::string& bytes) {
    std::ofstream output(path, std::ios::binary);
    output << bytes;
    require(output.good(), "fixture write failed");
}

std::string frame(const google::protobuf::Message& message) {
    std::string result;
    auto bytes = message.SerializeAsString();
    auto length = bytes.size();
    do {
        auto byte = static_cast<unsigned char>(length & 127);
        length >>= 7;
        result.push_back(static_cast<char>(byte | (length ? 128 : 0)));
    } while (length);
    return result + bytes;
}

Json context() {
    return {{"schema", "joint-source-context-v1"}, {"mode", "association_only"},
        {"run_id", "test-run"}, {"source_provenance_digest", std::string(64, 'a')},
        {"source_profile", "native-source-prefix-v1"},
        {"source_selection_sha256", std::string(64, 'b')}, {"ranks", {"0", "1"}},
        {"limits", {{"context_bytes", "65536"}, {"association_bytes", "67108864"},
            {"json_depth", "32"}, {"string_bytes", "512"},
            {"table_records", "262144"}, {"pieces", "1048576"},
            {"et_bytes", "268435456"}, {"frame_bytes", "4194304"},
            {"et_nodes", "1000000"}, {"pending_graphs_per_rank", "2"},
            {"live_index_charge_bytes", "268435456"}, {"live_pieces", "1048576"}}}};
}

Json companion(const Json& input) {
    return {{"schema", "joint-access-companion-v1"}, {"kind", "bootstrap"},
        {"execution", {{"run_id", input.at("run_id")},
            {"source_provenance_digest", input.at("source_provenance_digest")},
            {"execution_iteration", "0"}, {"rank", "0"}}},
        {"source_profile", input.at("source_profile")},
        {"source_selection_sha256", input.at("source_selection_sha256")},
        {"control", {{"duration_ns", "1"}}}};
}

std::string header(const std::string& attachment, bool duplicate = false) {
    ChakraProtoMsg::GlobalMetadata metadata;
    auto* version = metadata.add_attr();
    version->set_name("joint_association_version"); version->set_string_val("1");
    auto* hash = metadata.add_attr();
    hash->set_name("joint_companion_sha256");
    hash->set_string_val(AstraSim::ServiceBindingJson::digest_bytes(attachment).substr(7));
    if (duplicate) *metadata.add_attr() = *version;
    return frame(metadata);
}

std::string et(const std::string& attachment, bool duplicate = false) {
    ChakraProtoMsg::Node event;
    event.set_id(0); event.set_name("COMP_NODE_event_1ns");
    event.set_type(ChakraProtoMsg::COMP_NODE); event.set_duration_micros(1);
    return header(attachment, duplicate) + frame(event);
}

class NoServiceMemory final : public AstraSim::AstraMemoryAPI {
  public:
    void set_sys(int, AstraSim::Sys*) override {}
    void issue(const AstraSim::MemoryRequest&, AstraSim::WorkloadLayerHandlerData*) override {
        throw std::runtime_error("association validation invoked memory service");
    }
    AstraSim::MemoryLocationType get_memory_location_type() const override {
        return AstraSim::MemoryLocationType::LOCAL_MEMORY;
    }
};

Json business(const Json& input) {
    auto body = companion(input);
    body.erase("control"); body["kind"] = "business";
    body["execution"]["execution_iteration"] = "1";
    body["association_input_sha256"] = std::string(64, 'c');
    body["invocations"] = {{{"ref", "0"}, {"batch_id", "batch"}, {"invocation_id", "call"}}};
    body["backings"] = {{{"ref", "0"}, {"backing_identity", {{"profile_id", "profile"},
        {"owner_scope", "scope"}, {"allocation_id", "allocation"}}},
        {"extent_bytes", "16"}, {"catalog_generation", nullptr}}};
    body["operands"] = {{{"ref", "0"}, {"invocation_ref", "0"}, {"source_boundary", "original"},
        {"execution_layer", "0"}, {"role", "weight"}, {"origin", "reads"}, {"operation", "read"},
        {"ordinal_start", "0"}, {"backing_ref", "0"}, {"offset_bytes", "4"},
        {"length_bytes", "8"}, {"stride_bytes", "0"}, {"count", "2"}}};
    body["pieces"] = Json::array();
    for (unsigned i = 0; i < 4; ++i) {
        body["pieces"].push_back({{"ref", std::to_string(i)}, {"operand_ref", "0"},
            {"run_index", std::to_string(i / 2)}, {"offset_bytes", i % 2 ? "8" : "4"},
            {"length_bytes", "4"}, {"location", i % 2 ? "lpddr:0" : "hbm:0"},
            {"authorization", {{"kind", "fixed_weight"}, {"owner", "original-owner"},
                {"page_id", nullptr}, {"residency_version", nullptr}}}, {"volume_ordinal", nullptr}});
    }
    body["bindings"] = Json::array();
    for (unsigned i = 0; i < 2; ++i) {
        body["bindings"].push_back({{"ref", "r0/row7/read/s" + std::to_string(i)},
            {"final_node_id", std::to_string(42 + i)}, {"node_type", "MEM_LOAD_NODE"},
            {"row", "7"}, {"operation", "read"}, {"segment_index", std::to_string(i)},
            {"location", i ? "lpddr:0" : "hbm:0"}, {"bytes", "8"},
            {"piece_refs", {std::to_string(i), std::to_string(i + 2)}}});
    }
    body["source_unassociated"] = Json::array();
    body["memory_unassociated"] = {{{"final_node_id", "99"}, {"node_type", "MEM_LOAD_NODE"},
        {"location", "hbm:0"}, {"bytes", "0"}, {"reason", "outside_selected_source"}}};
    return body;
}

ChakraProtoMsg::Node memory(unsigned id, unsigned tier, unsigned bytes, const std::string& ref) {
    ChakraProtoMsg::Node node;
    node.set_id(id); node.set_name("original-memory"); node.set_type(ChakraProtoMsg::MEM_LOAD_NODE);
    auto* location = node.add_attr(); location->set_name("tensor_loc"); location->set_uint32_val(tier);
    auto* device = node.add_attr(); device->set_name("tensor_device"); device->set_uint32_val(0);
    auto* size = node.add_attr(); size->set_name("tensor_size"); size->set_uint64_val(bytes);
    if (!ref.empty()) { auto* attr = node.add_attr(); attr->set_name("joint_access_ref"); attr->set_string_val(ref); }
    return node;
}

std::string business_et(const Json& body) {
    return header(body.dump()) + frame(memory(42, 1, 8, "r0/row7/read/s0")) +
        frame(memory(43, 2, 8, "r0/row7/read/s1")) + frame(memory(99, 1, 0, ""));
}

void business_contracts(const std::filesystem::path& directory) {
    auto raw = context();
    const auto context_path = directory / "business-context.json";
    const auto graph = directory / "business.0.et";
    const auto attachment = graph.string() + ".joint-access.json";
    write(context_path, raw.dump());
    auto run = JointSourceContext::load(context_path.string());
    NoServiceMemory memory_api;
    AstraSim::MemoryTierRegistry tiers({{1, "hbm", 1, &memory_api, {}}, {2, "lpddr", 1, &memory_api, {}}});
    const auto good = business(raw);
    auto publish = [&](const Json& body) { write(attachment, body.dump()); write(graph, business_et(body)); };
    publish(good);
    auto index = JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, &tiers);
    require(index->binding_count() == 2 && run->live_piece_count() == 4, "split/repeated access lost");
    auto node = memory(42, 1, 8, "r0/row7/read/s0");
    require(index->lookup(node, 0, 1)->binding->at("piece_refs").size() == 2, "aggregation lookup");
    rejected([&] { index->lookup(node, 0, 2); });
    rejected([&] { index->lookup(memory(42, 2, 8, "r0/row7/read/s0"), 0, 1); });
    auto check_bad = [&](Json body) {
        publish(body);
        rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, &tiers); });
    };
    auto bad = good; bad["pieces"][1]["offset_bytes"] = "7"; check_bad(bad);
    bad = good; bad["bindings"][0]["piece_refs"] = {"0", "0"}; check_bad(bad);
    bad = good; bad["bindings"][0]["bytes"] = "9"; check_bad(bad);
    bad = good; bad["operands"][0]["stride_bytes"] = "-8"; check_bad(bad);
    bad = good; bad["backings"][0]["extent_bytes"] = "11"; check_bad(bad);
    bad = good; bad["execution"]["run_id"] = "foreign"; check_bad(bad);
    bad = good; bad["operands"][0]["count"] = "18446744073709551615"; check_bad(bad);
    bad = good; bad["pieces"][0]["ref"] = "00"; check_bad(bad);
    bad = good; bad["pieces"][0]["authorization"]["kind"] = "unverified"; check_bad(bad);
    bad = good; bad["memory_unassociated"] = Json::array(); check_bad(bad);
    bad = good; bad["bindings"][0]["final_node_id"] = "52"; check_bad(bad);
    bad = good; bad["pieces"][0]["length_bytes"] = true; check_bad(bad);
    bad = good; bad["source_unassociated"] = {{{"piece_ref", "0"}, {"reason", "outside_selected_segment"}}}; check_bad(bad);
    auto paged = good;
    paged["operands"][0]["origin"] = "captured_page_reads";
    for (auto& piece : paged["pieces"]) {
        piece["authorization"] = {{"kind", "page"}, {"owner", "original-owner"},
            {"page_id", "page-0"}, {"residency_version", "0"}};
        piece["volume_ordinal"] = "0";
    }
    publish(paged);
    auto page_index = JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, &tiers);
    require(page_index->binding_count() == 2, "captured Page access rejected");
    page_index.reset();
    bad = paged; bad["pieces"][0]["volume_ordinal"] = nullptr; check_bad(bad);
    bad = paged; bad["pieces"][0]["authorization"] = good["pieces"][0]["authorization"]; check_bad(bad);
    auto reversed = good;
    reversed["operands"][0]["offset_bytes"] = "8";
    reversed["operands"][0]["stride_bytes"] = "-4";
    reversed["pieces"][0]["offset_bytes"] = "8";
    reversed["pieces"][1]["offset_bytes"] = "12";
    publish(reversed);
    auto negative_stride = JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, &tiers);
    require(negative_stride->binding_count() == 2, "valid signed stride rejected");
    negative_stride.reset();
    publish(good);
    const auto charge = run->live_charge_bytes();
    raw["limits"]["live_index_charge_bytes"] = std::to_string(charge);
    write(context_path, raw.dump());
    auto bounded = JointSourceContext::load(context_path.string());
    auto first = JointAccessIndex::load_and_validate(graph.string(), bounded, 0, 1, &tiers);
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), bounded, 0, 1, &tiers); });
    first.reset(); require(bounded->live_charge_bytes() == 0, "index retirement leaked admission charge");
    first = JointAccessIndex::load_and_validate(graph.string(), bounded, 0, 1, &tiers);
    write(graph, business_et(good) + "x");
    rejected([&] { first->verify_file_identity(graph.string()); });
    index.reset(); require(run->live_charge_bytes() == 0 && run->live_piece_count() == 0, "failed loads leaked charges");
}

void parent_symlink_contracts(const std::filesystem::path& directory) {
    const auto real = directory / "real-inputs";
    const auto alias = directory / "linked-inputs";
    std::filesystem::create_directory(real);
    std::filesystem::create_directory_symlink(real, alias);
    const auto raw = context();
    const auto body = companion(raw);
    write(real / "context.json", raw.dump());
    write(real / "graph.et.joint-access.json", body.dump());
    write(real / "graph.et", et(body.dump()));
    auto run = JointSourceContext::load((real / "context.json").string());
    auto index = JointAccessIndex::load_and_validate((real / "graph.et").string(), run, 0, 0, nullptr);
    require(index->binding_count() == 0, "parent symlink fixture must otherwise be valid");
    rejected([&] { JointSourceContext::load((alias / "context.json").string()); });
    rejected([&] { JointAccessIndex::load_and_validate((alias / "graph.et").string(), run, 0, 0, nullptr); });
}

void collective_contracts(const std::filesystem::path& directory) {
    const auto raw = context();
    const auto context_path = directory / "collective-context.json";
    const auto graph = directory / "collective.0.et";
    write(context_path, raw.dump());
    auto run = JointSourceContext::load(context_path.string());
    auto body = companion(raw);
    body["kind"] = "dep_finish";
    body["execution"]["execution_iteration"] = "1";
    body["control"] = {{"rank_group", {"0", "1"}}, {"payload_bytes", "8"},
        {"involved_dim", {true}}, {"pg_name", nullptr}};
    ChakraProtoMsg::Node node;
    node.set_id(0); node.set_name("COMM_COLL_NODE_dep_finish_ALLREDUCE");
    node.set_type(ChakraProtoMsg::COMM_COLL_NODE);
    auto* type = node.add_attr(); type->set_name("comm_type"); type->set_int64_val(ChakraProtoMsg::ALL_REDUCE);
    auto* size = node.add_attr(); size->set_name("comm_size"); size->set_int64_val(8);
    auto* dims = node.add_attr(); dims->set_name("involved_dim"); dims->mutable_bool_list()->add_values(true);
    auto publish = [&] {
        write(graph.string() + ".joint-access.json", body.dump());
        write(graph, header(body.dump()) + frame(node));
    };
    publish();
    auto index = JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, nullptr);
    require(index->binding_count() == 0, "collective unexpectedly has memory bindings");
    body["control"]["rank_group"] = {"0"}; publish();
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, nullptr); });
}

void contracts(const std::filesystem::path& directory) {
    auto raw = context();
    const auto path = directory / "context.json";
    const auto graph = directory / "test.0.et";
    const auto attachment = graph.string() + ".joint-access.json";
    write(path, raw.dump());
    auto run = JointSourceContext::load(path.string());
    auto body = companion(raw);
    write(attachment, body.dump()); write(graph, et(body.dump()));
    auto index = JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr);
    require(index->binding_count() == 0, "control unexpectedly has source accesses");
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 1, 0, nullptr); });
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 1, nullptr); });
    rejected([&] { JointAccessIndex::probe_legacy(graph.string()); });
    write(graph, et(body.dump(), true));
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    write(graph, et(body.dump()) + std::string("\x80", 1));
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    write(graph, et(body.dump()).substr(0, et(body.dump()).size() - 1));
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    write(graph, std::string("\x1f\x8b\x08\x00", 4));
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    write(graph, et(body.dump()));
    std::filesystem::remove(attachment);
    require(mkfifo(attachment.c_str(), 0600) == 0, "FIFO fixture failed");
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    std::filesystem::remove(attachment);
    std::filesystem::create_symlink(path, attachment);
    rejected([&] { JointAccessIndex::load_and_validate(graph.string(), run, 0, 0, nullptr); });
    std::filesystem::remove(attachment);
    raw["ranks"] = {"0", "0"}; write(path, raw.dump());
    rejected([&] { JointSourceContext::load(path.string()); });
    raw = context(); raw["limits"]["json_depth"] = "1"; write(path, raw.dump());
    rejected([&] { JointSourceContext::load(path.string()); });
    raw = context(); raw["limits"]["string_bytes"] = "32"; write(path, raw.dump());
    rejected([&] { JointSourceContext::load(path.string()); });
    raw = context(); raw["limits"]["pieces"] = "1048577"; write(path, raw.dump());
    rejected([&] { JointSourceContext::load(path.string()); });
    raw = context(); raw["source_profile"] = "unknown"; write(path, raw.dump());
    rejected([&] { JointSourceContext::load(path.string()); });
    write(path, "{\"schema\":\"joint-source-context-v1\",\"schema\":\"other\"}");
    rejected([&] { JointSourceContext::load(path.string()); });
}
}  // namespace

int main() {
    auto directory = std::filesystem::temp_directory_path() /
        ("astra-joint-access-" + std::to_string(getpid()));
    std::filesystem::create_directory(directory);
    try {
        contracts(directory);
        business_contracts(directory);
        collective_contracts(directory);
        parent_symlink_contracts(directory);
        std::filesystem::remove_all(directory);
        std::cout << "JointAccessIndex contracts passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
