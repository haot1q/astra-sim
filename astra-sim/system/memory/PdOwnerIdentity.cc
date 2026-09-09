/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PdOwnerIdentity.hh"

#include <algorithm>
#include <map>
#include <set>
#include "PdPathWire.hh"
#include "PipelinePreparationIdentity.hh"

namespace AstraSim {
namespace {
using namespace PdPathWire;
namespace Wire = ServiceBindingJson;

uint64_t product(uint64_t left, uint64_t right) {
    require(left == 0 || right <= UINT64_MAX / left, "P/D pipeline byte geometry overflow");
    return left * right;
}
}

void validate_pd_owner_identity(const Json& identity, const PhysicalServiceConfig& physical,
                                const std::vector<uint32_t>& ranks) {
    Wire::fields(identity, {"backend_instance_id", "backend_node_id", "registry_instance_id",
                           "registry_node_id", "page_instance_id"});
    const auto instance = Wire::uint32(identity.at("backend_instance_id"));
    const auto node = Wire::uint32(identity.at("backend_node_id"));
    text(identity.at("registry_instance_id"));
    text(identity.at("registry_node_id"));
    if (!identity.at("page_instance_id").is_null()) text(identity.at("page_instance_id"));
    std::set<uint32_t> expected;
    for (const auto& [rank, owner] : physical.ranks) {
        if (owner.at("instance_id") == instance) {
            require(owner.at("node_id") == node, "P/D owner backend node mismatch");
            expected.insert(rank);
        }
    }
    require(!expected.empty() && expected == std::set<uint32_t>(ranks.begin(), ranks.end()) &&
            expected.size() == ranks.size(), "P/D owner requires every actual instance rank");
}

void validate_pd_context_owner(const Json& endpoint, bool mapped,
    const PhysicalServiceConfig& physical, const std::vector<uint32_t>& ranks) {
    if (!mapped) {
        Wire::fields(endpoint, {"endpoint", "node_id", "manifest", "capabilities", "catalogs"});
        return;
    }
    Wire::fields(endpoint, {"endpoint", "node_id", "manifest", "capabilities", "catalogs", "owner_identity"});
    const auto& identity = endpoint.at("owner_identity");
    validate_pd_owner_identity(identity, physical, ranks);
    require(identity.at("backend_instance_id") == endpoint.at("endpoint").at("instance_id") &&
            identity.at("registry_node_id") == endpoint.at("node_id"), "P/D endpoint owner map mismatch");
    const auto& catalogs = Wire::array(endpoint.at("catalogs"));
    const bool page = endpoint.at("manifest").contains("page_tiering") &&
                      !endpoint.at("manifest").at("page_tiering").is_null();
    require(page == !identity.at("page_instance_id").is_null() && page == !catalogs.empty(),
            "P/D owner Page mode mismatch");
    if (!page) return;
    Json geometry;
    for (const auto* name : {"model_id", "block_size_tokens", "kv_dim", "num_layers", "kv_dtype_bytes",
                            "num_npus", "tp_size", "pp_size", "attention_layout"}) {
        geometry[name] = endpoint.at("endpoint").at(name);
    }
    geometry["instance_id"] = identity.at("page_instance_id");
    std::set<uint32_t> observed;
    for (const auto& catalog : catalogs) {
        require(catalog.at("registry_instance_id") == identity.at("registry_instance_id") &&
                catalog.at("registry_node_id") == identity.at("registry_node_id") &&
                catalog.at("geometry") == geometry, "P/D catalog owner/geometry map mismatch");
        require(observed.insert(Wire::uint32(catalog.at("global_rank"))).second,
                "P/D duplicate owner catalog rank");
    }
    require(observed == std::set<uint32_t>(ranks.begin(), ranks.end()), "P/D owner catalogs omit actual ranks");
}

std::vector<uint32_t> validate_pd_pipeline_stage(const Json& stage, const Json& endpoint,
                                               const PhysicalServiceConfig& physical) {
    const auto pp = number(endpoint.at("pp_size"), true);
    require(pp > 1 && number(endpoint.at("num_layers"), true) % pp == 0,
            "P/D pipeline topology must be complete and divisible");
    return validate_pipeline_stage_identity(stage, endpoint, physical).ranks;
}
void validate_pd_pipeline_context(const Json& view, const PhysicalServiceConfig& physical,
                                  const std::vector<uint32_t>& ranks) {
    Wire::fields(view, {"endpoint", "node_id", "manifest", "capabilities", "catalogs",
                       "owner_identity", "pipeline_stages"});
    const auto& endpoint = view.at("endpoint");
    const auto& stages = Wire::array(view.at("pipeline_stages"));
    require(stages.size() == number(endpoint.at("pp_size"), true) && !stages.empty() &&
            stages.front().at("owner_identity") == view.at("owner_identity") &&
            view.at("manifest").contains("page_tiering") && !view.at("manifest").at("page_tiering").is_null(),
            "P/D pipeline requires all original stage owners and Page mode");
    std::set<std::string> registry_ids, page_ids;
    std::map<uint32_t, Json> geometries, identities;
    std::map<uint32_t, uint32_t> groups;
    for (size_t index = 0; index < stages.size(); ++index) {
        const auto& stage = stages[index];
        const auto& owner = stage.at("owner_identity");
        const auto selected = validate_pd_pipeline_stage(stage, endpoint, physical);
        require(number(stage.at("binding").at("stage_id")) == index &&
                owner.at("registry_node_id") == view.at("node_id") &&
                registry_ids.insert(text(owner.at("registry_instance_id"))).second &&
                page_ids.insert(text(owner.at("page_instance_id"))).second,
                "P/D duplicate or out-of-order stage owner");
        Json geometry;
        for (const auto* name : {"model_id", "block_size_tokens", "kv_dim", "kv_dtype_bytes",
                                "tp_size", "attention_layout"}) geometry[name] = endpoint.at(name);
        geometry["instance_id"] = owner.at("page_instance_id");
        geometry["num_layers"] = number(endpoint.at("num_layers")) / stages.size();
        geometry["num_npus"] = endpoint.at("tp_size");
        geometry["pp_size"] = 1;
        for (const auto rank : selected) {
            require(geometries.emplace(rank, geometry).second, "P/D duplicate stage rank");
            identities[rank] = owner;
            groups[rank] = index;
        }
    }
    std::set<uint32_t> observed;
    std::map<uint32_t, Json> normalized;
    for (const auto& catalog : Wire::array(view.at("catalogs"))) {
        const auto rank = Wire::uint32(catalog.at("global_rank"));
        require(geometries.count(rank) && observed.insert(rank).second,
                "P/D pipeline catalog rank is unknown or duplicate");
        const auto& owner = identities.at(rank);
        require(catalog.at("schema_version") == "pd-kv-owner-snapshot-v1" &&
                catalog.at("rank_projection") == "per_rank_normalized_v1" &&
                catalog.at("registry_instance_id") == owner.at("registry_instance_id") &&
                catalog.at("registry_node_id") == owner.at("registry_node_id") &&
                catalog.at("geometry") == geometries.at(rank), "P/D pipeline catalog owner/geometry mismatch");
        auto projection = catalog;
        projection.erase("global_rank");
        const auto group = groups.at(rank);
        require(!normalized.count(group) || normalized.at(group) == projection,
                "P/D TP ranks must share their original stage catalog");
        normalized[group] = std::move(projection);
    }
    require(observed == std::set<uint32_t>(ranks.begin(), ranks.end()),
            "P/D pipeline catalogs omit actual ranks");
}

void validate_pd_descriptor_geometry(const Json& descriptor, const Json& endpoints) {
    const auto& source = endpoints.at(0).at("endpoint");
    const auto& destination = endpoints.at(1).at("endpoint");
    Json layout = {{"profile", "homogeneous_pd_kv_v1"}};
    for (const auto* field : {"model_id", "kv_dtype", "block_size_tokens", "kv_dim", "num_layers",
                             "kv_dtype_bytes", "num_npus", "tp_size", "pp_size", "attention_layout"}) {
        require(source.at(field) == destination.at(field), "P/D pipeline endpoint layout mismatch");
        layout[field] = source.at(field);
    }
    require(descriptor.at("layout_digest") == Wire::digest(layout, true),
            "P/D descriptor layout digest differs from endpoint geometry");
    const auto tp = number(source.at("tp_size"), true);
    const auto pp = number(source.at("pp_size"), true);
    const auto layers = number(source.at("num_layers"), true);
    const auto ranks = number(source.at("num_npus"), true);
    require(ranks == product(tp, pp) && layers % pp == 0, "P/D pipeline partition mismatch");
    const auto token_numerator = product(product(product(2, number(source.at("kv_dim"), true)),
                                                layers), number(source.at("kv_dtype_bytes"), true));
    require(token_numerator % ranks == 0, "P/D pipeline token bytes must divide exactly");
    const auto token_bytes = token_numerator / ranks;
    const auto tokens = number(descriptor.at("initialized_tokens"), true);
    const auto block_tokens = number(source.at("block_size_tokens"), true);
    const auto block_bytes = product(token_bytes, block_tokens);
    const auto count = (tokens - 1) / block_tokens + 1;
    const auto allocated = product(count, block_bytes);
    require(number(descriptor.at("initialized_bytes_per_rank")) == product(tokens, token_bytes) &&
            number(descriptor.at("allocated_bytes_per_rank")) == allocated &&
            number(descriptor.at("charged_bytes_per_rank")) == allocated &&
            number(descriptor.at("charged_bytes_aggregate")) == product(allocated, ranks),
            "P/D pipeline descriptor bytes differ from full block geometry");
    const auto& blocks = Wire::array(descriptor.at("blocks"));
    require(blocks.size() == count, "P/D pipeline descriptor omits framework blocks");
    for (size_t index = 0; index < blocks.size(); ++index) {
        const auto& block = blocks[index];
        const auto start = product(index, block_tokens);
        require(block.at("request_id") == descriptor.at("request_id") &&
                number(block.at("logical_block_index")) == index &&
                number(block.at("token_start")) == start &&
                number(block.at("token_capacity")) == block_tokens &&
                number(block.at("initialized_tokens")) == std::min(block_tokens, tokens - start) &&
                number(block.at("allocated_bytes")) == block_bytes &&
                block.at("geometry_digest") == descriptor.at("layout_digest"),
                "P/D pipeline block geometry differs from descriptor");
    }
    const auto& pairs = Wire::array(descriptor.at("rank_pairs"));
    require(pairs.size() == ranks, "P/D pipeline descriptor omits rank pairs");
    for (size_t offset = 0; offset < pairs.size(); ++offset) {
        const auto& pair = pairs[offset];
        const auto stage = offset / tp;
        require(number(pair.at("source_rank")) == add(number(source.at("first_global_rank")), offset) &&
                number(pair.at("destination_rank")) == add(number(destination.at("first_global_rank")), offset) &&
                number(pair.at("tp_rank")) == offset % tp && number(pair.at("pp_stage")) == stage &&
                number(pair.at("layer_start")) == product(stage, layers / pp) &&
                number(pair.at("layer_end")) == product(stage + 1, layers / pp) &&
                number(pair.at("charged_bytes")) == allocated,
                "P/D pipeline rank pair does not preserve stage/TP/layer/byte ownership");
    }
}
}  // namespace AstraSim
