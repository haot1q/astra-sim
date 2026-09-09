/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PipelinePreparationIdentity.hh"

#include <set>
#include <algorithm>
#include <stdexcept>
#include "PhysicalServiceFactory.hh"
#include "ServiceBindingJson.hh"

namespace AstraSim {
namespace {
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;
void require(bool condition) {
    if (!condition) throw std::invalid_argument("pipeline preparation stage/owner/layout mismatch");
}
void text(const Json& value) {
    require(value.is_string() && !value.get<std::string>().empty());
}
}

PipelinePreparationIdentity validate_pipeline_preparation_identity(
    const Json& work, const PhysicalServiceFactory& services) {
    const auto& stage = work.at("pipeline_stage");
    const auto& endpoint = work.at("endpoint");
    const auto& identity = stage.at("owner_identity");
    require(Wire::uint32(work.at("backend_instance_id")) == Wire::uint32(endpoint.at("instance_id")) &&
            identity.at("registry_instance_id") == work.at("instance_id"));
    return validate_pipeline_stage_identity(stage, endpoint, services.configuration());
}

PipelinePreparationIdentity validate_pipeline_stage_identity(
    const Json& stage, const Json& endpoint, const PhysicalServiceConfig& physical) {
    Wire::fields(stage, {"binding", "owner_identity"});
    const auto& binding = stage.at("binding");
    const auto& identity = stage.at("owner_identity");
    Wire::fields(binding, {"stage_id", "layer_start", "layer_end", "ranks",
                           "registry_instance_id", "page_instance_id"});
    Wire::fields(identity, {"backend_instance_id", "backend_node_id", "registry_instance_id",
                            "registry_node_id", "page_instance_id"});
    Wire::fields(endpoint, {"instance_id", "first_global_rank", "model_id", "kv_dtype",
        "block_size_tokens", "kv_dim", "num_layers", "kv_dtype_bytes", "num_npus",
        "tp_size", "pp_size", "attention_layout"});
    for (const auto* key : {"model_id", "kv_dtype", "attention_layout"}) text(endpoint.at(key));
    for (const auto* key : {"registry_instance_id", "registry_node_id", "page_instance_id"}) text(identity.at(key));
    for (const auto* key : {"block_size_tokens", "kv_dim", "kv_dtype_bytes"}) require(Wire::uint32(endpoint.at(key)) > 0);
    const auto instance = Wire::uint32(endpoint.at("instance_id"));
    const auto node = Wire::uint32(identity.at("backend_node_id"));
    require(Wire::uint32(identity.at("backend_instance_id")) == instance &&
            binding.at("registry_instance_id") == identity.at("registry_instance_id") &&
            binding.at("page_instance_id") == identity.at("page_instance_id"));
    const auto tp = Wire::uint32(endpoint.at("tp_size"));
    const auto pp = Wire::uint32(endpoint.at("pp_size"));
    const auto layers = Wire::uint32(endpoint.at("num_layers"));
    const auto count = Wire::uint32(endpoint.at("num_npus"));
    const auto first = Wire::uint32(endpoint.at("first_global_rank"));
    const auto stage_id = Wire::uint32(binding.at("stage_id"));
    require(tp > 0 && pp > 0 && layers >= pp && stage_id < pp &&
            static_cast<uint64_t>(tp) * pp == count &&
            static_cast<uint64_t>(first) + count <= physical.ranks.size());
    const auto remainder = layers % pp;
    const auto first_extra = pp - remainder - 1;
    const auto boundary = [&](uint32_t stage) {
        const auto extras = stage > first_extra ? std::min(stage - first_extra, remainder) : 0;
        return stage * (layers / pp) + extras;
    };
    require(Wire::uint32(binding.at("layer_start")) == boundary(stage_id) &&
            Wire::uint32(binding.at("layer_end")) == boundary(stage_id + 1));
    std::set<uint32_t> actual;
    for (const auto& [rank, owner] : physical.ranks) {
        if (owner.at("instance_id") == instance) {
            require(owner.at("node_id") == node && first <= rank && rank < first + count);
            actual.insert(rank);
        }
    }
    require(actual.size() == count);
    const auto& ranks = Wire::array(binding.at("ranks"));
    require(ranks.size() == tp);
    PipelinePreparationIdentity result;
    for (uint32_t index = 0; index < tp; ++index) {
        const auto rank = Wire::uint32(ranks.at(index));
        require(rank == first + stage_id * tp + index && actual.count(rank));
        result.ranks.push_back(rank);
    }
    result.digest = Wire::digest(Json{{"pipeline_stage", stage}, {"endpoint", endpoint}}, true);
    return result;
}
}
