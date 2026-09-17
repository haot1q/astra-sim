/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/TemplateWorkloadFeeder.hh"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace AstraSim {
namespace {

using Json = nlohmann::json;
using Node = ChakraProtoMsg::Node;
Json resolve(
    const Json& expression,
    const std::unordered_map<std::string, Json>& bindings) {
    if (!expression.is_string() || expression.get<std::string>().empty() ||
        expression.get<std::string>().front() != '$') {
        return expression;
    }
    const auto name = expression.get<std::string>().substr(1);
    const auto found = bindings.find(name);
    if (found == bindings.end()) {
        throw std::invalid_argument("template-v2: unresolved binding " + name);
    }
    return found->second;
}

void add_uint64(Node& node, const std::string& name, uint64_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_uint64_val(value);
}

void add_uint32(Node& node, const std::string& name, uint32_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_uint32_val(value);
}

void add_int64(Node& node, const std::string& name, int64_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_int64_val(value);
}

void add_int32(Node& node, const std::string& name, int32_t value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_int32_val(value);
}

void add_bool(Node& node, const std::string& name, bool value) {
    auto* attribute = node.add_attr();
    attribute->set_name(name);
    attribute->set_bool_val(value);
}

ChakraProtoMsg::CollectiveCommType collective_type(const std::string& value) {
    if (value == "all_reduce") return ChakraProtoMsg::ALL_REDUCE;
    if (value == "all_gather") return ChakraProtoMsg::ALL_GATHER;
    if (value == "all_to_all") return ChakraProtoMsg::ALL_TO_ALL;
    if (value == "reduce_scatter") return ChakraProtoMsg::REDUCE_SCATTER;
    if (value == "broadcast") return ChakraProtoMsg::BROADCAST;
    throw std::invalid_argument("template-v2: unsupported collective kind " + value);
}

uint64_t positive_uint64(const Json& value, const std::string& field) {
    if (!value.is_number_unsigned() || value.get<uint64_t>() == 0) {
        throw std::invalid_argument("template-v2: " + field + " must be positive");
    }
    return value.get<uint64_t>();
}

uint32_t uint32_value(const Json& value, const std::string& field) {
    if (!value.is_number_unsigned() || value.get<uint64_t>() > UINT32_MAX) {
        throw std::invalid_argument("template-v2: " + field + " must be uint32");
    }
    return value.get<uint32_t>();
}

int32_t nonnegative_int32(const Json& value, const std::string& field) {
    const auto parsed = uint32_value(value, field);
    if (parsed > static_cast<uint32_t>(INT32_MAX)) {
        throw std::invalid_argument("template-v2: " + field +
                                    " exceeds Chakra int32 range");
    }
    return static_cast<int32_t>(parsed);
}

int64_t positive_int64(const Json& value, const std::string& field) {
    const auto parsed = positive_uint64(value, field);
    if (parsed > static_cast<uint64_t>(INT64_MAX)) {
        throw std::invalid_argument("template-v2: " + field +
                                    " exceeds Chakra int64 range");
    }
    return static_cast<int64_t>(parsed);
}

}  // namespace

TemplateWorkloadFeeder::TemplateWorkloadFeeder(
    std::shared_ptr<const TemplateDefinitionSet> definitions,
    TemplateInvocation invocation,
    TemplateRegistry* registry,
    bool roofline_enabled)
    : definitions_(std::move(definitions)),
      invocation_(std::move(invocation)),
      registry_(registry),
      roofline_enabled_(roofline_enabled) {
    if (registry_ == nullptr) {
        throw std::invalid_argument("template-v2: registry is required");
    }
    const auto plan = definitions_->plans.find(invocation_.root_template);
    if (plan == definitions_->plans.end() || plan->second.leaves.empty()) {
        throw std::invalid_argument("template-v2: root execution plan is missing");
    }
    plan_ = &plan->second;
    frame_count_ = plan_->frame_templates.size();
    unfinished_leaf_count_ = plan_->leaves.size();
    for (const auto& leaf : plan_->leaves) validateLeaf(leaf);
    states_.reserve(plan_->leaves.size());
    for (const auto& leaf : plan_->leaves) {
        states_.push_back(
            {leaf.parents.size(), leaf.parents.empty() ? LeafStatus::Ready
                                                       : LeafStatus::Blocked});
    }
    registry_->acquireFrames(frame_count_);
    try {
        for (const auto root_id : plan_->roots) {
            materializeLeaf(root_id);
            ready_.push(root_id);
        }
    } catch (...) {
        releaseMaterializedLeaves();
        releaseFrames();
        throw;
    }
}

TemplateWorkloadFeeder::~TemplateWorkloadFeeder() {
    releaseMaterializedLeaves();
    releaseFrames();
}

std::shared_ptr<Chakra::ETFeederNode>
TemplateWorkloadFeeder::materializeLeaf(uint64_t node_id) {
    const auto existing = materialized_.find(node_id);
    if (existing != materialized_.end()) return existing->second;
    if (node_id == 0 || node_id > plan_->leaves.size()) {
        throw std::out_of_range("template-v2: leaf id is outside the plan");
    }
    auto node = makeLeaf(plan_->leaves.at(node_id - 1));
    materialized_.emplace(node_id, node);
    for (const auto parent_id : plan_->leaves.at(node_id - 1).parents) {
        const auto parent = materialized_.find(parent_id);
        if (parent != materialized_.end()) {
            parent->second->addChild(node);
        }
    }
    registry_->acquireMaterializedLeaf();
    peak_materialized_leaf_count_ =
        std::max<uint64_t>(peak_materialized_leaf_count_, materialized_.size());
    return node;
}

void TemplateWorkloadFeeder::validateLeaf(
    const TemplateLeafPlan& leaf) const {
    const auto attr = [&](const std::string& name) {
        return resolve(leaf.attributes.at(name), invocation_.bindings);
    };
    if (leaf.kind == "compute") {
        const auto duration = attr("duration_ns");
        const auto num_ops = attr("num_ops");
        const auto tensor_size = attr("tensor_size");
        if (!duration.is_number_unsigned() || !num_ops.is_number_unsigned() ||
            !tensor_size.is_number_unsigned() ||
            !attr("is_cpu_op").is_boolean() ||
            (roofline_enabled_ && tensor_size.get<uint64_t>() == 0) ||
            (duration.get<uint64_t>() == 0 && num_ops.get<uint64_t>() == 0)) {
            throw std::invalid_argument("template-v2: invalid compute attributes");
        }
        return;
    }
    if (leaf.kind == "collective") {
        collective_type(attr("comm_type").get<std::string>());
        positive_int64(attr("comm_size"), "comm_size");
        nonnegative_int32(attr("comm_priority"), "comm_priority");
        const auto involved = attr("involved_dim");
        if (!involved.is_array() || involved.empty()) {
            throw std::invalid_argument(
                "template-v2: involved_dim must be a nonempty array");
        }
        return;
    }
    if (leaf.kind == "memory_load" || leaf.kind == "memory_store") {
        positive_uint64(attr("tensor_size"), "tensor_size");
        const auto location = uint32_value(attr("tensor_loc"), "tensor_loc");
        if (location == 0) {
            throw std::invalid_argument(
                "template-v2: tensor_loc must not be INVALID_MEMORY");
        }
        uint32_value(attr("tensor_device"), "tensor_device");
        uint32_value(attr("tensor_channel"), "tensor_channel");
        return;
    }
    if (leaf.kind == "send" || leaf.kind == "recv") {
        positive_int64(attr("comm_size"), "comm_size");
        nonnegative_int32(attr("comm_src"), "comm_src");
        nonnegative_int32(attr("comm_dst"), "comm_dst");
        nonnegative_int32(attr("comm_tag"), "comm_tag");
        return;
    }
    if (leaf.kind != "invalid") {
        throw std::invalid_argument("template-v2: unsupported leaf kind");
    }
}

std::shared_ptr<Chakra::ETFeederNode> TemplateWorkloadFeeder::makeLeaf(
    const TemplateLeafPlan& leaf) {
    auto node = std::make_shared<Node>();
    node->set_id(leaf.id);
    node->set_name(leaf.name);
    for (const auto dependency : leaf.parents) {
        if (states_.at(dependency - 1).status != LeafStatus::Complete) {
            node->add_data_deps(dependency);
        }
    }
    const auto attr = [&](const std::string& name) {
        return resolve(leaf.attributes.at(name), invocation_.bindings);
    };
    if (leaf.kind == "compute") {
        node->set_type(ChakraProtoMsg::COMP_NODE);
        const auto duration = attr("duration_ns");
        if (!duration.is_number_unsigned()) {
            throw std::invalid_argument("template-v2: duration_ns must be uint64");
        }
        const auto num_ops = attr("num_ops");
        if (!num_ops.is_number_unsigned()) {
            throw std::invalid_argument("template-v2: num_ops must be uint64");
        }
        if (duration.get<uint64_t>() == 0 && num_ops.get<uint64_t>() == 0) {
            throw std::invalid_argument(
                "template-v2: compute requires duration_ns or num_ops");
        }
        node->set_duration_micros(duration.get<uint64_t>());
        add_uint64(*node, "num_ops", num_ops.get<uint64_t>());
        const auto tensor_size = attr("tensor_size");
        if (!tensor_size.is_number_unsigned()) {
            throw std::invalid_argument("template-v2: tensor_size must be uint64");
        }
        add_uint64(*node, "tensor_size", tensor_size.get<uint64_t>());
        add_bool(*node, "is_cpu_op", attr("is_cpu_op").get<bool>());
    } else if (leaf.kind == "collective") {
        node->set_type(ChakraProtoMsg::COMM_COLL_NODE);
        add_int64(*node, "comm_type",
                  collective_type(attr("comm_type").get<std::string>()));
        add_int64(*node, "comm_size",
                  positive_int64(attr("comm_size"), "comm_size"));
        add_int32(*node, "comm_priority",
                  nonnegative_int32(attr("comm_priority"), "comm_priority"));
        auto* involved = node->add_attr();
        involved->set_name("involved_dim");
        for (const auto& value : attr("involved_dim")) {
            involved->mutable_bool_list()->add_values(value.get<bool>());
        }
    } else if (leaf.kind == "memory_load" ||
               leaf.kind == "memory_store") {
        node->set_type(leaf.kind == "memory_load"
                           ? ChakraProtoMsg::MEM_LOAD_NODE
                           : ChakraProtoMsg::MEM_STORE_NODE);
        add_uint64(*node, "tensor_size",
                   positive_uint64(attr("tensor_size"), "tensor_size"));
        const auto location = uint32_value(attr("tensor_loc"), "tensor_loc");
        if (location == 0) {
            throw std::invalid_argument(
                "template-v2: tensor_loc must not be INVALID_MEMORY");
        }
        add_uint32(*node, "tensor_loc", location);
        add_uint32(*node, "tensor_device",
                   uint32_value(attr("tensor_device"), "tensor_device"));
        add_uint32(*node, "tensor_channel",
                   uint32_value(attr("tensor_channel"), "tensor_channel"));
    } else if (leaf.kind == "send" || leaf.kind == "recv") {
        node->set_type(leaf.kind == "send"
                           ? ChakraProtoMsg::COMM_SEND_NODE
                           : ChakraProtoMsg::COMM_RECV_NODE);
        add_int64(*node, "comm_size",
                  positive_int64(attr("comm_size"), "comm_size"));
        add_int32(*node, "comm_src",
                  nonnegative_int32(attr("comm_src"), "comm_src"));
        add_int32(*node, "comm_dst",
                  nonnegative_int32(attr("comm_dst"), "comm_dst"));
        add_int32(*node, "comm_tag",
                  nonnegative_int32(attr("comm_tag"), "comm_tag"));
    } else if (leaf.kind == "invalid") {
        node->set_type(ChakraProtoMsg::INVALID_NODE);
    } else {
        throw std::invalid_argument("template-v2: unsupported leaf kind");
    }
    return std::make_shared<Chakra::ETFeederNode>(node);
}

bool TemplateWorkloadFeeder::hasNodesToIssue() {
    return unfinished_leaf_count_ != 0;
}

std::shared_ptr<Chakra::ETFeederNode>
TemplateWorkloadFeeder::getNextIssuableNode() {
    if (ready_.empty()) return nullptr;
    const auto node_id = ready_.top();
    ready_.pop();
    auto& state = states_.at(node_id - 1);
    if (state.status != LeafStatus::Ready) {
        throw std::logic_error("template-v2: ready queue state mismatch");
    }
    state.status = LeafStatus::Issued;
    return materializeLeaf(node_id);
}

void TemplateWorkloadFeeder::pushBackIssuableNode(uint64_t node_id) {
    auto& state = states_.at(node_id - 1);
    if (state.status != LeafStatus::Issued) {
        throw std::logic_error("template-v2: only issued leaves can be pushed back");
    }
    state.status = LeafStatus::Ready;
    ready_.push(node_id);
}

std::shared_ptr<Chakra::ETFeederNode>
TemplateWorkloadFeeder::lookupNode(uint64_t node_id) {
    return materialized_.at(node_id);
}

std::vector<std::shared_ptr<Chakra::ETFeederNode>>
TemplateWorkloadFeeder::childNodes(uint64_t node_id) {
    if (node_id == 0 || node_id > plan_->leaves.size()) {
        throw std::out_of_range("template-v2: parent leaf id is outside the plan");
    }
    std::vector<std::shared_ptr<Chakra::ETFeederNode>> result;
    for (const auto child_id : plan_->leaves.at(node_id - 1).children) {
        result.push_back(materializeLeaf(child_id));
    }
    return result;
}

void TemplateWorkloadFeeder::freeChildrenNodes(uint64_t node_id) {
    auto& parent_state = states_.at(node_id - 1);
    if (parent_state.status != LeafStatus::Issued) {
        throw std::logic_error("template-v2: completing a leaf not in flight");
    }
    const auto children = childNodes(node_id);
    parent_state.status = LeafStatus::Complete;
    for (const auto& child : children) {
        auto* dependencies = child->getChakraNode()->mutable_data_deps();
        const auto found = std::find(dependencies->begin(), dependencies->end(),
                                     node_id);
        if (found == dependencies->end()) {
            throw std::logic_error("template-v2: completed dependency is missing");
        }
        dependencies->erase(found);
        auto& child_state = states_.at(child->id() - 1);
        if (child_state.remaining_dependencies == 0) {
            throw std::logic_error(
                "template-v2: remaining dependency counter underflow");
        }
        --child_state.remaining_dependencies;
        if (child_state.remaining_dependencies == 0) {
            if (child_state.status != LeafStatus::Blocked) {
                throw std::logic_error("template-v2: child readiness mismatch");
            }
            child_state.status = LeafStatus::Ready;
            ready_.push(child->id());
        }
    }
}

void TemplateWorkloadFeeder::removeNode(uint64_t node_id) {
    if (states_.at(node_id - 1).status != LeafStatus::Complete ||
        materialized_.erase(node_id) != 1) {
        throw std::logic_error("template-v2: removing unknown node");
    }
    registry_->releaseMaterializedLeaf();
    --unfinished_leaf_count_;
    if (!hasNodesToIssue()) releaseFrames();
}

void TemplateWorkloadFeeder::printGraph() {
    for (const auto& [id, node] : materialized_) {
        std::cout << "{" << id << ": ";
        node->printNode();
    }
}

const std::string& TemplateWorkloadFeeder::tierManifestDigest() const {
    return definitions_->tier_manifest_digest;
}
const std::string& TemplateWorkloadFeeder::serviceBindingDigest() const {
    return definitions_->service_binding_digest;
}
const std::string& TemplateWorkloadFeeder::serviceActivationId() const {
    return definitions_->service_activation_id;
}
std::optional<uint32_t> TemplateWorkloadFeeder::serviceRank() const {
    return invocation_.rank;
}
uint64_t TemplateWorkloadFeeder::frameCount() const { return frame_count_; }
uint64_t TemplateWorkloadFeeder::materializedLeafCount() const {
    return materialized_.size();
}
uint64_t TemplateWorkloadFeeder::peakMaterializedLeafCount() const {
    return peak_materialized_leaf_count_;
}

void TemplateWorkloadFeeder::releaseFrames() {
    if (!frames_released_) {
        registry_->releaseFrames(frame_count_);
        frames_released_ = true;
    }
}

void TemplateWorkloadFeeder::releaseMaterializedLeaves() {
    while (!materialized_.empty()) {
        materialized_.erase(materialized_.begin());
        registry_->releaseMaterializedLeaf();
    }
}

}  // namespace AstraSim
