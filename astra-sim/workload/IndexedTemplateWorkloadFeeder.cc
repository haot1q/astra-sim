/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplateWorkloadFeeder.hh"

#include <algorithm>
#include <iostream>
#include <limits>
#include <stdexcept>

namespace AstraSim {
namespace {

using Json = nlohmann::json;
using Node = ChakraProtoMsg::Node;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v3: " + message);
    }
}

uint64_t positive_uint64(const Json& value, const std::string& field) {
    require(value.is_number_unsigned() && value.get<uint64_t>() > 0,
            field + " must be positive uint64");
    return value.get<uint64_t>();
}

uint32_t uint32_value(const Json& value, const std::string& field) {
    require(value.is_number_unsigned() && value.get<uint64_t>() <= UINT32_MAX,
            field + " must be uint32");
    return value.get<uint32_t>();
}

int32_t chakra_int32(const Json& value, const std::string& field) {
    const auto parsed = uint32_value(value, field);
    require(parsed <= static_cast<uint32_t>(INT32_MAX),
            field + " exceeds Chakra int32");
    return static_cast<int32_t>(parsed);
}

int64_t chakra_int64(const Json& value, const std::string& field,
                     bool allow_zero) {
    require(value.is_number_unsigned() &&
                value.get<uint64_t>() <= static_cast<uint64_t>(INT64_MAX) &&
                (allow_zero || value.get<uint64_t>() > 0),
            field + " must fit Chakra int64");
    return static_cast<int64_t>(value.get<uint64_t>());
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

ChakraProtoMsg::CollectiveCommType collective_type(
    const std::string& value) {
    if (value == "all_reduce") return ChakraProtoMsg::ALL_REDUCE;
    if (value == "all_gather") return ChakraProtoMsg::ALL_GATHER;
    if (value == "all_to_all") return ChakraProtoMsg::ALL_TO_ALL;
    if (value == "reduce_scatter") return ChakraProtoMsg::REDUCE_SCATTER;
    if (value == "broadcast") return ChakraProtoMsg::BROADCAST;
    throw std::invalid_argument(
        "template-v3: unsupported collective kind " + value);
}

}  // namespace

IndexedTemplateWorkloadFeeder::IndexedTemplateWorkloadFeeder(
    IndexedTemplatePlan plan)
    : plan_(std::move(plan)),
      unfinished_event_count_(plan_.eventCount()),
      root_cursor_(plan_.recipes().size(),
                   std::numeric_limits<uint64_t>::max()) {
    require(unfinished_event_count_ > 0, "plan must contain events");
    for (std::size_t recipe = 0; recipe < plan_.recipes().size(); ++recipe) {
        if (plan_.requiredParentCount(
                plan_.event(plan_.eventId(recipe, 0))) == 0) {
            activateRoot(recipe);
        }
    }
    require(!ready_.empty(), "plan has no root events");
}

void IndexedTemplateWorkloadFeeder::activateRoot(std::size_t recipe) {
    require(root_cursor_.at(recipe) == std::numeric_limits<uint64_t>::max(),
            "root recipe activated twice");
    root_cursor_.at(recipe) = 0;
    ready_.push(plan_.eventId(recipe, 0));
}

void IndexedTemplateWorkloadFeeder::advanceRoot(std::size_t recipe) {
    auto& cursor = root_cursor_.at(recipe);
    ++cursor;
    if (cursor < plan_.recipes().at(recipe).count) {
        ready_.push(plan_.eventId(recipe, cursor));
    }
}

void IndexedTemplateWorkloadFeeder::updatePeak() {
    peak_tracked_event_count_ =
        std::max<uint64_t>(peak_tracked_event_count_, states_.size());
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateWorkloadFeeder::materialize(
    const IndexedTemplateEvent& event) {
    const auto existing = materialized_.find(event.id);
    if (existing != materialized_.end()) return existing->second;
    auto node = makeNode(event);
    materialized_.emplace(event.id, node);
    return node;
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateWorkloadFeeder::makeNode(
    const IndexedTemplateEvent& event) const {
    const auto& recipe = plan_.recipes().at(event.recipe);
    const auto attributes = plan_.attributes(event);
    auto node = std::make_shared<Node>();
    node->set_id(event.id);
    node->set_name(recipe.id + "[" + std::to_string(event.index) + "]");
    const auto attr = [&](const std::string& name) -> const Json& {
        require(attributes.contains(name), "recipe attribute is missing " + name);
        return attributes.at(name);
    };
    if (recipe.kind == "compute") {
        node->set_type(ChakraProtoMsg::COMP_NODE);
        const auto duration =
            chakra_int64(attr("duration_ns"), "duration_ns", true);
        const auto operations =
            chakra_int64(attr("num_ops"), "num_ops", true);
        require(duration > 0 || operations > 0,
                "compute requires duration_ns or num_ops");
        node->set_duration_micros(static_cast<uint64_t>(duration));
        add_int64(*node, "num_ops", operations);
        require(attr("tensor_size").is_number_unsigned(),
                "tensor_size must be uint64");
        add_uint64(*node, "tensor_size",
                   attr("tensor_size").get<uint64_t>());
        require(attr("is_cpu_op").is_boolean(),
                "is_cpu_op must be boolean");
        add_bool(*node, "is_cpu_op", attr("is_cpu_op").get<bool>());
    } else if (recipe.kind == "memory_load" ||
               recipe.kind == "memory_store") {
        node->set_type(recipe.kind == "memory_load"
                           ? ChakraProtoMsg::MEM_LOAD_NODE
                           : ChakraProtoMsg::MEM_STORE_NODE);
        add_uint64(*node, "tensor_size",
                   positive_uint64(attr("tensor_size"), "tensor_size"));
        const auto location = uint32_value(attr("tensor_loc"), "tensor_loc");
        require(location != 0, "tensor_loc must not be INVALID_MEMORY");
        add_uint32(*node, "tensor_loc", location);
        add_uint32(*node, "tensor_device",
                   uint32_value(attr("tensor_device"), "tensor_device"));
        add_uint32(*node, "tensor_channel",
                   uint32_value(attr("tensor_channel"), "tensor_channel"));
    } else if (recipe.kind == "collective") {
        node->set_type(ChakraProtoMsg::COMM_COLL_NODE);
        require(attr("comm_type").is_string(), "comm_type must be string");
        add_int64(*node, "comm_type",
                  collective_type(attr("comm_type").get<std::string>()));
        add_int64(*node, "comm_size",
                  chakra_int64(attr("comm_size"), "comm_size", false));
        add_int32(*node, "comm_priority",
                  chakra_int32(attr("comm_priority"), "comm_priority"));
        require(attr("involved_dim").is_array() &&
                    !attr("involved_dim").empty(),
                "involved_dim must be a nonempty array");
        auto* involved = node->add_attr();
        involved->set_name("involved_dim");
        for (const auto& value : attr("involved_dim")) {
            require(value.is_boolean(), "involved_dim must contain booleans");
            involved->mutable_bool_list()->add_values(value.get<bool>());
        }
    } else if (recipe.kind == "send" || recipe.kind == "recv") {
        node->set_type(recipe.kind == "send"
                           ? ChakraProtoMsg::COMM_SEND_NODE
                           : ChakraProtoMsg::COMM_RECV_NODE);
        add_int64(*node, "comm_size",
                  chakra_int64(attr("comm_size"), "comm_size", false));
        add_int32(*node, "comm_src",
                  chakra_int32(attr("comm_src"), "comm_src"));
        add_int32(*node, "comm_dst",
                  chakra_int32(attr("comm_dst"), "comm_dst"));
        add_int32(*node, "comm_tag",
                  chakra_int32(attr("comm_tag"), "comm_tag"));
    } else {
        throw std::invalid_argument(
            "template-v3: unsupported recipe kind " + recipe.kind);
    }
    return std::make_shared<Chakra::ETFeederNode>(node);
}

bool IndexedTemplateWorkloadFeeder::hasNodesToIssue() {
    return unfinished_event_count_ != 0;
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateWorkloadFeeder::getNextIssuableNode() {
    if (ready_.empty()) return nullptr;
    const auto node_id = ready_.top();
    ready_.pop();
    const auto event = plan_.event(node_id);
    auto found = states_.find(node_id);
    if (found == states_.end()) {
        found = states_.emplace(
            node_id, EventState{0, EventStatus::Ready}).first;
        updatePeak();
    }
    require(found->second.status == EventStatus::Ready,
            "ready queue state mismatch");
    found->second.status = EventStatus::Issued;
    if (root_cursor_.at(event.recipe) == event.index) {
        advanceRoot(event.recipe);
    }
    return materialize(event);
}

void IndexedTemplateWorkloadFeeder::pushBackIssuableNode(uint64_t node_id) {
    auto& state = states_.at(node_id);
    require(state.status == EventStatus::Issued,
            "only issued events can be pushed back");
    state.status = EventStatus::Ready;
    ready_.push(node_id);
}

std::shared_ptr<Chakra::ETFeederNode>
IndexedTemplateWorkloadFeeder::lookupNode(uint64_t node_id) {
    return materialized_.at(node_id);
}

std::vector<std::shared_ptr<Chakra::ETFeederNode>>
IndexedTemplateWorkloadFeeder::childNodes(uint64_t node_id) {
    const auto parent = plan_.event(node_id);
    std::vector<std::shared_ptr<Chakra::ETFeederNode>> result;
    for (const auto& child : plan_.children(parent)) {
        auto [state, inserted] =
            states_.try_emplace(child.id, EventState{});
        if (inserted) updatePeak();
        if (!plan_.exposeChildAfterCompletion(
                parent, child, state->second.completed_parents)) {
            continue;
        }
        const bool first_materialization =
            materialized_.count(child.id) == 0;
        auto node = materialize(child);
        if (first_materialization) {
            auto* dependencies =
                node->getChakraNode()->mutable_data_deps();
            for (const auto parent_id : plan_.fixedParentIds(child)) {
                const auto parent_state = states_.find(parent_id);
                if (parent_state == states_.end() ||
                    parent_state->second.status != EventStatus::Complete) {
                    dependencies->Add(parent_id);
                }
            }
            if (plan_.isAllToOne(parent, child)) {
                dependencies->Add(parent.id);
            }
        }
        if (exposed_children_[parent.id].insert(child.id).second) {
            materialized_.at(parent.id)->addChild(node);
        }
        result.push_back(std::move(node));
    }
    return result;
}

void IndexedTemplateWorkloadFeeder::freeChildrenNodes(uint64_t node_id) {
    auto& parent_state = states_.at(node_id);
    require(parent_state.status == EventStatus::Issued,
            "completing an event not in flight");
    parent_state.status = EventStatus::Complete;
    for (const auto& child : plan_.children(plan_.event(node_id))) {
        auto [state, inserted] =
            states_.try_emplace(child.id, EventState{});
        if (inserted) updatePeak();
        const auto materialized = materialized_.find(child.id);
        if (materialized != materialized_.end()) {
            auto* dependencies =
                materialized->second->getChakraNode()->mutable_data_deps();
            const auto dependency =
                std::find(dependencies->begin(), dependencies->end(), node_id);
            require(dependency != dependencies->end(),
                    "completed dependency is missing");
            dependencies->erase(dependency);
        }
        ++state->second.completed_parents;
        const auto required = plan_.requiredParentCount(child);
        require(state->second.completed_parents <= required,
                "completed parent count overflow");
        if (state->second.completed_parents == required) {
            require(state->second.status == EventStatus::Blocked,
                    "child readiness mismatch");
            state->second.status = EventStatus::Ready;
            ready_.push(child.id);
        }
    }
}

void IndexedTemplateWorkloadFeeder::removeNode(uint64_t node_id) {
    const auto found = states_.find(node_id);
    require(found != states_.end() &&
                found->second.status == EventStatus::Complete &&
                materialized_.erase(node_id) == 1,
            "removing unknown event");
    states_.erase(found);
    exposed_children_.erase(node_id);
    require(unfinished_event_count_ > 0, "unfinished event count underflow");
    --unfinished_event_count_;
}

void IndexedTemplateWorkloadFeeder::printGraph() {
    for (const auto& [id, node] : materialized_) {
        std::cout << "{" << id << ": ";
        node->printNode();
    }
}

const std::string&
IndexedTemplateWorkloadFeeder::tierManifestDigest() const {
    return plan_.tierManifestDigest();
}
const std::string&
IndexedTemplateWorkloadFeeder::serviceBindingDigest() const {
    return plan_.serviceBindingDigest();
}
const std::string&
IndexedTemplateWorkloadFeeder::serviceActivationId() const {
    return plan_.serviceActivationId();
}
std::optional<uint32_t>
IndexedTemplateWorkloadFeeder::serviceRank() const {
    return plan_.rank();
}
uint64_t IndexedTemplateWorkloadFeeder::trackedEventCount() const {
    return states_.size();
}
uint64_t IndexedTemplateWorkloadFeeder::peakTrackedEventCount() const {
    return peak_tracked_event_count_;
}
uint64_t IndexedTemplateWorkloadFeeder::materializedEventCount() const {
    return materialized_.size();
}

}  // namespace AstraSim
