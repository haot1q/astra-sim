/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplatePlan.hh"

#include <algorithm>
#include <functional>
#include <regex>
#include <set>
#include <stdexcept>

#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/workload/IndexedTemplateExpression.hh"

namespace AstraSim {
namespace {

using Json = nlohmann::json;
namespace Wire = ServiceBindingJson;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v3: " + message);
    }
}

void fields(const Json& value, std::initializer_list<const char*> expected,
            const std::string& context) {
    require(value.is_object() && value.size() == expected.size(),
            context + " has missing or unknown fields");
    for (const auto* name : expected) {
        require(value.contains(name), context + " is missing " + name);
    }
}

std::string text(const Json& value, const std::string& field) {
    require(value.is_string() && !value.get<std::string>().empty(),
            field + " must be a nonempty string");
    return value.get<std::string>();
}

std::string identifier(const Json& value, const std::string& field) {
    static const std::regex pattern("^[A-Za-z_][A-Za-z0-9_.:-]*$");
    const auto result = text(value, field);
    require(std::regex_match(result, pattern),
            field + " must be an ASCII identifier");
    return result;
}

uint64_t unsigned_value(const Json& value, const std::string& field) {
    require(value.is_number_unsigned(), field + " must be uint64");
    return value.get<uint64_t>();
}

IndexedEdgeRelation relation(const Json& value) {
    const auto name = text(value, "edge.relation");
    if (name == "same_index") return IndexedEdgeRelation::SameIndex;
    if (name == "same_outer") return IndexedEdgeRelation::SameOuter;
    if (name == "all_to_one") return IndexedEdgeRelation::AllToOne;
    if (name == "one_to_all") return IndexedEdgeRelation::OneToAll;
    if (name == "one_to_one") return IndexedEdgeRelation::OneToOne;
    throw std::invalid_argument(
        "template-v3: unsupported edge relation " + name);
}

const std::set<std::string>& recipe_attributes(
    const std::string& kind) {
    static const std::set<std::string> compute = {
        "duration_ns", "num_ops", "tensor_size", "is_cpu_op"};
    static const std::set<std::string> memory = {
        "tensor_size", "tensor_loc", "tensor_device", "tensor_channel"};
    static const std::set<std::string> collective = {
        "comm_type", "comm_size", "comm_priority", "involved_dim"};
    static const std::set<std::string> point_to_point = {
        "comm_size", "comm_src", "comm_dst", "comm_tag"};
    if (kind == "compute") return compute;
    if (kind == "memory_load" || kind == "memory_store") return memory;
    if (kind == "collective") return collective;
    if (kind == "send" || kind == "recv") return point_to_point;
    throw std::invalid_argument(
        "template-v3: unsupported recipe kind " + kind);
}

const IndexedTemplateEdge* connecting_edge(
    const std::vector<IndexedTemplateEdge>& edges,
    std::size_t from, std::size_t to) {
    const auto found = std::find_if(
        edges.begin(), edges.end(), [&](const auto& edge) {
            return edge.from_recipe == from && edge.to_recipe == to;
        });
    return found == edges.end() ? nullptr : &*found;
}

IndexedIndices recipe_indices(
    const IndexedTemplateRecipe& recipe, uint64_t flat_index) {
    require(flat_index < recipe.count, "event index exceeds recipe domain");
    IndexedIndices result;
    uint64_t stride = recipe.count;
    for (std::size_t axis = 0; axis < recipe.axes.size(); ++axis) {
        stride /= recipe.extents.at(axis);
        result.emplace(
            recipe.axes.at(axis),
            (flat_index / stride) % recipe.extents.at(axis));
    }
    return result;
}

}  // namespace

std::string IndexedTemplatePlan::definitionDigest(const Json& definition) {
    require(definition.is_object() &&
                definition.contains("definition_digest"),
            "definition requires definition_digest");
    auto identity = definition;
    identity.erase("definition_digest");
    return Wire::digest(identity, true);
}

IndexedTemplatePlan IndexedTemplatePlan::compile(
    const Json& definition, const Json& invocation, uint32_t expected_rank) {
    fields(definition,
           {"schema_version", "template_id", "definition_digest",
            "tier_manifest_digest", "service_binding_digest",
            "service_activation_id", "ports", "domains", "recipes", "edges"},
           "definition");
    require(definition.at("schema_version") ==
                "template-definition-v3-proof",
            "unsupported definition schema_version");
    identifier(definition.at("template_id"), "template_id");
    require(definitionDigest(definition) ==
                text(definition.at("definition_digest"),
                     "definition_digest"),
            "definition_digest mismatch");

    fields(invocation,
           {"schema_version", "definition_id", "definition_digest", "ranks"},
           "invocation");
    require(invocation.at("schema_version") ==
                "template-invocation-v3-proof",
            "unsupported invocation schema_version");
    require(invocation.at("definition_id") == definition.at("template_id") &&
                invocation.at("definition_digest") ==
                    definition.at("definition_digest"),
            "invocation definition identity mismatch");

    IndexedTemplatePlan result;
    result.rank_ = expected_rank;
    result.tier_manifest_digest_ =
        text(definition.at("tier_manifest_digest"), "tier_manifest_digest");
    result.service_binding_digest_ =
        text(definition.at("service_binding_digest"), "service_binding_digest");
    result.service_activation_id_ =
        text(definition.at("service_activation_id"), "service_activation_id");
    const auto& selected = selectRank(invocation, expected_rank);
    result.loadBindings(definition, selected);
    const auto domains = result.loadDomains(definition);
    result.loadRecipes(definition, domains);
    result.assignEventIds();
    result.loadEdges(definition);
    result.validateGraph();
    return result;
}

const Json& IndexedTemplatePlan::selectRank(
    const Json& invocation, uint32_t expected_rank) {
    require(invocation.at("ranks").is_array(), "ranks must be an array");
    const Json* selected = nullptr;
    std::set<uint32_t> ranks;
    for (const auto& rank : invocation.at("ranks")) {
        fields(rank, {"rank", "bindings"}, "rank invocation");
        const auto value = unsigned_value(rank.at("rank"), "rank");
        require(value <= UINT32_MAX, "rank exceeds uint32");
        require(ranks.insert(static_cast<uint32_t>(value)).second,
                "duplicate rank");
        if (value == expected_rank) selected = &rank;
    }
    require(selected != nullptr, "invocation does not contain expected rank");
    require(selected->at("bindings").is_object(),
            "rank bindings must be an object");
    return *selected;
}

void IndexedTemplatePlan::loadBindings(
    const Json& definition, const Json& rank) {
    require(definition.at("ports").is_array(), "ports must be an array");
    std::set<std::string> ports;
    for (const auto& port : definition.at("ports")) {
        fields(port, {"name", "type"}, "port");
        const auto name = identifier(port.at("name"), "port.name");
        const auto type = text(port.at("type"), "port.type");
        require(type == "uint64" || type == "uint64_vector",
                "prototype ports must be uint64 or uint64_vector");
        require(ports.insert(name).second, "duplicate port");
        require(rank.at("bindings").contains(name),
                "rank binding is missing port " + name);
        const auto& bound = rank.at("bindings").at(name);
        if (type == "uint64") {
            bindings_.emplace(name, unsigned_value(bound, "binding " + name));
            continue;
        }
        require(bound.is_array() && !bound.empty(),
                "vector binding " + name + " must be a nonempty array");
        std::vector<uint64_t> values;
        values.reserve(bound.size());
        for (const auto& entry : bound) {
            values.push_back(unsigned_value(entry, "binding " + name));
        }
        vectors_.emplace(name, std::move(values));
    }
    require(rank.at("bindings").size() == ports.size(),
            "rank bindings do not exactly match ports");
}

std::unordered_map<std::string, IndexedTemplateDomain>
IndexedTemplatePlan::loadDomains(const Json& definition) const {
    require(definition.at("domains").is_array(),
            "domains must be an array");
    std::unordered_map<std::string, IndexedTemplateDomain> result;
    for (const auto& domain : definition.at("domains")) {
        require(domain.is_object() && domain.size() == 2 &&
                    domain.contains("id") &&
                    (domain.contains("extent") != domain.contains("axes")),
                "domain requires exactly one extent or axes declaration");
        const auto id = identifier(domain.at("id"), "domain.id");
        IndexedTemplateDomain loaded;
        loaded.count = 1;
        if (domain.contains("extent")) {
            loaded.axes.push_back(id);
            const auto extent = evaluateIndexedExpression(
                domain.at("extent"), bindings_, vectors_, IndexedIndices{});
            require(extent.is_number_unsigned(),
                    "domain extent must be uint64");
            loaded.extents.push_back(extent.get<uint64_t>());
            loaded.count = extent.get<uint64_t>();
        } else {
            require(domain.at("axes").is_array() &&
                        !domain.at("axes").empty() &&
                        domain.at("axes").size() <= 2,
                    "domain axes must contain one or two entries");
            std::set<std::string> axes;
            for (const auto& axis : domain.at("axes")) {
                fields(axis, {"id", "extent"}, "domain axis");
                const auto axis_id =
                    identifier(axis.at("id"), "domain axis.id");
                require(axes.insert(axis_id).second,
                        "domain axes must be unique");
                const auto extent = evaluateIndexedExpression(
                    axis.at("extent"), bindings_, vectors_, IndexedIndices{});
                require(extent.is_number_unsigned(),
                        "domain extent must be uint64");
                loaded.axes.push_back(axis_id);
                loaded.extents.push_back(extent.get<uint64_t>());
                loaded.count = checkedIndexedMultiply(
                    loaded.count, extent.get<uint64_t>(),
                    "domain event count");
            }
        }
        require(result.emplace(id, std::move(loaded)).second,
                "duplicate domain");
    }
    return result;
}

void IndexedTemplatePlan::loadRecipes(
    const Json& definition,
    const std::unordered_map<std::string, IndexedTemplateDomain>& domains) {
    require(definition.at("recipes").is_array() &&
                !definition.at("recipes").empty(),
            "recipes must be a nonempty array");
    std::set<std::string> recipe_ids;
    for (const auto& raw : definition.at("recipes")) {
        fields(raw, {"id", "kind", "domain", "attrs"}, "recipe");
        IndexedTemplateRecipe recipe;
        recipe.id = identifier(raw.at("id"), "recipe.id");
        recipe.kind = identifier(raw.at("kind"), "recipe.kind");
        require(recipe_ids.insert(recipe.id).second, "duplicate recipe");
        require(raw.at("attrs").is_object(),
                "recipe attrs must be an object");
        if (!raw.at("domain").is_null()) {
            recipe.domain = identifier(raw.at("domain"), "recipe.domain");
            require(domains.count(*recipe.domain) != 0,
                    "recipe references unknown domain");
            const auto& domain = domains.at(*recipe.domain);
            recipe.count = domain.count;
            recipe.axes = domain.axes;
            recipe.extents = domain.extents;
        } else {
            recipe.count = 1;
        }
        recipe.base_id = 0;
        recipe.stride = 1;
        for (const auto& [name, value] : raw.at("attrs").items()) {
            recipe.attributes.emplace(name, value);
        }
        const auto& expected = recipe_attributes(recipe.kind);
        require(recipe.attributes.size() == expected.size(),
                "recipe attributes do not exactly match kind");
        for (const auto& name : expected) {
            require(recipe.attributes.count(name) != 0,
                    "recipe is missing attribute " + name);
        }
        for (const auto& [name, value] : recipe.attributes) {
            (void)name;
            if (recipe.count == 0) continue;
            evaluateIndexedExpression(
                value, bindings_, vectors_, recipe_indices(recipe, 0));
            evaluateIndexedExpression(
                value, bindings_, vectors_,
                recipe_indices(recipe, recipe.count - 1));
        }
        recipes_.push_back(std::move(recipe));
    }
}

void IndexedTemplatePlan::assignEventIds() {
    std::set<std::string> assigned_domains;
    for (std::size_t index = 0; index < recipes_.size(); ++index) {
        auto& recipe = recipes_.at(index);
        if (!recipe.domain.has_value()) {
            recipe.base_id =
                checkedIndexedAdd(event_count_, 1, "event id");
            event_count_ =
                checkedIndexedAdd(event_count_, 1, "event count");
            continue;
        }
        if (!assigned_domains.insert(*recipe.domain).second) continue;
        std::vector<std::size_t> members;
        for (std::size_t candidate = 0;
             candidate < recipes_.size(); ++candidate) {
            if (recipes_.at(candidate).domain == recipe.domain) {
                members.push_back(candidate);
            }
        }
        const auto stride = static_cast<uint64_t>(members.size());
        const auto base =
            checkedIndexedAdd(event_count_, 1, "event id");
        for (std::size_t offset = 0; offset < members.size(); ++offset) {
            auto& member = recipes_.at(members.at(offset));
            member.base_id =
                checkedIndexedAdd(base, offset, "event id");
            member.stride = stride;
        }
        event_count_ = checkedIndexedAdd(
            event_count_,
            checkedIndexedMultiply(
                recipe.count, stride, "domain event count"),
            "event count");
    }
}

void IndexedTemplatePlan::loadEdges(const Json& definition) {
    require(definition.at("edges").is_array(), "edges must be an array");
    std::unordered_map<std::string, std::size_t> recipe_ids;
    for (std::size_t index = 0; index < recipes_.size(); ++index) {
        recipe_ids.emplace(recipes_.at(index).id, index);
    }
    std::set<std::pair<std::size_t, std::size_t>> edge_pairs;
    for (const auto& raw : definition.at("edges")) {
        fields(raw, {"from", "to", "relation"}, "edge");
        const auto from_name = identifier(raw.at("from"), "edge.from");
        const auto to_name = identifier(raw.at("to"), "edge.to");
        require(recipe_ids.count(from_name) && recipe_ids.count(to_name),
                "edge references unknown recipe");
        const auto from = recipe_ids.at(from_name);
        const auto to = recipe_ids.at(to_name);
        require(edge_pairs.emplace(from, to).second, "duplicate edge");
        const auto edge_relation = relation(raw.at("relation"));
        const auto& source = recipes_.at(from);
        const auto& destination = recipes_.at(to);
        if (edge_relation == IndexedEdgeRelation::SameIndex) {
            require(source.domain.has_value() &&
                        source.domain == destination.domain &&
                        source.count == destination.count,
                    "same_index requires one matching domain");
        } else if (edge_relation == IndexedEdgeRelation::SameOuter) {
            require(source.domain.has_value() &&
                        destination.domain.has_value() &&
                        !source.axes.empty() &&
                        !destination.axes.empty() &&
                        source.axes.front() == destination.axes.front() &&
                        (source.extents.front() ==
                             destination.extents.front() ||
                         destination.count == 0),
                    "same_outer requires repeated domains with one common "
                    "outer axis");
        } else if (edge_relation == IndexedEdgeRelation::AllToOne) {
            require(source.domain.has_value() &&
                        !destination.domain.has_value(),
                    "all_to_one requires repeated source and static target");
        } else if (edge_relation == IndexedEdgeRelation::OneToAll) {
            require(!source.domain.has_value() &&
                        destination.domain.has_value(),
                    "one_to_all requires static source and repeated target");
        } else {
            require(!source.domain.has_value() &&
                        !destination.domain.has_value(),
                    "one_to_one requires static recipes");
        }
        edges_.push_back({from, to, edge_relation});
    }
}

void IndexedTemplatePlan::validateGraph() const {
    std::vector<int> state(recipes_.size());
    std::function<void(std::size_t)> visit = [&](std::size_t recipe) {
        require(state.at(recipe) != 1, "recipe dependency cycle");
        if (state.at(recipe) == 2) return;
        state.at(recipe) = 1;
        for (const auto& edge : edges_) {
            if (edge.from_recipe == recipe) visit(edge.to_recipe);
        }
        state.at(recipe) = 2;
    };
    for (std::size_t recipe = 0; recipe < recipes_.size(); ++recipe) {
        visit(recipe);
    }
}

IndexedTemplateEvent IndexedTemplatePlan::event(uint64_t event_id) const {
    require(event_id > 0 && event_id <= event_count_,
            "event id is outside the plan");
    const auto found = std::find_if(
        recipes_.begin(), recipes_.end(), [&](const auto& recipe) {
            if (event_id < recipe.base_id) return false;
            const auto delta = event_id - recipe.base_id;
            return delta % recipe.stride == 0 &&
                   delta / recipe.stride < recipe.count;
        });
    require(found != recipes_.end(), "event id does not map to a recipe");
    const auto recipe =
        static_cast<std::size_t>(std::distance(recipes_.begin(), found));
    const auto index = (event_id - found->base_id) / found->stride;
    return {event_id, recipe, index, recipe_indices(*found, index)};
}

uint64_t IndexedTemplatePlan::eventId(
    std::size_t recipe, uint64_t index) const {
    const auto& selected = recipes_.at(recipe);
    require(index < selected.count, "event index exceeds recipe domain");
    return checkedIndexedAdd(
        selected.base_id,
        checkedIndexedMultiply(index, selected.stride, "event id"),
        "event id");
}

Json IndexedTemplatePlan::attributes(
    const IndexedTemplateEvent& event) const {
    Json result = Json::object();
    for (const auto& [name, value] : recipes_.at(event.recipe).attributes) {
        result[name] = evaluateIndexedExpression(
            value, bindings_, vectors_, event.indices);
    }
    return result;
}

std::vector<IndexedTemplateEvent> IndexedTemplatePlan::children(
    const IndexedTemplateEvent& event) const {
    std::vector<IndexedTemplateEvent> result;
    for (const auto& edge : edges_) {
        if (edge.from_recipe != event.recipe) continue;
        if (edge.relation == IndexedEdgeRelation::SameOuter) {
            const auto& source = recipes_.at(edge.from_recipe);
            const auto& target = recipes_.at(edge.to_recipe);
            if (target.count == 0) continue;
            const auto outer = event.indices.at(source.axes.front());
            const auto inner = target.count / target.extents.front();
            for (uint64_t offset = 0; offset < inner; ++offset) {
                result.push_back(this->event(
                    eventId(edge.to_recipe, outer * inner + offset)));
            }
            continue;
        }
        if (edge.relation == IndexedEdgeRelation::OneToAll) {
            const auto count = recipes_.at(edge.to_recipe).count;
            for (uint64_t index = 0; index < count; ++index) {
                result.push_back(
                    this->event(eventId(edge.to_recipe, index)));
            }
            continue;
        }
        const uint64_t index =
            edge.relation == IndexedEdgeRelation::SameIndex ? event.index : 0;
        result.push_back(this->event(eventId(edge.to_recipe, index)));
    }
    return result;
}

std::vector<uint64_t> IndexedTemplatePlan::fixedParentIds(
    const IndexedTemplateEvent& event) const {
    std::vector<uint64_t> result;
    for (const auto& edge : edges_) {
        if (edge.to_recipe != event.recipe ||
            edge.relation == IndexedEdgeRelation::AllToOne ||
            edge.relation == IndexedEdgeRelation::SameOuter) {
            continue;
        }
        const uint64_t index =
            edge.relation == IndexedEdgeRelation::SameIndex ? event.index : 0;
        result.push_back(eventId(edge.from_recipe, index));
    }
    return result;
}

uint64_t IndexedTemplatePlan::requiredParentCount(
    const IndexedTemplateEvent& event) const {
    uint64_t result = 0;
    for (const auto& edge : edges_) {
        if (edge.to_recipe != event.recipe) continue;
        result = checkedIndexedAdd(
            result,
            edge.relation == IndexedEdgeRelation::AllToOne
                ? recipes_.at(edge.from_recipe).count
                : edge.relation == IndexedEdgeRelation::SameOuter
                ? recipes_.at(edge.from_recipe).count /
                      recipes_.at(edge.from_recipe).extents.front()
                : 1,
            "parent count");
    }
    return result;
}

bool IndexedTemplatePlan::isAllToOne(
    const IndexedTemplateEvent& parent,
    const IndexedTemplateEvent& child) const {
    const auto* edge =
        connecting_edge(edges_, parent.recipe, child.recipe);
    require(edge != nullptr, "events are not connected");
    return edge->relation == IndexedEdgeRelation::AllToOne ||
           edge->relation == IndexedEdgeRelation::SameOuter;
}

bool IndexedTemplatePlan::exposeChildAfterCompletion(
    const IndexedTemplateEvent& parent,
    const IndexedTemplateEvent& child,
    uint64_t completed_parent_count) const {
    const auto* edge =
        connecting_edge(edges_, parent.recipe, child.recipe);
    require(edge != nullptr, "events are not connected");
    // A join is materialized once, by its last parent. That also holds when the
    // join additionally has static parents, so a node never records a
    // dependency on a repeated parent it cannot enumerate.
    const auto joined = std::any_of(
        edges_.begin(), edges_.end(), [&](const auto& item) {
            return item.to_recipe == child.recipe &&
                   (item.relation == IndexedEdgeRelation::AllToOne ||
                    item.relation == IndexedEdgeRelation::SameOuter);
        });
    return !joined ||
           completed_parent_count + 1 == requiredParentCount(child);
}

const std::vector<IndexedTemplateRecipe>&
IndexedTemplatePlan::recipes() const {
    return recipes_;
}
const std::vector<IndexedTemplateEdge>& IndexedTemplatePlan::edges() const {
    return edges_;
}
uint64_t IndexedTemplatePlan::eventCount() const { return event_count_; }
uint32_t IndexedTemplatePlan::rank() const { return rank_; }
const std::string& IndexedTemplatePlan::tierManifestDigest() const {
    return tier_manifest_digest_;
}
const std::string& IndexedTemplatePlan::serviceBindingDigest() const {
    return service_binding_digest_;
}
const std::string& IndexedTemplatePlan::serviceActivationId() const {
    return service_activation_id_;
}

}  // namespace AstraSim
