/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/TemplateRegistry.hh"

#include <algorithm>
#include <functional>
#include <limits>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <sys/stat.h>

#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim {
namespace {

using Json = nlohmann::json;
namespace Wire = ServiceBindingJson;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v2: " + message);
    }
}

std::string required_text(const Json& value, const std::string& field) {
    require(value.is_string() && !value.get<std::string>().empty(),
            field + " must be a nonempty string");
    return value.get<std::string>();
}

std::string identifier(const Json& value, const std::string& field) {
    static const std::regex pattern("^[A-Za-z_][A-Za-z0-9_.:-]*$");
    const auto text = required_text(value, field);
    require(std::regex_match(text, pattern), field + " must be an ASCII identifier");
    return text;
}

void fields(const Json& value, std::initializer_list<const char*> expected,
            const std::string& context) {
    require(value.is_object() && value.size() == expected.size(),
            context + " has missing or unknown fields");
    for (const auto* name : expected) {
        require(value.contains(name), context + " is missing " + name);
    }
}

uint64_t unsigned_value(const Json& value, const std::string& field,
                        uint64_t maximum = std::numeric_limits<uint64_t>::max()) {
    require(value.is_number_unsigned() && value.get<uint64_t>() <= maximum,
            field + " must be an unsigned integer in range");
    return value.get<uint64_t>();
}

bool is_binding(const Json& value) {
    return value.is_string() && value.get<std::string>().size() > 1 &&
           value.get<std::string>().front() == '$';
}

std::string binding_name(const Json& value) {
    return value.get<std::string>().substr(1);
}

void validate_literal(const Json& value, const std::string& type,
                      const std::string& field) {
    if (type == "uint64") {
        unsigned_value(value, field);
    } else if (type == "uint32") {
        unsigned_value(value, field, UINT32_MAX);
    } else if (type == "int32") {
        require(value.is_number_integer() &&
                    value.get<int64_t>() >= INT32_MIN &&
                    value.get<int64_t>() <= INT32_MAX,
                field + " must be int32");
    } else if (type == "bool") {
        require(value.is_boolean(), field + " must be boolean");
    } else if (type == "string") {
        required_text(value, field);
    } else if (type == "bool_list") {
        require(value.is_array() && !value.empty(),
                field + " must be a nonempty boolean array");
        for (const auto& item : value) {
            require(item.is_boolean(), field + " must contain booleans");
        }
    } else {
        throw std::invalid_argument("template-v2: unknown port type " + type);
    }
}

std::unordered_map<std::string, std::string> port_types(
    const TemplateDefinition& definition) {
    std::unordered_map<std::string, std::string> result;
    for (const auto& port : definition.ports) {
        result.emplace(port.name, port.type);
    }
    return result;
}

const std::unordered_map<std::string, std::string>& attribute_types(
    const std::string& kind) {
    static const std::unordered_map<std::string, std::string> compute = {
        {"duration_ns", "uint64"}, {"num_ops", "uint64"},
        {"tensor_size", "uint64"}, {"is_cpu_op", "bool"}};
    static const std::unordered_map<std::string, std::string> collective = {
        {"comm_type", "string"}, {"comm_size", "uint64"},
        {"comm_priority", "uint32"}, {"involved_dim", "bool_list"}};
    static const std::unordered_map<std::string, std::string> memory = {
        {"tensor_size", "uint64"}, {"tensor_loc", "uint32"},
        {"tensor_device", "uint32"}, {"tensor_channel", "uint32"}};
    static const std::unordered_map<std::string, std::string> point_to_point = {
        {"comm_size", "uint64"}, {"comm_src", "uint32"},
        {"comm_dst", "uint32"}, {"comm_tag", "uint32"}};
    static const std::unordered_map<std::string, std::string> empty;
    if (kind == "compute") return compute;
    if (kind == "collective") return collective;
    if (kind == "memory_load" || kind == "memory_store") return memory;
    if (kind == "send" || kind == "recv") return point_to_point;
    if (kind == "invalid") return empty;
    throw std::invalid_argument("template-v2: unknown leaf kind " + kind);
}

void validate_expression(const Json& expression, const std::string& expected,
                         const std::unordered_map<std::string, std::string>& ports,
                         const std::string& field) {
    if (!is_binding(expression)) {
        validate_literal(expression, expected, field);
        return;
    }
    const auto found = ports.find(binding_name(expression));
    require(found != ports.end(), field + " references an unknown binding");
    require(found->second == expected, field + " binding type mismatch");
}

TemplateDefinition parse_template(const Json& raw) {
    fields(raw, {"id", "ports", "nodes"}, "template");
    TemplateDefinition result;
    result.id = identifier(raw.at("id"), "template.id");
    require(raw.at("ports").is_array(), "template.ports must be an array");
    std::set<std::string> names;
    for (const auto& raw_port : raw.at("ports")) {
        fields(raw_port, {"name", "type"}, "template port");
        TemplatePort port{identifier(raw_port.at("name"), "port.name"),
                          required_text(raw_port.at("type"), "port.type")};
        validate_literal(port.type == "bool" ? Json(false) :
                             port.type == "string" ? Json("x") :
                             port.type == "bool_list" ? Json::array({true}) :
                             Json(0U),
                         port.type, "port.type");
        require(names.insert(port.name).second, "duplicate template port");
        result.ports.push_back(std::move(port));
    }
    require(raw.at("nodes").is_array() && !raw.at("nodes").empty(),
            "template.nodes must be a nonempty array");
    std::set<std::string> prior_nodes;
    for (const auto& raw_node : raw.at("nodes")) {
        require(raw_node.is_object() && raw_node.contains("kind"),
                "template node must contain kind");
        TemplateNodeDefinition node;
        node.kind = required_text(raw_node.at("kind"), "node.kind");
        if (node.kind == "call") {
            fields(raw_node, {"id", "kind", "deps", "template", "bindings"},
                   "call node");
            node.callee = identifier(raw_node.at("template"), "call.template");
            require(raw_node.at("bindings").is_object(),
                    "call.bindings must be an object");
            for (const auto& [name, value] : raw_node.at("bindings").items()) {
                node.bindings.emplace(name, value);
            }
        } else {
            fields(raw_node, {"id", "kind", "deps", "attrs"}, "leaf node");
            require(raw_node.at("attrs").is_object(), "leaf.attrs must be an object");
            for (const auto& [name, value] : raw_node.at("attrs").items()) {
                node.attributes.emplace(name, value);
            }
        }
        node.id = identifier(raw_node.at("id"), "node.id");
        require(prior_nodes.insert(node.id).second, "duplicate node id");
        require(raw_node.at("deps").is_array(), "node.deps must be an array");
        std::set<std::string> dependencies;
        for (const auto& dependency : raw_node.at("deps")) {
            const auto id = identifier(dependency, "node dependency");
            require(prior_nodes.count(id) != 0,
                    "node dependency is a forward or unknown reference");
            require(dependencies.insert(id).second, "duplicate node dependency");
            node.dependencies.push_back(id);
        }
        result.nodes.push_back(std::move(node));
    }
    return result;
}

void validate_definitions(TemplateDefinitionSet& definitions) {
    for (const auto& [template_id, definition] : definitions.templates) {
        const auto ports = port_types(definition);
        for (const auto& node : definition.nodes) {
            if (node.kind == "call") {
                const auto child = definitions.templates.find(node.callee);
                require(child != definitions.templates.end(),
                        "call references unknown template");
                const auto child_ports = port_types(child->second);
                require(node.bindings.size() == child_ports.size(),
                        "call bindings do not exactly match child ports");
                for (const auto& [name, type] : child_ports) {
                    const auto value = node.bindings.find(name);
                    require(value != node.bindings.end(),
                            "call is missing child binding");
                    validate_expression(value->second, type, ports,
                                        template_id + "." + node.id + "." + name);
                }
                continue;
            }
            const auto& expected = attribute_types(node.kind);
            require(node.attributes.size() == expected.size(),
                    "leaf attributes do not exactly match kind");
            for (const auto& [name, type] : expected) {
                const auto value = node.attributes.find(name);
                require(value != node.attributes.end(), "leaf attribute is missing");
                validate_expression(value->second, type, ports,
                                    template_id + "." + node.id + "." + name);
            }
        }
    }
    std::unordered_map<std::string, int> state;
    std::function<void(const std::string&)> visit = [&](const std::string& id) {
        require(state[id] != 1, "template call cycle");
        if (state[id] == 2) return;
        state[id] = 1;
        for (const auto& node : definitions.templates.at(id).nodes) {
            if (node.kind == "call") visit(node.callee);
        }
        state[id] = 2;
    };
    for (const auto& [id, definition] : definitions.templates) {
        (void)definition;
        visit(id);
    }
}

struct PlanExpansion {
    std::vector<uint64_t> roots;
    std::vector<uint64_t> terminals;
};

Json resolve_plan_expression(
    const Json& expression,
    const std::unordered_map<std::string, Json>& bindings) {
    if (!is_binding(expression)) return expression;
    return bindings.at(binding_name(expression));
}

PlanExpansion compile_template(
    const TemplateDefinitionSet& definitions,
    const TemplateDefinition& definition,
    const std::unordered_map<std::string, Json>& bindings,
    const std::vector<uint64_t>& incoming_dependencies,
    const std::string& path,
    TemplateExecutionPlan& plan) {
    plan.frame_templates.push_back(definition.id);
    std::unordered_map<std::string, PlanExpansion> expansions;
    std::set<std::string> has_child;
    PlanExpansion result;
    for (const auto& node : definition.nodes) {
        std::vector<uint64_t> dependencies;
        if (node.dependencies.empty()) {
            dependencies = incoming_dependencies;
        } else {
            for (const auto& dependency : node.dependencies) {
                const auto& parent = expansions.at(dependency);
                dependencies.insert(dependencies.end(), parent.terminals.begin(),
                                    parent.terminals.end());
                has_child.insert(dependency);
            }
        }
        PlanExpansion expansion;
        if (node.kind == "call") {
            std::unordered_map<std::string, Json> child_bindings;
            for (const auto& [name, expression] : node.bindings) {
                child_bindings.emplace(
                    name, resolve_plan_expression(expression, bindings));
            }
            expansion = compile_template(
                definitions, definitions.templates.at(node.callee),
                child_bindings, dependencies,
                path + "/" + node.id + ":" + node.callee, plan);
        } else {
            TemplateLeafPlan leaf;
            leaf.id = plan.leaves.size() + 1;
            leaf.name = path + "/" + node.id;
            leaf.kind = node.kind;
            leaf.parents = dependencies;
            for (const auto& [name, expression] : node.attributes) {
                leaf.attributes.emplace(
                    name, resolve_plan_expression(expression, bindings));
            }
            plan.leaves.push_back(std::move(leaf));
            expansion.roots.push_back(plan.leaves.back().id);
            expansion.terminals.push_back(plan.leaves.back().id);
        }
        if (node.dependencies.empty()) {
            result.roots.insert(result.roots.end(), expansion.roots.begin(),
                                expansion.roots.end());
        }
        expansions.emplace(node.id, std::move(expansion));
    }
    for (const auto& node : definition.nodes) {
        if (has_child.count(node.id) == 0) {
            const auto& terminals = expansions.at(node.id).terminals;
            result.terminals.insert(result.terminals.end(), terminals.begin(),
                                    terminals.end());
        }
    }
    return result;
}

TemplateExecutionPlan compile_plan(
    const TemplateDefinitionSet& definitions,
    const TemplateDefinition& root) {
    TemplateExecutionPlan plan;
    plan.root_template = root.id;
    std::unordered_map<std::string, Json> root_bindings;
    for (const auto& port : root.ports) {
        root_bindings.emplace(port.name, "$" + port.name);
    }
    plan.roots = compile_template(
        definitions, root, root_bindings, {}, root.id, plan).roots;
    for (auto& leaf : plan.leaves) {
        for (const auto parent_id : leaf.parents) {
            plan.leaves.at(parent_id - 1).children.push_back(leaf.id);
        }
    }
    return plan;
}

}  // namespace

bool TemplateRegistry::DefinitionFileIdentity::operator==(
    const DefinitionFileIdentity& other) const {
    return device == other.device && inode == other.inode &&
           size == other.size && modified_seconds == other.modified_seconds &&
           modified_nanoseconds == other.modified_nanoseconds;
}

std::shared_ptr<const TemplateDefinitionSet> TemplateRegistry::loadDefinition(
    const std::string& path) {
    struct stat file_status {};
    require(stat(path.c_str(), &file_status) == 0 &&
                S_ISREG(file_status.st_mode),
            "definition path must identify a readable regular file");
#if defined(__APPLE__)
    const auto modified_seconds = file_status.st_mtimespec.tv_sec;
    const auto modified_nanoseconds = file_status.st_mtimespec.tv_nsec;
#else
    const auto modified_seconds = file_status.st_mtim.tv_sec;
    const auto modified_nanoseconds = file_status.st_mtim.tv_nsec;
#endif
    const DefinitionFileIdentity file_identity{
        static_cast<uint64_t>(file_status.st_dev),
        static_cast<uint64_t>(file_status.st_ino),
        static_cast<uint64_t>(file_status.st_size),
        static_cast<int64_t>(modified_seconds),
        static_cast<int64_t>(modified_nanoseconds)};
    const auto cached = definitions_.find(path);
    if (cached != definitions_.end()) {
        require(cached->second.file_identity == file_identity,
                "cached definition path changed during the run");
        return cached->second.definition;
    }
    constexpr std::size_t kMaximumCachedDefinitions = 64;
    require(definitions_.size() < kMaximumCachedDefinitions,
            "definition cache capacity exceeded");
    const Json raw = Wire::read(path);
    fields(raw, {"schema_version", "template_id", "definition_digest",
                 "tier_manifest_digest", "service_binding_digest",
                 "service_activation_id", "templates"}, "definition");
    require(raw.at("schema_version") == "template-definition-v2",
            "unsupported definition schema_version");
    auto result = std::make_shared<TemplateDefinitionSet>();
    result->id = identifier(raw.at("template_id"), "template_id");
    result->digest = required_text(raw.at("definition_digest"), "definition_digest");
    result->tier_manifest_digest =
        required_text(raw.at("tier_manifest_digest"), "tier_manifest_digest");
    result->service_binding_digest =
        required_text(raw.at("service_binding_digest"), "service_binding_digest");
    result->service_activation_id =
        required_text(raw.at("service_activation_id"), "service_activation_id");
    require(raw.at("templates").is_array() && !raw.at("templates").empty(),
            "templates must be a nonempty array");
    for (const auto& raw_template : raw.at("templates")) {
        auto definition = parse_template(raw_template);
        require(result->templates.emplace(definition.id, std::move(definition)).second,
                "duplicate template id");
    }
    const Json identity = {
        {"schema_version", raw.at("schema_version")},
        {"template_id", raw.at("template_id")},
        {"tier_manifest_digest", raw.at("tier_manifest_digest")},
        {"service_binding_digest", raw.at("service_binding_digest")},
        {"service_activation_id", raw.at("service_activation_id")},
        {"templates", raw.at("templates")}};
    require(Wire::digest(identity, true) == result->digest,
            "definition_digest mismatch");
    validate_definitions(*result);
    for (const auto& [id, definition] : result->templates) {
        result->plans.emplace(id, compile_plan(*result, definition));
        ++plan_compilation_count_;
    }
    definitions_.emplace(
        path, CachedDefinition{file_identity, result});
    ++definition_load_count_;
    return result;
}

TemplateInvocation TemplateRegistry::loadInvocation(
    const std::string& path, uint32_t expected_rank,
    const TemplateDefinitionSet& definition) {
    const Json raw = Wire::read(path);
    fields(raw, {"schema_version", "definition_id", "definition_digest", "ranks"},
           "invocation");
    require(raw.at("schema_version") == "template-invocation-v2",
            "unsupported invocation schema_version");
    require(raw.at("definition_id") == definition.id &&
                raw.at("definition_digest") == definition.digest,
            "invocation definition identity mismatch");
    require(raw.at("ranks").is_array(), "invocation.ranks must be an array");
    const Json* selected = nullptr;
    std::set<uint32_t> ranks;
    for (const auto& rank : raw.at("ranks")) {
        fields(rank, {"rank", "root_template", "bindings"}, "rank invocation");
        const auto rank_id = static_cast<uint32_t>(
            unsigned_value(rank.at("rank"), "rank", UINT32_MAX));
        require(ranks.insert(rank_id).second, "duplicate invocation rank");
        const auto root_name = identifier(rank.at("root_template"), "root_template");
        const auto root = definition.templates.find(root_name);
        require(root != definition.templates.end(), "unknown root template");
        require(rank.at("bindings").is_object(),
                "rank bindings must be an object");
        const auto expected_ports = port_types(root->second);
        require(rank.at("bindings").size() == expected_ports.size(),
                "rank bindings do not exactly match root ports");
        for (const auto& [name, type] : expected_ports) {
            require(rank.at("bindings").contains(name), "missing root binding");
            const auto& value = rank.at("bindings").at(name);
            require(!is_binding(value),
                    "root bindings cannot reference other bindings");
            validate_literal(value, type, "root binding " + name);
        }
        if (rank_id == expected_rank) selected = &rank;
    }
    require(selected != nullptr, "invocation does not contain expected rank");
    TemplateInvocation invocation{
        definition.id, definition.digest, expected_rank,
        required_text(selected->at("root_template"), "root_template"), {}};
    for (const auto& [name, value] : selected->at("bindings").items()) {
        invocation.bindings.emplace(name, value);
    }
    ++invocation_count_;
    return invocation;
}

uint64_t TemplateRegistry::definitionLoadCount() const {
    return definition_load_count_;
}
uint64_t TemplateRegistry::invocationCount() const { return invocation_count_; }
uint64_t TemplateRegistry::planCompilationCount() const {
    return plan_compilation_count_;
}
uint64_t TemplateRegistry::activeFrameCount() const {
    return active_frame_count_;
}
uint64_t TemplateRegistry::peakFrameCount() const { return peak_frame_count_; }
uint64_t TemplateRegistry::materializedLeafCount() const {
    return materialized_leaf_count_;
}
uint64_t TemplateRegistry::peakMaterializedLeafCount() const {
    return peak_materialized_leaf_count_;
}
void TemplateRegistry::acquireFrames(uint64_t count) {
    require(count <= std::numeric_limits<uint64_t>::max() - active_frame_count_,
            "active frame counter overflow");
    active_frame_count_ += count;
    peak_frame_count_ = std::max(peak_frame_count_, active_frame_count_);
}
void TemplateRegistry::releaseFrames(uint64_t count) {
    require(count <= active_frame_count_, "active frame counter underflow");
    active_frame_count_ -= count;
}
void TemplateRegistry::acquireMaterializedLeaf() {
    require(materialized_leaf_count_ != std::numeric_limits<uint64_t>::max(),
            "materialized leaf counter overflow");
    ++materialized_leaf_count_;
    peak_materialized_leaf_count_ =
        std::max(peak_materialized_leaf_count_, materialized_leaf_count_);
}
void TemplateRegistry::releaseMaterializedLeaf() {
    require(materialized_leaf_count_ > 0,
            "materialized leaf counter underflow");
    --materialized_leaf_count_;
}

std::string format_template_metrics_line(uint32_t rank,
                                         const TemplateRegistry& registry,
                                         uint64_t et_read_count) {
    std::ostringstream line;
    line << "TEMPLATE_V2_METRICS"
         << " rank=" << rank
         << " definition_load_count=" << registry.definitionLoadCount()
         << " plan_compilation_count=" << registry.planCompilationCount()
         << " invocation_count=" << registry.invocationCount()
         << " et_read_count=" << et_read_count
         << " active_frames=" << registry.activeFrameCount()
         << " peak_frames=" << registry.peakFrameCount()
         << " materialized_leaves=" << registry.materializedLeafCount()
         << " peak_materialized_leaves="
         << registry.peakMaterializedLeafCount();
    return line.str();
}

}  // namespace AstraSim
