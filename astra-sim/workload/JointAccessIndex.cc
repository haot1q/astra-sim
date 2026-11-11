/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/
#include "astra-sim/workload/JointAccessIndex.hh"
#include "astra-sim/workload/JointAccessIo.hh"
#include "astra-sim/system/MemoryTierRegistry.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "protoio.hh"

#include <algorithm>
#include <map>
#include <set>
#include <stdexcept>
#include <tuple>
#include <vector>

namespace AstraSim {
namespace {
using namespace JointAccessIo;
using ServiceBindingJson::array;
using ServiceBindingJson::fields;

void require(bool condition, const char* message) {
    if (!condition) throw std::invalid_argument(std::string("joint access: ") + message);
}

void table(const Json& entries, uint64_t maximum) {
    require(array(entries).size() <= maximum, "table record budget");
    for (size_t i = 0; i < entries.size(); ++i) {
        require(u64(entries[i].at("ref")) == i, "table ref must equal its index");
    }
}

const Json& reference(const Json& entries, const Json& ref) {
    const auto index = u64(ref);
    require(index < entries.size(), "unknown table reference");
    return entries[static_cast<size_t>(index)];
}

uint64_t start(const Json& operand, uint64_t index) {
    require(index < u64(operand.at("count"), true), "run index outside operand");
    const auto result = static_cast<__int128>(u64(operand.at("offset_bytes"))) +
        static_cast<__int128>(integer(operand.at("stride_bytes"))) * index;
    require(result >= 0 && result <= UINT64_MAX, "operand stride overflow");
    return static_cast<uint64_t>(result);
}

void validate_backings(const Json& backings) {
    std::set<std::string> identities;
    for (const auto& item : backings) {
        fields(item, {"ref", "backing_identity", "extent_bytes", "catalog_generation"});
        const auto& identity = item.at("backing_identity");
        fields(identity, {"profile_id", "owner_scope", "allocation_id"});
        for (const auto& value : identity) text(value);
        u64(item.at("extent_bytes"), true);
        if (!item.at("catalog_generation").is_null()) text(item.at("catalog_generation"));
        require(identities.insert(identity.dump()).second, "duplicate backing identity");
    }
}

void validate_operands(const Json& document, const JointAccessLimits& limits) {
    std::set<std::string> calls;
    for (const auto& item : document.at("invocations")) {
        fields(item, {"ref", "batch_id", "invocation_id"});
        require(calls.insert(Json({text(item.at("batch_id")), text(item.at("invocation_id"))}).dump()).second,
                "duplicate original invocation");
    }
    using Interval = std::pair<uint64_t, uint64_t>;
    std::map<std::string, std::vector<Interval>> ordinals;
    uint64_t count = 0;
    for (const auto& item : document.at("operands")) {
        fields(item, {"ref", "invocation_ref", "source_boundary", "execution_layer", "role",
            "origin", "operation", "ordinal_start", "backing_ref", "offset_bytes",
            "length_bytes", "stride_bytes", "count"});
        reference(document.at("invocations"), item.at("invocation_ref"));
        text(item.at("source_boundary")); text(item.at("role")); u64(item.at("execution_layer"));
        auto origin = text(item.at("origin"));
        auto operation = text(item.at("operation"));
        require((operation == "read" && (origin == "reads" || origin == "captured_page_reads")) ||
                (operation == "write" && origin == "writes"), "operand origin/direction mismatch");
        auto occurrences = u64(item.at("count"), true);
        count = add(count, occurrences);
        require(count <= limits.table_records, "expanded occurrence budget");
        auto length = u64(item.at("length_bytes"), true);
        auto extent = u64(reference(document.at("backings"), item.at("backing_ref")).at("extent_bytes"), true);
        require(add(std::max(start(item, 0), start(item, occurrences - 1)), length) <= extent,
                "operand outside original backing");
        auto ordinal = u64(item.at("ordinal_start"));
        auto key = Json({item.at("invocation_ref"), item.at("source_boundary"),
                         item.at("execution_layer"), item.at("origin")}).dump();
        ordinals[key].push_back({ordinal, add(ordinal, occurrences)});
    }
    for (auto& [key, ranges] : ordinals) {
        (void)key;
        std::sort(ranges.begin(), ranges.end());
        for (size_t i = 1; i < ranges.size(); ++i) {
            require(ranges[i].first >= ranges[i - 1].second, "overlapping operand ordinals");
        }
    }
}

void authorization(const Json& value) {
    fields(value, {"kind", "owner", "page_id", "residency_version"});
    const auto kind = text(value.at("kind"));
    require(kind == "workspace" || kind == "fixed_weight" || kind == "page", "unknown authorization kind");
    const auto& owner = value.at("owner");
    if (owner.is_array()) {
        require(owner.size() == 3, "authorization owner tuple length");
        for (const auto& part : owner) text(part);
    } else { text(owner); }
    if (kind == "page") {
        text(value.at("page_id")); u64(value.at("residency_version"));
    } else {
        require(value.at("page_id").is_null() && value.at("residency_version").is_null(),
                "non-Page authorization carries Page identity");
    }
}

void validate_pieces(const Json& document) {
    using Key = std::pair<uint64_t, uint64_t>;
    std::map<Key, std::vector<std::pair<uint64_t, uint64_t>>> coverage;
    for (const auto& piece : document.at("pieces")) {
        fields(piece, {"ref", "operand_ref", "run_index", "offset_bytes", "length_bytes",
            "location", "authorization", "volume_ordinal"});
        const auto& operand = reference(document.at("operands"), piece.at("operand_ref"));
        auto index = u64(piece.at("run_index"));
        auto offset = u64(piece.at("offset_bytes"));
        auto end = add(offset, u64(piece.at("length_bytes"), true));
        require(offset >= start(operand, index) &&
                end <= add(start(operand, index), u64(operand.at("length_bytes"))),
                "piece outside original occurrence");
        text(piece.at("location")); authorization(piece.at("authorization"));
        if (operand.at("origin") == "captured_page_reads") {
            require(piece.at("authorization").at("kind") == "page" &&
                    !piece.at("volume_ordinal").is_null(), "captured Page access lost original volume identity");
        }
        if (!piece.at("volume_ordinal").is_null()) u64(piece.at("volume_ordinal"));
        coverage[{u64(piece.at("operand_ref")), index}].push_back({offset, end});
    }
    for (const auto& operand : document.at("operands")) {
        for (uint64_t i = 0; i < u64(operand.at("count")); ++i) {
            auto& intervals = coverage[{u64(operand.at("ref")), i}];
            std::sort(intervals.begin(), intervals.end());
            auto offset = start(operand, i);
            for (auto [first, end] : intervals) {
                require(first == offset, "piece coverage has overlap or gap"); offset = end;
            }
            require(offset == add(start(operand, i), u64(operand.at("length_bytes"))),
                    "piece coverage incomplete");
        }
    }
}

std::string node_type(const Json& entry) {
    auto type = text(entry.at("node_type"));
    require(type == "MEM_LOAD_NODE" || type == "MEM_STORE_NODE", "invalid memory node type");
    return type;
}

void validate_bindings(const Json& document, uint32_t rank) {
    std::vector<unsigned char> uses(document.at("pieces").size(), 0);
    std::set<uint64_t> nodes;
    std::set<std::string> refs;
    for (const auto& binding : array(document.at("bindings"))) {
        fields(binding, {"ref", "final_node_id", "node_type", "row", "operation", "segment_index",
            "location", "bytes", "piece_refs"});
        auto operation = text(binding.at("operation"));
        require(node_type(binding) == (operation == "read" ? "MEM_LOAD_NODE" : "MEM_STORE_NODE") &&
                (operation == "read" || operation == "write"), "binding direction mismatch");
        const auto canonical = "r" + std::to_string(rank) + "/row" + std::to_string(u64(binding.at("row"))) +
            "/" + operation + "/s" + std::to_string(u64(binding.at("segment_index")));
        require(text(binding.at("ref")) == canonical && refs.insert(canonical).second &&
                nodes.insert(u64(binding.at("final_node_id"))).second, "duplicate/noncanonical binding");
        uint64_t total = 0;
        for (const auto& ref : array(binding.at("piece_refs"))) {
            const auto& piece = reference(document.at("pieces"), ref);
            require(!uses[u64(ref)], "piece bound more than once"); uses[u64(ref)] = 1;
            const auto& operand = reference(document.at("operands"), piece.at("operand_ref"));
            require(piece.at("location") == binding.at("location") && operand.at("operation") == operation,
                    "binding endpoint/direction mismatch");
            total = add(total, u64(piece.at("length_bytes"), true));
        }
        require(total == u64(binding.at("bytes"), true), "binding byte bill mismatch");
        text(binding.at("location"));
    }
    for (const auto& entry : array(document.at("memory_unassociated"))) {
        fields(entry, {"final_node_id", "node_type", "location", "bytes", "reason"});
        node_type(entry); text(entry.at("location")); u64(entry.at("bytes"));
        require(entry.at("reason") == "outside_selected_source" &&
                nodes.insert(u64(entry.at("final_node_id"))).second, "duplicate/invalid unassociated memory");
    }
    for (const auto& entry : array(document.at("source_unassociated"))) {
        fields(entry, {"piece_ref", "reason"});
        reference(document.at("pieces"), entry.at("piece_ref"));
        require(entry.at("reason") == "no_existing_mem_lowering" ||
                entry.at("reason") == "outside_selected_segment", "unsupported source omission");
        auto ref = u64(entry.at("piece_ref"));
        require(!uses[ref], "piece duplicated between binding and omission"); uses[ref] = 2;
    }
    std::map<std::pair<uint64_t, uint64_t>, unsigned char> qualification;
    for (size_t i = 0; i < uses.size(); ++i) {
        require(uses[i] != 0, "captured piece absent from coverage inventory");
        const auto& piece = document.at("pieces")[i];
        auto [it, inserted] = qualification.emplace(std::make_pair(u64(piece.at("operand_ref")),
            u64(piece.at("run_index"))), uses[i]);
        require(inserted || it->second == uses[i], "partially associated occurrence");
    }
}
}  // namespace

std::unique_ptr<JointAccessIndex> JointAccessIndex::load_and_validate(
    const std::string& path, std::shared_ptr<JointSourceContext> context,
    uint32_t rank, uint32_t iteration, const MemoryTierRegistry* tiers) {
    require(context && context->admits_rank(rank), "rank not admitted by expected context");
    auto result = std::unique_ptr<JointAccessIndex>(new JointAccessIndex());
    result->et_identity_ = file_identity(path);
    std::string raw;
    result->document_ = read_json(path + ".joint-access.json", context->limits(),
                                  context->limits().association_bytes, raw);
    const auto& document = result->document_;
    auto kind = text(document.at("kind"));
    if (kind == "business") {
        fields(document, {"schema", "kind", "execution", "source_profile", "source_selection_sha256",
            "association_input_sha256", "invocations", "backings", "operands", "pieces", "bindings",
            "source_unassociated", "memory_unassociated"});
        hash(document.at("association_input_sha256"));
    } else {
        fields(document, {"schema", "kind", "execution", "source_profile", "source_selection_sha256", "control"});
        require(kind == "bootstrap" || kind == "idle" || kind == "dep_forward" || kind == "dep_finish",
                "unknown control kind");
    }
    require(document.at("schema") == "joint-access-companion-v1", "unsupported companion version");
    const auto& execution = document.at("execution");
    fields(execution, {"run_id", "source_provenance_digest", "rank", "execution_iteration"});
    require(execution.at("run_id") == context->identity().at("run_id") &&
            execution.at("source_provenance_digest") == context->identity().at("source_provenance_digest") &&
            document.at("source_profile") == context->identity().at("source_profile") &&
            document.at("source_selection_sha256") == context->identity().at("source_selection_sha256") &&
            u64(execution.at("rank")) == rank && u64(execution.at("execution_iteration")) == iteration,
            "execution identity differs from actual owner");
    result->rank_ = rank; result->iteration_ = iteration;
    uint64_t records = 0;
    if (kind == "business") {
        result->validate_business(context->limits());
        for (auto key : {"invocations", "backings", "operands", "pieces", "bindings", "source_unassociated", "memory_unassociated"}) {
            records = add(records, document.at(key).size());
        }
        result->charge_pieces_ = document.at("pieces").size();
    }
    result->charge_bytes_ = add(raw.size(), multiply(records, 128));
    context->reserve(result->charge_bytes_, result->charge_pieces_);
    result->context_ = std::move(context);
    result->validate_et(path, tiers, ServiceBindingJson::digest_bytes(raw).substr(7));
    result->verify_file_identity(path);
    return result;
}

void JointAccessIndex::verify_file_identity(const std::string& path) const {
    require(file_identity(path) == et_identity_, "ET replaced after validation");
}

void JointAccessIndex::validate_business(const JointAccessLimits& limits) {
    uint64_t count = 0;
    for (auto key : {"invocations", "backings", "operands"}) {
        table(document_.at(key), limits.table_records);
        count = add(count, document_.at(key).size());
    }
    require(count <= limits.table_records, "combined table budget");
    table(document_.at("pieces"), limits.pieces);
    require(array(document_.at("bindings")).size() <= limits.et_nodes &&
            array(document_.at("memory_unassociated")).size() <= limits.et_nodes &&
            array(document_.at("source_unassociated")).size() <= limits.pieces,
            "binding/omission budget");
    validate_backings(document_.at("backings"));
    validate_operands(document_, limits);
    validate_pieces(document_);
    validate_bindings(document_, rank_);
}

JointAccessIndex::~JointAccessIndex() {
    if (context_) context_->release(charge_bytes_, charge_pieces_);
}

void JointAccessIndex::probe_legacy(const std::string& path) {
    ProtoInputStream stream(path);
    ChakraProtoMsg::GlobalMetadata metadata;
    if (!stream.read(metadata)) return;
    for (const auto& attr : metadata.attr()) {
        require(attr.name() != "joint_association_version" && attr.name() != "joint_companion_sha256",
                "associated ET requires explicit joint source context");
    }
}

void JointAccessIndex::reject_legacy_reference(const ChakraProtoMsg::Node& node) {
    for (const auto& attr : node.attr()) {
        require(attr.name() != "joint_access_ref", "source reference requires explicit joint source context");
    }
}

namespace {
const ChakraProtoMsg::AttributeProto& attribute(const Attributes& attrs, const char* key) {
    const auto found = attrs.find(key);
    require(found != attrs.end(), "missing required protobuf attribute");
    return *found->second;
}

std::pair<uint32_t, uint32_t> memory_node(const ChakraProtoMsg::Node& node,
    const Json& binding, const MemoryTierRegistry* tiers) {
    require(tiers != nullptr, "business memory requires original memory tier registry");
    auto attrs = attributes(node.attr());
    const auto& tier = attribute(attrs, "tensor_loc");
    const auto& device = attribute(attrs, "tensor_device");
    const auto& bytes = attribute(attrs, "tensor_size");
    require(tier.has_uint32_val() && device.has_uint32_val() && bytes.has_uint64_val(),
            "memory node attribute types");
    tiers->at(tier.uint32_val(), device.uint32_val());
    const auto location = tiers->tier_name(tier.uint32_val()) + ":" + std::to_string(device.uint32_val());
    require(location == binding.at("location") && bytes.uint64_val() == u64(binding.at("bytes")) &&
            ChakraProtoMsg::NodeType_Name(node.type()) == binding.at("node_type"),
            "actual memory node differs from association");
    return {tier.uint32_val(), device.uint32_val()};
}

void control_node(const ChakraProtoMsg::Node& node, const Json& document,
                  const JointSourceContext& context, uint32_t rank, uint32_t iteration) {
    const auto kind = text(document.at("kind"));
    const auto& control = document.at("control");
    require(node.data_deps_size() == 0 && node.ctrl_deps_size() == 0 &&
            node.start_time_micros() == 0 && !node.has_inputs() && !node.has_outputs() &&
            node.GetReflection()->GetUnknownFields(node).empty(), "control graph has extra fields");
    auto attrs = attributes(node.attr());
    if (kind == "bootstrap" || kind == "idle") {
        fields(control, {"duration_ns"});
        const auto duration = u64(control.at("duration_ns"), true);
        require((kind == "bootstrap" ? iteration == 0 : iteration > 0) &&
                node.name() == "COMP_NODE_event_" + std::to_string(duration) + "ns" &&
                node.type() == ChakraProtoMsg::COMP_NODE && node.duration_micros() == duration && attrs.empty(),
                "event control shape or iteration mismatch");
        return;
    }
    fields(control, {"rank_group", "payload_bytes", "involved_dim", "pg_name"});
    std::set<uint32_t> group;
    uint64_t previous = 0;
    for (const auto& value : array(control.at("rank_group"))) {
        auto current = u64(value);
        require(current <= UINT32_MAX && context.admits_rank(static_cast<uint32_t>(current)) &&
                (group.empty() || current > previous), "invalid control rank group");
        group.insert(static_cast<uint32_t>(current)); previous = current;
    }
    auto payload = kind == "dep_forward" ? multiply(16, group.size()) : 8;
    require(group.size() == context.rank_count() && group.count(rank) && iteration > 0 &&
            u64(control.at("payload_bytes")) == payload &&
            node.name() == "COMM_COLL_NODE_" + kind + "_ALLREDUCE" &&
            node.type() == ChakraProtoMsg::COMM_COLL_NODE && node.duration_micros() == 0,
            "collective control identity or payload mismatch");
    const auto& comm = attribute(attrs, "comm_type");
    const auto& size = attribute(attrs, "comm_size");
    const auto& dims = attribute(attrs, "involved_dim");
    require(comm.has_int64_val() && comm.int64_val() == ChakraProtoMsg::ALL_REDUCE &&
            size.has_int64_val() && size.int64_val() >= 0 && static_cast<uint64_t>(size.int64_val()) == payload &&
            dims.has_bool_list(), "collective control attributes");
    const auto& declared = array(control.at("involved_dim"));
    require(!declared.empty() && declared.size() == static_cast<size_t>(dims.bool_list().values_size()),
            "control dimensions mismatch");
    bool involved = false;
    for (size_t i = 0; i < declared.size(); ++i) {
        require(declared[i].is_boolean() && declared[i].get<bool>() == dims.bool_list().values(i),
                "control dimension value mismatch");
        involved = involved || declared[i].get<bool>();
    }
    require(involved, "control has no involved dimension");
    if (control.at("pg_name").is_null()) {
        require(attrs.size() == 3, "control has extra attributes");
    } else {
        const auto& pg = attribute(attrs, "pg_name");
        require(attrs.size() == 4 && pg.has_string_val() && pg.string_val() == text(control.at("pg_name")),
                "control group name mismatch");
    }
}
}  // namespace

void JointAccessIndex::validate_et(const std::string& path, const MemoryTierRegistry* tiers,
                                  const std::string& companion_hash) {
    const bool business = document_.at("kind") == "business";
    std::unordered_map<uint64_t, const Json*> inventory;
    if (business) {
        for (auto key : {"bindings", "memory_unassociated"}) {
            for (const auto& item : document_.at(key)) inventory.emplace(u64(item.at("final_node_id")), &item);
        }
    }
    uint64_t control_count = 0;
    auto metadata = [&](const ChakraProtoMsg::GlobalMetadata& header) {
        auto attrs = attributes(header.attr());
        const auto& version = attribute(attrs, "joint_association_version");
        const auto& digest = attribute(attrs, "joint_companion_sha256");
        require(version.has_string_val() && version.string_val() == "1" && digest.has_string_val() &&
                digest.string_val() == companion_hash, "ET companion version/hash mismatch");
    };
    auto consume = [&](const ChakraProtoMsg::Node& node) {
        auto attrs = attributes(node.attr());
        if (!business) {
            require(++control_count == 1, "control graph must contain exactly one node");
            control_node(node, document_, *context_, rank_, iteration_);
            return;
        }
        const bool memory = node.type() == ChakraProtoMsg::MEM_LOAD_NODE || node.type() == ChakraProtoMsg::MEM_STORE_NODE;
        if (!memory) { require(!attrs.count("joint_access_ref"), "non-memory node carries source reference"); return; }
        auto found = inventory.find(node.id());
        require(found != inventory.end(), "final MEM absent from coverage inventory");
        const auto& entry = *found->second;
        auto [tier, device] = memory_node(node, entry, tiers);
        if (entry.contains("ref")) {
            const auto& ref = attribute(attrs, "joint_access_ref");
            require(ref.has_string_val() && ref.string_val() == entry.at("ref"), "final MEM reference mismatch");
            nodes_.emplace(node.id(), JointNodeAccess{&document_, &entry, tier, device});
        } else {
            require(!attrs.count("joint_access_ref"), "unassociated MEM carries source reference");
        }
        inventory.erase(found);
    };
    scan_et(path, context_->limits(), metadata, consume);
    require(inventory.empty(), "declared memory node absent from final ET");
}

const JointNodeAccess* JointAccessIndex::lookup(const ChakraProtoMsg::Node& node,
                                               uint32_t rank, uint32_t iteration) const {
    require(rank == rank_ && iteration == iteration_, "query uses stale feeder execution context");
    auto found = nodes_.find(node.id());
    if (found == nodes_.end()) {
        reject_legacy_reference(node);
        return nullptr;
    }
    const auto& entry = found->second;
    auto attrs = attributes(node.attr());
    const auto& ref = attribute(attrs, "joint_access_ref");
    const auto& tier = attribute(attrs, "tensor_loc");
    const auto& device = attribute(attrs, "tensor_device");
    const auto& bytes = attribute(attrs, "tensor_size");
    require(ref.has_string_val() && ref.string_val() == entry.binding->at("ref") &&
            tier.has_uint32_val() && tier.uint32_val() == entry.tier_id &&
            device.has_uint32_val() && device.uint32_val() == entry.device_id &&
            bytes.has_uint64_val() && bytes.uint64_val() == u64(entry.binding->at("bytes")) &&
            ChakraProtoMsg::NodeType_Name(node.type()) == entry.binding->at("node_type"),
            "actual issue node differs from validated binding");
    return &entry;
}
}  // namespace AstraSim
