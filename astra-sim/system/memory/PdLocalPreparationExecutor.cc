/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#include "PdLocalPreparationExecutor.hh"

#include <stdexcept>
#include <utility>

#include "MemoryPreparationTrace.hh"
#include "MemoryPreparationWire.hh"
#include "PipelinePreparationIdentity.hh"
#include "ServiceBindingJson.hh"
#include "astra-sim/system/Sys.hh"
#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"

namespace AstraSim {
namespace {
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;

std::string text(const Json& value) {
    if (!value.is_string() || value.get<std::string>().empty()) {
        throw std::invalid_argument("preparation requires a nonempty string");
    }
    return value.get<std::string>();
}

std::string attribute(const std::shared_ptr<Chakra::ETFeederNode>& node,
                      const std::string& name) {
    if (!node->has_other_attr(name) || !node->get_other_attr(name).has_string_val()) {
        throw std::invalid_argument("preparation node lacks string attribute " + name);
    }
    return node->get_other_attr(name).string_val();
}

Json identity(const Json& work) {
    Json result;
    for (const auto* field : {"run_id", "attempt_id", "transfer_id", "preparation_id",
                             "instance_id", "manifest_digest", "service_binding_digest",
                             "service_activation_id"}) {
        result[field] = text(work.at(field));
    }
    return result;
}

void validate_node(const std::shared_ptr<Chakra::ETFeederNode>& node,
                   const Json& owner, const std::set<std::string>& rank_events) {
    for (const auto& pair : std::vector<std::pair<std::string, std::string>>{
            {"movement_run_id", "run_id"}, {"movement_instance_id", "instance_id"},
            {"memory_movement_manifest_digest", "manifest_digest"}}) {
        if (attribute(node, pair.first) != owner.at(pair.second)) {
            throw std::invalid_argument("preparation node/envelope identity mismatch");
        }
    }
    if (attribute(node, "memory_movement_schema_version") != "memory-events-v1") {
        throw std::invalid_argument("unsupported preparation node contract");
    }
    if (!node->has_other_attr("movement_dependencies") ||
        !node->get_other_attr("movement_dependencies").has_string_list()) {
        throw std::invalid_argument("preparation node requires explicit movement dependencies");
    }
    std::set<std::string> dependencies;
    for (const auto& dep : node->get_other_attr("movement_dependencies").string_list().values()) {
        if (!rank_events.count(dep) || !dependencies.insert(dep).second) {
            throw std::invalid_argument("preparation dependency is duplicate or outside rank work");
        }
    }
}
}  // namespace

PdLocalPreparationExecutor::PdLocalPreparationExecutor(const std::vector<Sys*>& systems,
        PhysicalServiceFactory* services, const MemoryTierConfigSet* memory)
    : systems_(systems), services_(services) {
    if ((services == nullptr) != (memory == nullptr)) {
        throw std::invalid_argument("Cache preparation needs both services and memory");
    }
    if (services) cache_ = std::make_unique<PdCachePreparation>(systems, *services, *memory);
    if (systems.empty()) throw std::invalid_argument("preparation requires actual systems");
    for (std::size_t rank = 0; rank < systems.size(); ++rank) {
        if (systems[rank] == nullptr || systems[rank]->id != static_cast<int>(rank)) {
            throw std::invalid_argument("preparation systems must match their actual ranks");
        }
    }
}

std::vector<PdLocalPreparationExecutor::Submission>
PdLocalPreparationExecutor::read_work(const std::string& path) const {
    const auto work = Wire::read(path);
    const bool direct =
        work.at("schema_version") == "pd-local-preparation-v3";
    const bool staged = direct
        ? work.contains("pipeline_stage") || work.contains("endpoint")
        : work.at("schema_version") == "pd-local-preparation-v2";
    if (direct &&
        work.contains("pipeline_stage") != work.contains("endpoint")) {
        throw std::invalid_argument(
            "direct preparation requires stage and endpoint together");
    }
    auto envelope = work;
    if (staged) {
        envelope.erase("pipeline_stage");
        envelope.erase("endpoint");
    }
    Wire::fields(
        envelope,
        {"schema_version", "run_id", "attempt_id", "transfer_id",
         "preparation_id", "instance_id", "manifest_digest",
         "service_binding_digest", "service_activation_id",
         "backend_instance_id",
         direct ? "rank_events" : "rank_traces"});
    if (!direct && !staged &&
        work.at("schema_version") != "pd-local-preparation-v1") {
        throw std::invalid_argument("unsupported preparation work schema");
    }
    const auto owner = identity(work);
    const auto instance = Wire::uint32(work.at("backend_instance_id"));
    if (!services_) throw std::invalid_argument("preparation requires actual physical services");
    const auto stage = staged ? validate_pipeline_preparation_identity(work, *services_)
                              : PipelinePreparationIdentity{};
    std::vector<Submission> submissions;
    std::set<uint32_t> ranks;
    std::set<std::string> physical_ids;
    std::set<std::string> expected_logical;
    const auto& rank_work =
        work.at(direct ? "rank_events" : "rank_traces");
    for (const auto& item : Wire::array(rank_work)) {
        if (direct) {
            Wire::fields(item, {"rank", "event_path", "events"});
        } else {
            Wire::fields(item, {"rank", "trace_path", "events"});
        }
        const auto rank = Wire::uint32(item.at("rank"));
        if (rank >= systems_.size() || (!ranks.empty() && rank <= *ranks.rbegin())) {
            throw std::invalid_argument("preparation ranks must be actual, sorted, and unique");
        }
        ranks.insert(rank);
        const auto actual = services_->configuration().ranks.find(rank);
        if (actual == services_->configuration().ranks.end() ||
            actual->second.at("instance_id") != instance) {
            throw std::invalid_argument("preparation rank belongs to a different actual instance");
        }
        std::map<std::string, std::string> logical_by_physical;
        std::set<std::string> logical_ids;
        std::set<std::string> rank_events;
        for (const auto& event : Wire::array(item.at("events"))) {
            Wire::fields(event, {"logical_event_id", "physical_event_id"});
            const auto physical = text(event.at("physical_event_id"));
            const auto logical = text(event.at("logical_event_id"));
            if (!physical_ids.insert(physical).second || !logical_ids.insert(logical).second) {
                throw std::invalid_argument("duplicate preparation event mapping");
            }
            logical_by_physical.emplace(physical, logical);
            rank_events.insert(physical);
        }
        if (expected_logical.empty()) expected_logical = logical_ids;
        if (logical_ids != expected_logical) {
            throw std::invalid_argument("preparation rank has an incomplete logical event set");
        }
        auto& system = *systems_.at(rank);
        system.validate_tier_manifest_digest(
            text(owner.at("manifest_digest")));
        system.validate_service_metadata(
            text(owner.at("service_binding_digest")),
            text(owner.at("service_activation_id")), rank);
        std::set<std::string> observed;
        std::vector<std::shared_ptr<Chakra::ETFeederNode>> nodes;
        if (direct) {
            const auto sidecar = Wire::read(text(item.at("event_path")));
            if (text(sidecar.at("run_id")) != owner.at("run_id") ||
                text(sidecar.at("instance_id")) != owner.at("instance_id") ||
                text(sidecar.at("manifest_digest")) !=
                    owner.at("manifest_digest")) {
                throw std::invalid_argument(
                    "direct preparation event/envelope identity mismatch");
            }
            for (const auto& event : Wire::array(sidecar.at("events"))) {
                nodes.push_back(direct_movement_node(
                    sidecar, event, rank, nodes.size() + 1));
            }
            if (nodes.size() != rank_events.size()) {
                throw std::invalid_argument(
                    "direct preparation event set cardinality differs");
            }
        } else {
            const auto trace = read_memory_preparation_trace(
                text(item.at("trace_path")), rank_events.size());
            if (trace.pipeline_stage_digest != stage.digest ||
                trace.rank != rank ||
                trace.manifest_digest != owner.at("manifest_digest") ||
                trace.binding_digest != owner.at("service_binding_digest") ||
                trace.activation_id != owner.at("service_activation_id")) {
                throw std::invalid_argument(
                    "preparation ET/envelope metadata mismatch");
            }
            nodes = trace.nodes;
        }
        for (const auto& node : nodes) {
            const auto event_id = attribute(node, "movement_event_id");
            if (!rank_events.count(event_id) || !observed.insert(event_id).second) {
                throw std::invalid_argument("preparation ET event mapping mismatch");
            }
            validate_node(node, owner, rank_events);
            submissions.push_back({rank, logical_by_physical.at(event_id), event_id, owner, node});
        }
        if (observed != rank_events) throw std::invalid_argument("preparation event set incomplete");
    }
    if (submissions.empty()) throw std::invalid_argument("empty preparation work");
    if (staged && std::vector<uint32_t>(ranks.begin(), ranks.end()) != stage.ranks) {
        throw std::invalid_argument("preparation work does not cover the exact stage TP ranks");
    }
    return submissions;
}

bool PdLocalPreparationExecutor::submit_command(const std::string& command) {
    for (const auto* marker : {"pd-cache-prepare\t", "pd-cache-cancel\t"}) {
        if (command.rfind(marker, 0) != 0) continue;
        rethrow_failure();
        const auto argument = command.substr(std::char_traits<char>::length(marker));
        if (!cache_ || argument.empty() || argument.find_first_of("\t\r\n") != std::string::npos) {
            throw std::invalid_argument("invalid or unavailable Cache preparation command");
        }
        if (std::string(marker) == "pd-cache-prepare\t") cache_->submit(argument);
        else cache_->cancel(argument);
        return true;
    }
    constexpr const char* prefix = "pd-local-prepare\t";
    if (command.rfind(prefix, 0) != 0) return false;
    rethrow_failure();
    const auto path = command.substr(std::char_traits<char>::length(prefix));
    if (path.empty() || path.find_first_of("\t\r\n") != std::string::npos) {
        throw std::invalid_argument("malformed pd-local-prepare command");
    }
    submit(path);
    return true;
}

void PdLocalPreparationExecutor::submit(const std::string& path) {
    auto submissions = read_work(path);
    const auto& owner = submissions.front().identity;
    const std::string key = Json::array({owner.at("run_id"), owner.at("attempt_id"),
        owner.at("transfer_id"), owner.at("preparation_id")}).dump();
    if (used_preparations_.count(key)) throw std::invalid_argument("duplicate preparation work");
    for (const auto& submission : submissions) {
        if (used_event_ids_.count(submission.physical_event_id)) {
            throw std::invalid_argument("preparation physical event identity was reused");
        }
    }
    used_preparations_.insert(key);
    try {
        std::set<uint32_t> ranks;
        for (const auto& submission : submissions) {
            used_event_ids_.insert(submission.physical_event_id);
            pending_.emplace(submission.physical_event_id, submission);
            systems_.at(submission.rank)->memory_movement_executor->submit_preparation(
                submission.node, {*this, text(owner.at("preparation_id"))});
            ranks.insert(submission.rank);
        }
        for (const auto rank : ranks) systems_.at(rank)->memory_movement_executor->dispatch();
    } catch (...) {
        failure_ = std::current_exception();
        throw;
    }
}

void PdLocalPreparationExecutor::complete_memory_preparation(
    const MemoryPreparationReceipt& receipt) {
    try {
        const auto found = pending_.find(receipt.movement.event_id);
        if (found == pending_.end() ||
            receipt.preparation_id != found->second.identity.at("preparation_id")) {
            throw std::invalid_argument("unknown preparation callback identity");
        }
        const auto& submission = found->second;
        auto envelope = submission.identity;
        envelope["schema_version"] = "pd-local-preparation-receipt-v1";
        envelope["rank"] = submission.rank;
        envelope["logical_event_id"] = submission.logical_event_id;
        envelope["movement"] = Json::parse(receipt.serialized_movement);
        emit_memory_protocol_line("PD_LOCAL_PREPARATION_COMPLETE " + envelope.dump());
        pending_.erase(found);
        ++completed_count_;
    } catch (...) {
        failure_ = std::current_exception();
        throw;
    }
}

bool PdLocalPreparationExecutor::drained() const {
    return !failure_ && pending_.empty() && (!cache_ || cache_->drained());
}

void PdLocalPreparationExecutor::rethrow_failure() const {
    if (failure_) std::rethrow_exception(failure_);
    if (cache_) cache_->rethrow_failure();
}

}  // namespace AstraSim
