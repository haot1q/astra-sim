/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/MemoryMovementExecutor.hh"

#include <algorithm>
#include <cerrno>
#include <climits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include <unistd.h>

#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/memory/UcieTransport.hh"
#include "astra-sim/workload/Workload.hh"
#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"
#include "extern/helper/json/json.hpp"

namespace AstraSim {
namespace {

using Chakra::ETFeederNode;
using ChakraProtoMsg::AttributeProto;
using json = nlohmann::json;

enum class MovementStage {
    SourceEndpoint,
    Intermediate,
    DestinationEndpoint,
};

class MovementHandlerData : public WorkloadLayerHandlerData {
  public:
    std::string event_id;
    uint32_t tier_id = 0;
    MovementStage stage = MovementStage::SourceEndpoint;
    std::size_t segment_index = 0;
    std::size_t hop_index = 0;
    uint64_t billed_bytes = 0;
};

json endpoint_timing_json(const MemoryEndpointTiming& timing) {
    return {
        {"tier_id", timing.tier_id},
        {"device_id", timing.device_id},
        {"operation", timing.operation},
        {"ready_ns", timing.ready_ns},
        {"start_ns", timing.start_ns},
        {"finish_ns", timing.finish_ns},
        {"queue_wait_ns", timing.start_ns - timing.ready_ns},
        {"service_ns", timing.finish_ns - timing.start_ns},
    };
}

json segment_timing_json(const MemoryPathSegmentTiming& timing) {
    return {
        {"segment_id", timing.segment_id},
        {"kind", timing.kind},
        {"resource_ref", timing.resource_ref},
        {"operation", timing.operation},
        {"logical_bytes", timing.logical_bytes},
        {"billed_bytes", timing.billed_bytes},
        {"ready_ns", timing.ready_ns},
        {"start_ns", timing.start_ns},
        {"finish_ns", timing.finish_ns},
        {"queue_wait_ns", timing.queue_wait_ns},
        {"service_ns", timing.service_ns},
    };
}

const AttributeProto& required_attr(const std::shared_ptr<ETFeederNode>& node,
                                    const std::string& name) {
    if (!node->has_other_attr(name)) {
        throw std::invalid_argument(
            "movement node is missing required attribute '" + name + "'");
    }
    return node->get_other_attr(name);
}

std::string string_attr(const std::shared_ptr<ETFeederNode>& node,
                        const std::string& name) {
    const auto& attr = required_attr(node, name);
    if (!attr.has_string_val() || attr.string_val().empty()) {
        throw std::invalid_argument("movement attribute '" + name +
                                    "' must be a non-empty string");
    }
    return attr.string_val();
}

uint32_t uint32_attr(const std::shared_ptr<ETFeederNode>& node,
                     const std::string& name) {
    const auto& attr = required_attr(node, name);
    if (!attr.has_uint32_val()) {
        throw std::invalid_argument("movement attribute '" + name +
                                    "' must be uint32");
    }
    return attr.uint32_val();
}

std::vector<std::string> string_list_attr(
    const std::shared_ptr<ETFeederNode>& node, const std::string& name) {
    const auto& attr = required_attr(node, name);
    if (!attr.has_string_list()) {
        throw std::invalid_argument("movement attribute '" + name +
                                    "' must be a string list");
    }
    return {attr.string_list().values().begin(),
            attr.string_list().values().end()};
}

json receipt_json(const DmaReceipt& receipt,
                  const std::string& run_id,
                  const std::string& instance_id,
                  const std::string& manifest_digest,
                  const DmaSchedulerConfig& scheduler_config,
                  uint64_t overlap_compute_ns,
                  uint64_t overlap_compute_bytes,
                  const std::optional<uint64_t>& exposed_to_dependent_ns,
                  const std::optional<std::string>& page_id,
                  const std::optional<std::string>& transaction_id,
                  const std::optional<uint32_t>& expected_residency_version,
                  const std::optional<uint32_t>& home_domain_id,
                  const std::string& path_schema_version,
                  const std::string& path_contract_status,
                  const std::string& timing_provenance,
                  const std::vector<MemoryPathSegmentTiming>& segment_timings,
                  const MemoryEndpointTiming& source_endpoint,
                  const MemoryEndpointTiming& destination_endpoint) {
    json payload = {
        {"schema_version", kMemoryMovementReceiptSchemaVersion},
        {"run_id", run_id},
        {"instance_id", instance_id},
        {"manifest_digest", manifest_digest},
        {"event_id", receipt.event_id},
        {"source_iteration_id", receipt.source_iteration_id},
        {"ready_ns", receipt.ready_ns},
        {"start_ns", receipt.start_ns},
        {"finish_ns", receipt.finish_ns},
        {"queue_wait_ns", receipt.queue_wait_ns},
        {"service_ns", receipt.service_ns},
        {"bytes", receipt.bytes},
        {"kind", receipt.kind},
        {"phase", receipt.phase},
        {"priority_class", dma_priority_class_name(receipt.priority)},
        {"path_id", receipt.path_id},
        {"path_schema_version", path_schema_version},
        {"path_contract_status", path_contract_status},
        {"timing_provenance", timing_provenance},
        {"engine_id", receipt.engine_id},
        {"engine_count", scheduler_config.engine_count},
        {"max_priority_burst", scheduler_config.max_priority_burst},
        {"max_in_flight_page_movements",
         scheduler_config.max_in_flight_page_movements},
        {"resource_ids", receipt.resource_ids},
        {"overlap_compute_ns", overlap_compute_ns},
        {"overlap_compute_bytes", overlap_compute_bytes},
        {"overlap_bytes_basis", "time_proportional_logical_v1"},
        {"exposed_to_dependent_ns", exposed_to_dependent_ns.has_value()
                                        ? json(*exposed_to_dependent_ns)
                                        : json(nullptr)},
        {"status", "completed"},
        {"endpoint_timings",
         json::array({endpoint_timing_json(source_endpoint),
                      endpoint_timing_json(destination_endpoint)})},
        {"segment_timings", json::array()},
    };
    for (const auto& timing : segment_timings) {
        payload["segment_timings"].push_back(segment_timing_json(timing));
    }
    if (page_id.has_value()) {
        payload["page_id"] = *page_id;
        payload["transaction_id"] = *transaction_id;
        payload["expected_residency_version"] =
            *expected_residency_version;
        payload["home_domain_id"] = *home_domain_id;
    }
    return payload;
}

uint64_t time_proportional_bytes(uint64_t bytes,
                                 uint64_t overlap_ns,
                                 uint64_t service_ns) {
    if (service_ns == 0 || overlap_ns == 0) {
        return 0;
    }
    const auto product = static_cast<unsigned __int128>(bytes) * overlap_ns;
    return std::min(
        bytes, static_cast<uint64_t>(product / service_ns));
}

void emit_protocol_line(const std::string& content) {
    const std::string line = content + "\n";
    const long reported_limit = ::fpathconf(STDOUT_FILENO, _PC_PIPE_BUF);
    const std::size_t atomic_limit =
        reported_limit > 0 ? static_cast<std::size_t>(reported_limit)
                           : static_cast<std::size_t>(PIPE_BUF);
    if (line.size() > atomic_limit) {
        throw std::length_error(
            "memory movement protocol line exceeds atomic pipe capacity");
    }
    while (true) {
        const ssize_t written =
            ::write(STDOUT_FILENO, line.data(), line.size());
        if (written == static_cast<ssize_t>(line.size())) {
            return;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        throw std::runtime_error(
            "failed to atomically write memory movement protocol line");
    }
}

}  // namespace

MemoryMovementExecutor::MemoryMovementExecutor(Sys* sys) : sys_(sys) {
    if (sys_ == nullptr) {
        throw std::invalid_argument("memory movement executor requires Sys");
    }
}

bool MemoryMovementExecutor::is_movement_node(
    const std::shared_ptr<ETFeederNode>& node) const {
    return node->has_other_attr("memory_movement_schema_version");
}

bool MemoryMovementExecutor::submit(const std::shared_ptr<ETFeederNode>& node,
                                    Workload* workload) {
    if (workload == nullptr) {
        throw std::invalid_argument("movement submission requires Workload");
    }
    if (string_attr(node, "memory_movement_schema_version") !=
        kMemoryMovementSchemaVersion) {
        throw std::invalid_argument("unsupported memory movement schema");
    }
    const std::string manifest_digest =
        string_attr(node, "memory_movement_manifest_digest");
    sys_->validate_tier_manifest_digest(manifest_digest);
    const std::string run_id = string_attr(node, "movement_run_id");
    if (run_id_.empty()) {
        run_id_ = run_id;
        manifest_digest_ = manifest_digest;
    } else if (run_id_ != run_id || manifest_digest_ != manifest_digest) {
        throw std::invalid_argument(
            "memory movement run identity changed within one simulation");
    }
    const std::string kind = string_attr(node, "movement_kind");
    if (kind != "load" && kind != "store" && kind != "page_promote" &&
        kind != "page_demote") {
        throw std::invalid_argument("unsupported memory movement kind");
    }
    const bool page_movement =
        kind == "page_promote" || kind == "page_demote";
    std::optional<std::string> page_id;
    std::optional<std::string> transaction_id;
    std::optional<uint32_t> expected_residency_version;
    std::optional<uint32_t> home_domain_id;
    if (page_movement) {
        page_id = string_attr(node, "movement_page_id");
        transaction_id = string_attr(node, "movement_transaction_id");
        expected_residency_version =
            uint32_attr(node, "movement_expected_residency_version");
        home_domain_id = uint32_attr(node, "movement_home_domain_id");
    } else if (node->has_other_attr("movement_page_id") ||
               node->has_other_attr("movement_transaction_id") ||
               node->has_other_attr("movement_expected_residency_version") ||
               node->has_other_attr("movement_home_domain_id")) {
        throw std::invalid_argument(
            "non-page movement must not carry page identity");
    }
    const std::string path_id = string_attr(node, "movement_path_id");
    const bool has_path_contract =
        node->has_other_attr("movement_path_contract_status");
    const std::string path_contract_status = has_path_contract
        ? string_attr(node, "movement_path_contract_status")
        : "compatibility_checkpoint";
    const std::string path_schema_version = has_path_contract
        ? string_attr(node, "movement_path_schema_version")
        : "adr-0020-checkpoint";
    const std::string timing_provenance = has_path_contract
        ? string_attr(node, "movement_path_timing_provenance")
        : "proxy_unimplemented";
    const uint32_t engine_count = uint32_attr(node, "movement_engine_count");
    const uint32_t max_priority_burst =
        uint32_attr(node, "movement_max_priority_burst");
    const uint32_t max_in_flight_page_movements =
        uint32_attr(node, "movement_max_in_flight_page_movements");
    const auto resource_ids =
        string_list_attr(node, "movement_resource_ids");
    if (path_contract_status == "implemented") {
        if (path_schema_version != "movement-path-v1" ||
            (timing_provenance != "estimated" &&
             timing_provenance != "measured")) {
            throw std::invalid_argument(
                "implemented movement path has invalid schema or provenance");
        }
        if (sys_->movement_paths.empty() ||
            sys_->movement_paths.selected_id() != path_id) {
            throw std::invalid_argument(
                "ET movement path does not match the runtime manifest selection");
        }
        const auto& capability =
            sys_->movement_paths.capability(path_id);
        if (capability.engine_count != engine_count ||
            capability.max_priority_burst != max_priority_burst ||
            capability.max_in_flight_page_movements !=
                max_in_flight_page_movements ||
            capability.timing_provenance != timing_provenance) {
            throw std::invalid_argument(
                "ET movement path capability differs from runtime manifest");
        }
        const auto segment_ids =
            string_list_attr(node, "movement_segment_ids");
        const auto segment_kinds =
            string_list_attr(node, "movement_segment_kinds");
        const auto segment_refs =
            string_list_attr(node, "movement_segment_resource_refs");
        const auto segment_operations =
            string_list_attr(node, "movement_segment_operations");
        const auto segment_byte_rules =
            string_list_attr(node, "movement_segment_byte_rules");
        const auto segment_count = capability.segments.size();
        if (segment_ids.size() != segment_count ||
            segment_kinds.size() != segment_count ||
            segment_refs.size() != segment_count ||
            segment_operations.size() != segment_count ||
            segment_byte_rules.size() != segment_count) {
            throw std::invalid_argument(
                "ET movement path segment cardinality differs from manifest");
        }
        std::vector<std::string> expected_resources = {"lpddr_read"};
        for (std::size_t index = 0; index < segment_count; ++index) {
            const auto& segment = capability.segments[index];
            if (segment_ids[index] != segment.id ||
                segment_kinds[index] != segment.kind ||
                segment_refs[index] != segment.resource_ref ||
                segment_operations[index] != segment.operation ||
                segment_byte_rules[index] != segment.byte_rule) {
                throw std::invalid_argument(
                    "ET movement path segments differ from runtime manifest");
            }
            expected_resources.push_back(
                segment.id + ":" + segment.resource_ref);
        }
        expected_resources.push_back("hbm_write");
        if (resource_ids != expected_resources) {
            throw std::invalid_argument(
                "ET movement resource bill differs from runtime manifest");
        }
    } else if (path_contract_status == "compatibility_checkpoint") {
        if (!sys_->movement_paths.empty()) {
            throw std::invalid_argument(
                "runtime manifest path cannot use compatibility checkpoint ET");
        }
        if (path_schema_version != "adr-0020-checkpoint" ||
            timing_provenance != "proxy_unimplemented") {
            throw std::invalid_argument(
                "compatibility checkpoint must remain proxy_unimplemented");
        }
    } else {
        throw std::invalid_argument(
            "unsupported movement path contract status");
    }
    if (scheduler_ == nullptr) {
        scheduler_ = std::make_unique<DmaScheduler>(
            DmaSchedulerConfig{path_id, engine_count, max_priority_burst,
                               max_in_flight_page_movements});
    } else if (scheduler_->config().selected_path_id != path_id ||
               scheduler_->config().engine_count != engine_count ||
               scheduler_->config().max_priority_burst != max_priority_burst ||
               scheduler_->config().max_in_flight_page_movements !=
                   max_in_flight_page_movements) {
        throw std::invalid_argument(
            "movement scheduler capability changed within one run");
    }

    const std::string event_id = string_attr(node, "movement_event_id");
    const std::string phase = string_attr(node, "movement_phase");
    if (phase != "critical_line" && phase != "background_fill" &&
        phase != "whole_object") {
        throw std::invalid_argument("unsupported memory movement phase");
    }
    const bool foreground = phase != "background_fill";
    if (!foreground && !node->getChildren().empty()) {
        throw std::invalid_argument(
            "background movement must not release ET compute nodes");
    }
    DmaJob job{
        event_id,
        uint32_attr(node, "movement_source_iteration_id"),
        Sys::boostedTick(),
        node->tensor_size(),
        kind,
        phase,
        parse_dma_priority_class(string_attr(node, "movement_priority_class")),
        path_id,
        node->tensor_loc(),
        node->tensor_device(),
        uint32_attr(node, "movement_destination_tier_id"),
        uint32_attr(node, "movement_destination_device_id"),
        resource_ids,
        string_list_attr(node, "movement_dependencies"),
        foreground,
        page_movement,
    };
    if (page_movement &&
        (*home_domain_id != job.source_device_id ||
         *home_domain_id != job.destination_device_id)) {
        throw std::invalid_argument(
            "movement home_domain_id must match paired device IDs");
    }
    scheduler_->submit(std::move(job));
    const auto inserted = submissions_.emplace(
        event_id, Submission{
                      node->id(),
                      workload,
                      foreground,
                      run_id,
                      string_attr(node, "movement_instance_id"),
                      manifest_digest,
                      page_id,
                      transaction_id,
                      expected_residency_version,
                      home_domain_id,
                      path_schema_version,
                      path_contract_status,
                      timing_provenance,
                      0,
                      std::nullopt,
                      std::nullopt,
                      0,
                      0,
                      0,
                      {},
                      std::nullopt,
                      std::nullopt,
                  });
    if (!inserted.second) {
        throw std::invalid_argument("duplicate movement submission context");
    }
    return foreground;
}

void MemoryMovementExecutor::dispatch() {
    if (scheduler_ == nullptr) {
        return;
    }
    scheduler_->validate_dependencies();
    for (const auto& dispatch :
         scheduler_->dispatch_ready(Sys::boostedTick())) {
        start_source_read(dispatch);
    }
}

void MemoryMovementExecutor::start_source_read(const DmaDispatch& dispatch) {
    auto* data = new MovementHandlerData;
    data->sys_id = sys_->id;
    data->completion_target = this;
    data->event_id = dispatch.job.event_id;
    data->tier_id = dispatch.job.source_tier_id;
    data->device_id = dispatch.job.source_device_id;
    data->stage = MovementStage::SourceEndpoint;
    try {
        sys_->memory_api(dispatch.job.source_tier_id,
                         dispatch.job.source_device_id)
            ->issue({dispatch.job.bytes, MemoryOperation::Read}, data);
    } catch (...) {
        delete data;
        throw;
    }
}

void MemoryMovementExecutor::start_next_segment(
    const std::string& event_id) {
    auto& submission = submissions_.at(event_id);
    if (submission.path_contract_status != "implemented") {
        start_destination_write(event_id);
        return;
    }
    const auto active = scheduler_->active_job(event_id);
    const auto& capability =
        sys_->movement_paths.capability(active.path_id);
    if (submission.next_segment_index == capability.segments.size()) {
        start_destination_write(event_id);
        return;
    }
    submission.active_segment_ready_ns.reset();
    submission.active_segment_start_ns.reset();
    submission.active_segment_billed_bytes = 0;
    submission.active_segment_queue_wait_ns = 0;
    submission.active_segment_service_ns = 0;
    start_segment_hop(event_id, submission.next_segment_index, 0);
}

void MemoryMovementExecutor::start_segment_hop(
    const std::string& event_id,
    std::size_t segment_index,
    std::size_t hop_index) {
    const auto active = scheduler_->active_job(event_id);
    const auto& capability =
        sys_->movement_paths.capability(active.path_id);
    if (segment_index >= capability.segments.size()) {
        throw std::out_of_range("movement segment index is out of range");
    }
    const auto& segment = capability.segments[segment_index];
    auto* data = new MovementHandlerData;
    data->sys_id = sys_->id;
    data->completion_target = this;
    data->event_id = event_id;
    data->device_id = active.source_device_id;
    data->stage = MovementStage::Intermediate;
    data->segment_index = segment_index;
    data->hop_index = hop_index;
    try {
        if (segment.kind == "bandwidth_resource") {
            if (hop_index != 0) {
                throw std::logic_error(
                    "bandwidth segment has more than one hop");
            }
            const auto& resource =
                sys_->movement_resource(segment.resource_ref);
            if (active.source_device_id >= resource.stack_count) {
                throw std::out_of_range(
                    "movement resource home domain is out of range");
            }
            const auto operation = segment.operation == "read"
                ? MemoryOperation::Read
                : MemoryOperation::Write;
            data->billed_bytes = active.bytes;
            resource.api->issue({active.bytes, operation}, data);
            return;
        }
        if (segment.kind != "ucie_transaction") {
            throw std::invalid_argument(
                "unsupported movement path segment kind");
        }
        const auto& link = sys_->ucie_link(segment.resource_ref);
        if (active.source_device_id >= link.stack_count) {
            throw std::out_of_range(
                "movement UCIe home domain is out of range");
        }
        const auto operation = segment.operation == "read"
            ? MemoryOperation::Read
            : MemoryOperation::Write;
        const auto hops = ucie_transaction_hops(
            operation, active.bytes, link.header_bytes);
        if (hop_index >= hops.size()) {
            throw std::out_of_range("movement UCIe hop is out of range");
        }
        data->billed_bytes = hops[hop_index].bytes;
        link.api->issue(
            {hops[hop_index].bytes, hops[hop_index].operation}, data);
    } catch (...) {
        delete data;
        throw;
    }
}

void MemoryMovementExecutor::start_destination_write(
    const std::string& event_id) {
    const auto& submission = submissions_.at(event_id);
    (void)submission;
    const auto active = scheduler_->active_job(event_id);
    auto* data = new MovementHandlerData;
    data->sys_id = sys_->id;
    data->completion_target = this;
    data->event_id = event_id;
    data->tier_id = active.destination_tier_id;
    data->device_id = active.destination_device_id;
    data->stage = MovementStage::DestinationEndpoint;
    try {
        sys_->memory_api(active.destination_tier_id,
                         active.destination_device_id)
            ->issue({active.bytes, MemoryOperation::Write}, data);
    } catch (...) {
        delete data;
        throw;
    }
}

void MemoryMovementExecutor::finish(const std::string& event_id) {
    const DmaReceipt receipt =
        scheduler_->complete(event_id, Sys::boostedTick());
    const Submission submission = submissions_.at(event_id);
    if (!submission.source_endpoint.has_value() ||
        !submission.destination_endpoint.has_value()) {
        throw std::logic_error(
            "movement completed without both endpoint timings");
    }
    const uint64_t overlap_compute_ns =
        compute_overlap_ns(receipt.start_ns, receipt.finish_ns);
    const uint64_t overlap_compute_bytes = time_proportional_bytes(
        receipt.bytes, overlap_compute_ns, receipt.service_ns);
    const std::optional<uint64_t> exposed_to_dependent_ns =
        submission.foreground
            ? submission.workload->movement_exposed_to_dependent_ns(
                  submission.node_id, receipt.ready_ns, receipt.finish_ns)
            : std::nullopt;
    emit_protocol_line(
        "MEMORY_MOVEMENT_COMPLETE " +
        receipt_json(receipt, submission.run_id, submission.instance_id,
                     submission.manifest_digest, scheduler_->config(),
                     overlap_compute_ns, overlap_compute_bytes,
                     exposed_to_dependent_ns, submission.page_id,
                     submission.transaction_id,
                     submission.expected_residency_version,
                     submission.home_domain_id,
                     submission.path_schema_version,
                     submission.path_contract_status,
                     submission.timing_provenance,
                     submission.segment_timings,
                     *submission.source_endpoint,
                     *submission.destination_endpoint)
            .dump());
    submissions_.erase(event_id);
    if (submission.foreground) {
        submission.workload->complete_memory_movement(submission.node_id);
    }
    dispatch();
    if (scheduler_->drained()) {
        emit_protocol_line("MEMORY_MOVEMENT_DRAINED");
    }
}

void MemoryMovementExecutor::call(EventType type, CallData* data) {
    (void)type;
    if (data == nullptr) {
        throw std::invalid_argument("invalid memory movement callback data");
    }
    auto* movement = static_cast<MovementHandlerData*>(data);
    const std::string event_id = movement->event_id;
    if (!(movement->memory_ready_ns <= movement->memory_start_ns &&
          movement->memory_start_ns <= movement->memory_finish_ns &&
          movement->memory_finish_ns == Sys::boostedTick())) {
        delete movement;
        throw std::logic_error("invalid analytical memory endpoint timing");
    }
    auto& submission = submissions_.at(event_id);
    const auto stage = movement->stage;
    const auto segment_index = movement->segment_index;
    const auto hop_index = movement->hop_index;
    const auto billed_bytes = movement->billed_bytes;
    if (stage == MovementStage::SourceEndpoint ||
        stage == MovementStage::DestinationEndpoint) {
        const MemoryEndpointTiming timing{
            movement->tier_id,
            movement->device_id,
            stage == MovementStage::SourceEndpoint ? "read" : "write",
            movement->memory_ready_ns,
            movement->memory_start_ns,
            movement->memory_finish_ns,
        };
        if (stage == MovementStage::SourceEndpoint) {
            submission.source_endpoint = timing;
        } else {
            submission.destination_endpoint = timing;
        }
    } else {
        if (segment_index != submission.next_segment_index) {
            delete movement;
            throw std::logic_error(
                "movement segment completion arrived out of order");
        }
        if (!submission.active_segment_ready_ns.has_value()) {
            submission.active_segment_ready_ns = movement->memory_ready_ns;
            submission.active_segment_start_ns = movement->memory_start_ns;
        }
        submission.active_segment_billed_bytes += billed_bytes;
        submission.active_segment_queue_wait_ns +=
            movement->memory_start_ns - movement->memory_ready_ns;
        submission.active_segment_service_ns +=
            movement->memory_finish_ns - movement->memory_start_ns;
    }
    delete movement;
    if (stage == MovementStage::SourceEndpoint) {
        start_next_segment(event_id);
    } else if (stage == MovementStage::DestinationEndpoint) {
        finish(event_id);
    } else {
        const auto active = scheduler_->active_job(event_id);
        const auto& segment = sys_->movement_paths
            .capability(active.path_id)
            .segments[segment_index];
        std::size_t hop_count = 1;
        if (segment.kind == "ucie_transaction") {
            const auto& link = sys_->ucie_link(segment.resource_ref);
            const auto operation = segment.operation == "read"
                ? MemoryOperation::Read
                : MemoryOperation::Write;
            hop_count = ucie_transaction_hops(
                operation, active.bytes, link.header_bytes).size();
        }
        if (hop_index + 1 < hop_count) {
            start_segment_hop(event_id, segment_index, hop_index + 1);
            return;
        }
        if (!submission.active_segment_ready_ns.has_value() ||
            !submission.active_segment_start_ns.has_value()) {
            throw std::logic_error("movement segment timing was not initialized");
        }
        submission.segment_timings.push_back(
            {segment.id,
             segment.kind,
             segment.resource_ref,
             segment.operation,
             active.bytes,
             submission.active_segment_billed_bytes,
             *submission.active_segment_ready_ns,
             *submission.active_segment_start_ns,
             Sys::boostedTick(),
             submission.active_segment_queue_wait_ns,
             submission.active_segment_service_ns});
        ++submission.next_segment_index;
        start_next_segment(event_id);
    }
}

bool MemoryMovementExecutor::drained() const {
    return scheduler_ == nullptr || scheduler_->drained();
}

void MemoryMovementExecutor::record_compute_start(uint64_t node_id) {
    if (!active_compute_nodes_.insert(node_id).second) {
        throw std::logic_error("compute node started more than once");
    }
    if (active_compute_nodes_.size() == 1) {
        active_compute_union_start_ns_ = Sys::boostedTick();
    }
}

void MemoryMovementExecutor::record_compute_finish(uint64_t node_id) {
    if (active_compute_nodes_.erase(node_id) != 1) {
        throw std::logic_error("completion for unknown compute node");
    }
    if (!active_compute_nodes_.empty()) {
        return;
    }
    if (!active_compute_union_start_ns_.has_value() ||
        *active_compute_union_start_ns_ > Sys::boostedTick()) {
        throw std::logic_error("invalid compute interval state");
    }
    compute_intervals_.emplace_back(*active_compute_union_start_ns_,
                                    Sys::boostedTick());
    active_compute_union_start_ns_.reset();
}

uint64_t MemoryMovementExecutor::compute_overlap_ns(uint64_t start_ns,
                                                    uint64_t finish_ns) const {
    if (finish_ns < start_ns) {
        throw std::invalid_argument("compute overlap interval is reversed");
    }
    uint64_t overlap_ns = 0;
    const auto add_overlap = [&](uint64_t compute_start,
                                 uint64_t compute_finish) {
        const uint64_t overlap_start = std::max(start_ns, compute_start);
        const uint64_t overlap_finish = std::min(finish_ns, compute_finish);
        if (overlap_finish > overlap_start) {
            overlap_ns += overlap_finish - overlap_start;
        }
    };
    for (const auto& interval : compute_intervals_) {
        if (interval.first >= finish_ns) {
            break;
        }
        add_overlap(interval.first, interval.second);
    }
    if (active_compute_union_start_ns_.has_value()) {
        add_overlap(*active_compute_union_start_ns_, finish_ns);
    }
    if (overlap_ns > finish_ns - start_ns) {
        throw std::logic_error("compute overlap exceeds movement service time");
    }
    return overlap_ns;
}

}  // namespace AstraSim
