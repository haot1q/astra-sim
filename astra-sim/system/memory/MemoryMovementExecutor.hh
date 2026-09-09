/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __MEMORY_MOVEMENT_EXECUTOR_HH__
#define __MEMORY_MOVEMENT_EXECUTOR_HH__

#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/memory/DmaScheduler.hh"

namespace Chakra {
class ETFeederNode;
}

namespace AstraSim {

class Sys;
class Workload;

// One shared bounded/atomic writer for ordinary and preparation movement IPC.
void emit_memory_protocol_line(const std::string& content);

struct MemoryPreparationReceipt {
    std::string preparation_id;
    DmaReceipt movement;
    // The unchanged, complete v1 receipt, including endpoint and path timings.
    std::string serialized_movement;
};

/** Run-owned preparation callback. It must outlive every accepted submission.
 * No Workload/compute dependency is created by external preparation work.
 */
class MemoryPreparationOwner {
  public:
    virtual ~MemoryPreparationOwner() = default;
    virtual void complete_memory_preparation(
        const MemoryPreparationReceipt& receipt) = 0;
};

struct WorkloadMovementCompletion {
    std::reference_wrapper<Workload> workload;
};

struct ExternalPreparationCompletion {
    std::reference_wrapper<MemoryPreparationOwner> owner;
    std::string preparation_id;
};

using MovementCompletionOwner =
    std::variant<WorkloadMovementCompletion, ExternalPreparationCompletion>;

struct MemoryEndpointTiming {
    uint32_t tier_id;
    uint32_t device_id;
    std::string operation;
    uint64_t ready_ns;
    uint64_t start_ns;
    uint64_t finish_ns;
};

struct MemoryPathSegmentTiming {
    std::string segment_id;
    std::string kind;
    std::string resource_ref;
    std::string operation;
    uint64_t logical_bytes;
    uint64_t billed_bytes;
    uint64_t ready_ns;
    uint64_t start_ns;
    uint64_t finish_ns;
    uint64_t queue_wait_ns;
    uint64_t service_ns;
};

class MemoryMovementExecutor : public Callable {
  public:
    explicit MemoryMovementExecutor(Sys* sys);

    bool is_movement_node(
        const std::shared_ptr<Chakra::ETFeederNode>& node) const;
    bool submit(const std::shared_ptr<Chakra::ETFeederNode>& node,
                Workload* workload);
    void submit_preparation(const std::shared_ptr<Chakra::ETFeederNode>& node,
                            ExternalPreparationCompletion completion);
    void dispatch();
    void call(EventType type, CallData* data) override;
    bool drained() const;
    void rethrow_failure() const;
    void record_compute_start(uint64_t node_id);
    void record_compute_finish(uint64_t node_id);

  private:
    struct Submission {
        uint64_t node_id;
        MovementCompletionOwner completion;
        bool foreground;
        std::string run_id;
        std::string instance_id;
        std::string manifest_digest;
        std::optional<std::string> page_id;
        std::optional<std::string> transaction_id;
        std::optional<uint32_t> expected_residency_version;
        std::optional<uint32_t> home_domain_id;
        std::string path_schema_version;
        std::string path_contract_status;
        std::string timing_provenance;
        std::size_t next_segment_index;
        std::optional<uint64_t> active_segment_ready_ns;
        std::optional<uint64_t> active_segment_start_ns;
        uint64_t active_segment_billed_bytes;
        uint64_t active_segment_queue_wait_ns;
        uint64_t active_segment_service_ns;
        std::vector<MemoryPathSegmentTiming> segment_timings;
        std::optional<MemoryEndpointTiming> source_endpoint;
        std::optional<MemoryEndpointTiming> destination_endpoint;
    };

    bool submit_owned(const std::shared_ptr<Chakra::ETFeederNode>& node,
                      MovementCompletionOwner completion);

    void start_source_read(const DmaDispatch& dispatch);
    void complete_stage(EventType type, CallData* data);
    void start_next_segment(const std::string& event_id);
    void start_segment_hop(const std::string& event_id,
                           std::size_t segment_index,
                           std::size_t hop_index);
    void start_destination_write(const std::string& event_id);
    void finish(const std::string& event_id);
    uint64_t compute_overlap_ns(uint64_t start_ns, uint64_t finish_ns) const;

    Sys* sys_;
    std::exception_ptr failure_;
    std::unique_ptr<DmaScheduler> scheduler_;
    std::unordered_map<std::string, Submission> submissions_;
    std::string run_id_;
    std::string manifest_digest_;
    std::unordered_set<uint64_t> active_compute_nodes_;
    std::optional<uint64_t> active_compute_union_start_ns_;
    std::vector<std::pair<uint64_t, uint64_t>> compute_intervals_;
};

}  // namespace AstraSim

#endif /* __MEMORY_MOVEMENT_EXECUTOR_HH__ */
