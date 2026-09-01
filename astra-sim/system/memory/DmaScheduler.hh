/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __DMA_SCHEDULER_HH__
#define __DMA_SCHEDULER_HH__

#include <array>
#include <cstdint>
#include <optional>
#include <queue>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace AstraSim {

constexpr const char* kMemoryMovementSchemaVersion = "memory-events-v1";
constexpr const char* kMemoryMovementReceiptSchemaVersion =
    "memory-movement-receipt-v1";

enum class DmaPriorityClass : uint8_t {
    DecodeCritical = 0,
    PrefillCritical = 1,
    Demand = 2,
    BackgroundFill = 3,
    Demote = 4,
};

struct DmaSchedulerConfig {
    std::string selected_path_id;
    uint32_t engine_count;
    uint32_t max_priority_burst;
    uint32_t max_in_flight_page_movements;
};

struct DmaJob {
    std::string event_id;
    uint32_t source_iteration_id;
    uint64_t ready_ns;
    uint64_t bytes;
    std::string kind;
    std::string phase;
    DmaPriorityClass priority;
    std::string path_id;
    uint32_t source_tier_id;
    uint32_t source_device_id;
    uint32_t destination_tier_id;
    uint32_t destination_device_id;
    std::vector<std::string> resource_ids;
    std::vector<std::string> dependencies;
    bool foreground;
    bool page_movement;
};

struct DmaDispatch {
    DmaJob job;
    uint32_t engine_id;
    uint64_t start_ns;
};

struct DmaReceipt {
    std::string event_id;
    uint32_t source_iteration_id;
    uint64_t ready_ns;
    uint64_t start_ns;
    uint64_t finish_ns;
    uint64_t queue_wait_ns;
    uint64_t service_ns;
    uint64_t bytes;
    std::string kind;
    std::string phase;
    DmaPriorityClass priority;
    std::string path_id;
    uint32_t engine_id;
    std::vector<std::string> resource_ids;
};

struct DmaSchedulerDiagnostics {
    uint64_t full_dependency_validations = 0;
    uint64_t dependency_release_visits = 0;
    uint64_t ready_queue_pops = 0;
};

/** Deterministic run-scoped admission policy for DMA jobs.
 *
 * The class owns only ordering, dependencies, and engine slots. Physical
 * memory timing remains owned by the configured memory backend. This split is
 * deliberate: callers issue the source read and destination write through the
 * #28 bandwidth resources after receiving a dispatch.
 */
class DmaScheduler {
  public:
    explicit DmaScheduler(DmaSchedulerConfig config);

    void submit(DmaJob job);
    void validate_dependencies();
    std::vector<DmaDispatch> dispatch_ready(uint64_t now_ns);
    DmaReceipt complete(const std::string& event_id, uint64_t finish_ns);
    DmaJob active_job(const std::string& event_id) const;

    bool drained() const;
    std::size_t pending_count() const;
    std::size_t in_flight_count() const;
    const DmaSchedulerConfig& config() const;
    const DmaSchedulerDiagnostics& diagnostics() const;

  private:
    struct PendingJob {
        DmaJob job;
        uint64_t submission_seq;
    };

    struct InFlightJob {
        DmaJob job;
        uint32_t engine_id;
        uint64_t start_ns;
    };

    struct TimeReadyEntry {
        uint64_t ready_ns;
        uint64_t submission_seq;
        std::string event_id;
    };

    struct TimeReadyGreater {
        bool operator()(const TimeReadyEntry& lhs,
                        const TimeReadyEntry& rhs) const {
            return std::tie(lhs.ready_ns, lhs.submission_seq, lhs.event_id) >
                   std::tie(rhs.ready_ns, rhs.submission_seq, rhs.event_id);
        }
    };

    struct ReadyEntry {
        uint8_t priority;
        uint64_t ready_ns;
        uint64_t submission_seq;
        std::string event_id;
    };

    struct ReadyGreater {
        bool operator()(const ReadyEntry& lhs, const ReadyEntry& rhs) const {
            return std::tie(lhs.priority, lhs.ready_ns, lhs.submission_seq,
                            lhs.event_id) > std::tie(rhs.priority, rhs.ready_ns,
                                                     rhs.submission_seq,
                                                     rhs.event_id);
        }
    };

    using ReadyQueue =
        std::priority_queue<ReadyEntry, std::vector<ReadyEntry>, ReadyGreater>;

    void enqueue_dependency_ready(const PendingJob& pending);
    void promote_time_ready(uint64_t now_ns);
    std::optional<std::size_t> select_ready_queue() const;
    uint32_t first_free_engine() const;

    DmaSchedulerConfig config_;
    uint64_t next_submission_seq_ = 0;
    uint32_t consecutive_high_priority_starts_ = 0;
    uint32_t in_flight_page_movements_ = 0;
    std::vector<bool> engine_busy_;
    std::unordered_map<std::string, PendingJob> pending_;
    std::unordered_map<std::string, InFlightJob> in_flight_;
    std::unordered_set<std::string> accepted_ids_;
    std::unordered_set<std::string> completed_ids_;
    std::unordered_map<std::string, std::size_t> unresolved_dependencies_;
    std::unordered_map<std::string, std::vector<std::string>> dependents_;
    std::priority_queue<TimeReadyEntry,
                        std::vector<TimeReadyEntry>,
                        TimeReadyGreater>
        time_waiting_;
    std::array<ReadyQueue, 4> ready_;
    bool dependency_graph_dirty_ = false;
    DmaSchedulerDiagnostics diagnostics_;
};

DmaPriorityClass parse_dma_priority_class(const std::string& value);
std::string dma_priority_class_name(DmaPriorityClass value);

}  // namespace AstraSim

#endif /* __DMA_SCHEDULER_HH__ */
