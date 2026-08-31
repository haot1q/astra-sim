/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __DMA_SCHEDULER_HH__
#define __DMA_SCHEDULER_HH__

#include <cstdint>
#include <optional>
#include <string>
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
    void validate_dependencies() const;
    std::vector<DmaDispatch> dispatch_ready(uint64_t now_ns);
    DmaReceipt complete(const std::string& event_id, uint64_t finish_ns);
    DmaJob active_job(const std::string& event_id) const;

    bool drained() const;
    std::size_t pending_count() const;
    std::size_t in_flight_count() const;
    const DmaSchedulerConfig& config() const;

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

    bool dependencies_complete(const PendingJob& pending) const;
    std::optional<std::size_t> select_pending(uint64_t now_ns) const;
    uint32_t first_free_engine() const;

    DmaSchedulerConfig config_;
    uint64_t next_submission_seq_ = 0;
    uint32_t consecutive_high_priority_starts_ = 0;
    uint32_t in_flight_page_movements_ = 0;
    std::vector<bool> engine_busy_;
    std::vector<PendingJob> pending_;
    std::unordered_map<std::string, InFlightJob> in_flight_;
    std::unordered_set<std::string> accepted_ids_;
    std::unordered_set<std::string> completed_ids_;
};

DmaPriorityClass parse_dma_priority_class(const std::string& value);
std::string dma_priority_class_name(DmaPriorityClass value);

}  // namespace AstraSim

#endif /* __DMA_SCHEDULER_HH__ */
