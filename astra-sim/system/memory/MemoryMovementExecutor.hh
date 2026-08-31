/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __MEMORY_MOVEMENT_EXECUTOR_HH__
#define __MEMORY_MOVEMENT_EXECUTOR_HH__

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/memory/DmaScheduler.hh"

namespace Chakra {
class ETFeederNode;
}

namespace AstraSim {

class Sys;
class Workload;

class MemoryMovementExecutor : public Callable {
  public:
    explicit MemoryMovementExecutor(Sys* sys);

    bool is_movement_node(
        const std::shared_ptr<Chakra::ETFeederNode>& node) const;
    bool submit(const std::shared_ptr<Chakra::ETFeederNode>& node,
                Workload* workload);
    void dispatch();
    void call(EventType type, CallData* data) override;
    bool drained() const;
    void record_compute_start(uint64_t node_id);
    void record_compute_finish(uint64_t node_id);

  private:
    struct Submission {
        uint64_t node_id;
        Workload* workload;
        bool foreground;
        std::string run_id;
        std::string instance_id;
        std::string manifest_digest;
        std::optional<std::string> page_id;
        std::optional<std::string> transaction_id;
        std::optional<uint32_t> expected_residency_version;
    };

    void start_source_read(const DmaDispatch& dispatch);
    void start_destination_write(const std::string& event_id);
    void finish(const std::string& event_id);
    uint64_t compute_overlap_ns(uint64_t start_ns, uint64_t finish_ns) const;

    Sys* sys_;
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
