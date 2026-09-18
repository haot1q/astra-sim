/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __WORKLOAD_HH__
#define __WORKLOAD_HH__

#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>

#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/CommunicatorGroup.hh"
#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/workload/HardwareResource.hh"
#include "astra-sim/workload/IndexedTemplateRegistry.hh"
#include "astra-sim/workload/TemplateRegistry.hh"
#include "astra-sim/workload/WorkloadFeeder.hh"

namespace AstraSim {

class Sys;
class DataSet;

MemoryOperation memory_operation_for_node_type(
    ChakraProtoMsg::NodeType node_type);

class Workload : public Callable {
  public:
    Workload(Sys* sys,
             std::string et_filename,
             std::string comm_group_filename);
    ~Workload();

    // communicator groups
    void initialize_comm_group(std::string comm_group_filename);

    // event-based simulation
    void issue_dep_free_nodes();
    void issue(std::shared_ptr<Chakra::ETFeederNode> node);
    void issue_replay(std::shared_ptr<Chakra::ETFeederNode> node);
    // void issue_remote_mem(std::shared_ptr<Chakra::ETFeederNode> node); integrated into issue_mem
    void issue_mem(std::shared_ptr<Chakra::ETFeederNode> node);
    void issue_memory_movement(std::shared_ptr<Chakra::ETFeederNode> node);
    void complete_memory_wait(const std::shared_ptr<Chakra::ETFeederNode>& node,
                              uint32_t expected_iteration);
    void complete_memory_movement(uint64_t node_id);
    std::optional<uint64_t> movement_exposed_to_dependent_ns(
        uint64_t node_id,
        uint64_t ready_ns,
        uint64_t finish_ns);
    void issue_comp(std::shared_ptr<Chakra::ETFeederNode> node);
    void issue_comm(std::shared_ptr<Chakra::ETFeederNode> node);
    void skip_invalid(std::shared_ptr<Chakra::ETFeederNode> node);
    void call(EventType event, CallData* data);
    void fire();
    void add_workload(const std::string& new_filename,
                      const std::vector<Sys*>& systems);
    void sleep_workload(const std::vector<Sys*>& systems);
    WorkloadFeeder* load_et_feeder(const std::string& workload_filename);

    // stats
    void report();
    // Opt-in completion evidence is independent of frontend stdin handshakes.
    bool emit_rank_completions = false;
    uint64_t rank_completion_count = 0;

    WorkloadFeeder* et_feeder;
    CommunicatorGroup* comm_group;
    HardwareResource* hw_resource;
    Sys* sys;
    std::unordered_map<int, uint64_t> collective_comm_node_id_map;
    std::unordered_map<int, DataSet*> collective_comm_wrapper_map;
    bool is_finished;
    uint32_t iteration;
    std::string filename;

    bool is_sleep;
    struct PendingWorkload {
        std::string et_filename;
        std::unique_ptr<WorkloadFeeder> prepared_feeder;
    };
    std::queue<PendingWorkload> pending_workloads;

  private:
    std::unique_ptr<WorkloadFeeder> prepare_template_feeder(
        const std::string& command);
    void validate_feeder(const WorkloadFeeder& feeder) const;
    void install_feeder(std::unique_ptr<WorkloadFeeder> feeder);
    void report_template_metrics() const;
    void record_parent_completion(
        const std::shared_ptr<Chakra::ETFeederNode>& node);
    void reset_iteration_tracking();
    std::unordered_map<uint64_t, uint64_t> latest_parent_completion_ns_;
    std::unordered_map<uint64_t, std::shared_ptr<Chakra::ETFeederNode>> pending_memory_waits_;
    TemplateRegistry template_registry_;
    IndexedTemplateRegistry indexed_registry_;
    uint64_t et_read_count_ = 0;
};

}  // namespace AstraSim

#endif /* __WORKLOAD_HH__ */
