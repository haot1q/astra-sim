#ifndef ASTRA_ANALYTICAL_JOINT_SIMULATION_COORDINATOR_HH
#define ASTRA_ANALYTICAL_JOINT_SIMULATION_COORDINATOR_HH

#include "common/JointRuntimeClient.hh"
#include "common/JointProgress.hh"
#include "astra-sim/system/memory/ExternalMemoryService.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace AstraSimAnalytical {
class JointSimulationCoordinator : public AstraSim::ExternalMemoryService::Provider,
                                   public std::enable_shared_from_this<JointSimulationCoordinator> {
  public:
    using Service = AstraSim::ExternalMemoryService;
    using Json = JointRuntimeClient::Json;
    using Exchange = std::function<Json(const std::string&, const Json&)>;
    using ReceiveObserver = std::function<void(const Service::Identity&, uint64_t, Service::ReceiveEdge, uint64_t)>;
    // The queue may not be stepped after its run owner is destroyed.
    JointSimulationCoordinator(std::shared_ptr<NetworkAnalytical::EventQueue> queue,
                               Json config, Exchange exchange);
    void bind(std::shared_ptr<AstraSim::JointSourceContext> context,
              const std::vector<AstraSim::Workload*>& workloads);
    Service::Admission try_submit(const Service::NodeRequest& request) override;
    Json activate(const Json& manifest);
    Json successor(const Json& manifest);
    void check_workload(const std::string& prefix, const std::vector<AstraSim::Workload*>& targets);
    void advance();
    void observe_view_completion();
    void safe_point();
    void deadline(uint32_t endpoint, uint64_t when_ns, uint64_t generation);
    void enqueue_receive(uint64_t completed_fs, std::function<void(Service::ReceiveEdge)> callback);
    void observe_receives(ReceiveObserver observer);
    void observe_progress(double timeout_seconds, JointProgress::Sink sink,
                          JointProgress::Clock clock = std::chrono::steady_clock::now);
    void finish();
    void abort();
    void rethrow_failure() const;
    const Json& status() const noexcept { return status_; }
    uint64_t frontier_fs() const noexcept { return frontier_fs_; }
    size_t pending_nodes() const noexcept { return pending_.size(); }
    const std::map<Service::NodeKey, Service::NodeRequest>& promises() const { return pending_; }
  private:
    friend struct JointCoordinatorTestAccess;
    Json start_service(const Json& ref, const Json& facts, uint64_t epoch);
    void require_finished_cohort(uint64_t iteration) const;
    Json inspect_control(const std::string& prefix, uint32_t iteration) const;
    void validate_business_graphs(const Json& manifest, uint32_t iteration) const;
    void check_successor_facts(const Json& manifest, const Json& ref, uint64_t cut) const;
    struct Callback { JointSimulationCoordinator* owner; uint64_t id; std::function<void()> function; };
    std::shared_ptr<NetworkAnalytical::EventQueue> queue_;
    std::shared_ptr<Service> service_;
    Json config_, status_, context_, activation_, view_identity_, control_;
    std::optional<uint32_t> retired_business_;
    std::vector<AstraSim::Workload*> workloads_;
    std::set<uint32_t> loaded_ranks_;
    bool awaiting_ = false, handoff_ = false;
    Exchange exchange_;
    ReceiveObserver receive_observer_;
    std::unique_ptr<JointProgress> progress_observer_;
    std::map<Service::NodeKey, Service::NodeRequest> pending_;
    std::set<Service::NodeKey> sent_, receiving_;
    std::set<Service::AdmissionKey> rejected_;
    std::map<uint64_t, std::unique_ptr<Callback>> callbacks_;
    std::map<uint32_t, std::pair<uint64_t, uint64_t>> deadlines_;
    std::optional<uint64_t> closed_ns_;
    uint64_t frontier_fs_ = 0, serial_ = 0, events_ = 0, same_time_ = 0, idle_events_ = 0;
    uint64_t progress_ = 0;
    std::string tie_, failure_;
    bool stopped_ = false, draining_ = false, hold_set_ = false, hold_released_ = false;
    using Candidates = std::map<std::string, std::pair<uint64_t, Json>>;
    uint64_t mw_per_call_ = 0;
    bool mw_retry_step_ = false;
    bool in_process_ = false, exact_memory_retry_ = false;
    bool phase_timing_ = false;
    std::optional<uint64_t> phase_target_;
    std::optional<std::set<uint64_t>> memory_controls_fs_;
    uint64_t phase_start_ns_ = 0;
    void begin_view_timing();
    bool waiting_for_supply(const std::string& domain) const;
    void advance_memory(const Candidates& candidates, const std::string& chosen, uint64_t first);
    void install_memory_grant(const Json& grant, const Json& reply, uint64_t before);
    std::map<std::string, uint64_t> mw_fallbacks_;
    bool advance_mw(const Candidates& candidates, const std::string& chosen, uint64_t first);
    bool advance_cx(const Candidates& candidates, const std::string& chosen, uint64_t first);
    Json call(const std::string& command, const Json& body = Json::object(),
              const std::function<void(const Json&)>& before_consume = {});
    void consume(Json reply);
    void schedule(uint64_t when_ns, std::function<void()> callback);
    static void dispatch(void* argument) noexcept;
    static uint64_t fs(uint64_t ns);
    uint64_t limit(const char* name) const;
};
}
#endif
