/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __EXTERNAL_MEMORY_SERVICE_HH__
#define __EXTERNAL_MEMORY_SERVICE_HH__

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/workload/JointAccessIndex.hh"

namespace AstraSim {
class Workload;

// Serialized by one run-owner event thread. Provider calls must not reenter the bridge.
class ExternalMemoryService {
  public:
    struct Limits { uint64_t pending_nodes, descriptor_bytes, diagnostic_bytes; };
    struct Identity {
        std::string run, session;
        uint32_t rank, iteration;
        uint64_t node;
        bool operator==(const Identity& other) const;
    };
    using AdmissionKey = std::tuple<uint32_t, uint32_t, uint32_t>;
    using NodeKey = std::tuple<uint32_t, uint32_t, uint64_t>;
    struct NodeRequest {
        Identity identity;
        MemoryOperation operation;
        AdmissionKey admission_key;
        uint64_t submitted_ns;
        std::shared_ptr<const JointNodeAccess> access;
    };
    enum class Admission { Accepted, Rejected };
    enum class Outcome { Success, Partial, Error };
    struct Response { Identity identity; uint64_t completed_fs; Outcome outcome; };
    struct ReceiveEdge { uint64_t visible_ns; std::optional<uint64_t> last_closed_ns; };
    struct Status {
        std::size_t accepted, acceptance_unknown, gates, waiting_owners;
        uint64_t descriptor_bytes;
        bool failed, stopped;
        // At most one identity per category; no completed-node tombstone history.
        std::optional<NodeKey> first_accepted, first_unknown;
    };
    struct Provider {
        virtual ~Provider() = default;
        virtual Admission try_submit(const NodeRequest& request) = 0;
    };

    ExternalMemoryService(std::shared_ptr<JointSourceContext> context, std::string session,
                          std::shared_ptr<Provider> provider, Limits limits);
    void bind(Workload& owner);
    void retire(Workload& owner) noexcept;
    Admission submit(Workload& owner, uint64_t node,
                     std::shared_ptr<const JointNodeAccess> access, MemoryOperation operation);
    void complete(const Response& response, ReceiveEdge edge);
    void notify_capacity(AdmissionKey key);
    void resume_ready(ReceiveEdge edge);
    bool pending_for(const Workload& owner) const;
    Status status() const;
    void rethrow_failure() const;
    void close();
    Status abort_local() noexcept;

  private:
    enum class State { Submitting, Accepted, AcceptanceUnknown };
    enum class Failure { None, Protocol, Provider, Completion, Owner, Stopped };
    struct Pending { NodeRequest request; State state; };
    struct Owner { Workload* workload = nullptr; bool retired = false, local_wait = false, wake = false; };
    using PendingTable = std::map<NodeKey, Pending>;
    using Owners = std::map<uint32_t, Owner>;
    using Gates = std::set<AdmissionKey>;
    std::shared_ptr<JointSourceContext> context_;
    std::shared_ptr<Provider> provider_;
    std::string run_, session_;
    Limits limits_;
    PendingTable pending_;
    Owners owners_;
    Gates gates_;
    uint64_t charge_ = 0, pending_charge_ = 0;
    Failure failure_ = Failure::None;
    std::string diagnostic_;
    std::optional<NodeKey> failed_node_;
    std::optional<uint64_t> last_closed_ns_;
    bool submitting_ = false, stopped_ = false;
    Status aborted_{};

    void check();
    [[noreturn]] void fail(Failure code, const char* message,
                         std::optional<NodeKey> node = std::nullopt);
    void remember(Failure code, const char* message,
                  std::optional<NodeKey> node = std::nullopt);
    Owner& owner(Workload& workload);
    void released();
    void validate_edge(ReceiveEdge edge);
    static NodeKey key(const Identity& identity);
    static constexpr uint64_t gate_charge_ = sizeof(AdmissionKey) + 3 * sizeof(void*);
};
}  // namespace AstraSim
#endif
