/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __PD_PATH_EXECUTOR_HH__
#define __PD_PATH_EXECUTOR_HH__

#include <exception>
#include <map>
#include <memory>
#include <set>
#include "PdPathStages.hh"
#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/NativeTagRegistry.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"

namespace AstraSim {

// Numerical path execution only. The original P/D owner commits visibility.
class PdPathExecutor : public Callable {
  public:
    PdPathExecutor(const std::vector<Sys*>& systems, PhysicalServiceFactory& services,
                   NativeTagRegistry& tags, nlohmann::json actual_network);
    void submit(const std::string& path, uint64_t ready_ns);
    void cancel(const std::string& transfer_id);
    void call(EventType event, CallData* data) override;
    bool drained() const;
    void rethrow_failure() const;
    uint64_t completed_count() const { return completed_count_; }

  private:
    enum class CallbackKind { Start, Memory, Send, Receive };
    struct Callback : WorkloadLayerHandlerData {
        uint64_t work_id;
        uint64_t slice;
        uint64_t stage;
        CallbackKind kind;
    };
    struct SliceState {
        size_t stage = 0;
        bool started = false;
        bool finished = false;
        bool sent = false;
        bool received = false;
        uint64_t ready_ns = 0;
        uint64_t send_finish_ns = 0;
        uint64_t recv_finish_ns = 0;
        NativeTagLease lease{};
    };
    struct Transfer {
        PdPathWork work;
        std::vector<std::vector<PdPathStage>> stages;
        std::vector<SliceState> slices;
        std::map<uint32_t, uint64_t> active_bytes;
        uint64_t ready_ns;
        uint64_t pending = 0;
        bool cancelled = false;
    };

    Callback* callback(uint64_t work, uint64_t slice, CallbackKind kind);
    void pump(uint64_t work);
    void issue(uint64_t work, uint64_t slice);
    void issue_network(uint64_t work, uint64_t slice);
    void completed(uint64_t work, uint64_t slice, Callback& handler);
    void finish_if_ready(uint64_t work);
    nlohmann::json identity(const Transfer& transfer) const;

    std::vector<Sys*> systems_;
    PhysicalServiceFactory& services_;
    NativeTagRegistry& tags_;
    nlohmann::json network_;
    std::map<uint64_t, std::unique_ptr<Transfer>> transfers_;
    std::set<std::string> used_transfers_;
    uint64_t next_work_ = 0;
    uint64_t completed_count_ = 0;
    std::exception_ptr failure_;
};
}  // namespace AstraSim
#endif
