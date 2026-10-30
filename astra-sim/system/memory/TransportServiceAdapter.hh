/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TRANSPORT_SERVICE_ADAPTER_HH__
#define __TRANSPORT_SERVICE_ADAPTER_HH__

#include <cstdint>
#include <exception>
#include <string>

#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/memory/PhysicalServiceAdapter.hh"
#include "astra-sim/system/memory/TransportServiceConfig.hh"

namespace AstraSim {
class TransportServiceAdapter;

struct TransportReceipt {
    std::string request_id, link_id, resource_id;
    uint64_t payload_bytes, ready_ns, start_ns, finish_ns, release_ns;
};

// Caller owns this context through queue release and final arrival. No copy/reuse.
class TransportRequest : public WorkloadLayerHandlerData, public Callable {
  public:
    TransportRequest(uint32_t rank, std::string id, uint64_t bytes, Callable& completion);
    TransportRequest(const TransportRequest&) = delete;
    TransportRequest& operator=(const TransportRequest&) = delete;
    void call(EventType type, CallData* data) override;
    TransportReceipt receipt() const;

  private:
    friend class TransportServiceAdapter;
    const std::string id_;
    const uint64_t bytes_;
    Callable& completion_;
    TransportServiceAdapter* service_ = nullptr;  // Factory-owned, non-owning.
    enum class Phase { Service, Propagation, Complete, Failed };
    Phase phase_ = Phase::Service;
    uint64_t finish_ns_ = 0;
    bool completed_ = false;
};

// No queue/clock: the original adapter points at the factory's unique FIFO.
class TransportServiceAdapter {
  public:
    TransportServiceAdapter(uint32_t rank, TransportLink link, TransportResource resource,
                            PhysicalServiceAdapter& adapter, TransportLatencyPolicy policy);
    void set_sys(Sys* sys);
    void issue(TransportRequest& request);
    void rethrow_failure() const;

  private:
    friend class TransportRequest;
    uint32_t rank_;
    TransportLink link_;
    BandwidthResource service_time_;
    uint64_t latency_ns_;
    TransportLatencyPolicy policy_;
    Sys* sys_ = nullptr;  // Original event owner; no private clock or queue.
    PhysicalServiceAdapter& adapter_;
    std::exception_ptr failure_;
};
}  // namespace AstraSim
#endif
