#ifndef __MEMORY_TRANSPORT_ADAPTER_HH__
#define __MEMORY_TRANSPORT_ADAPTER_HH__

#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <string>
#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/memory/MemoryTransportConfig.hh"
#include "astra-sim/system/memory/TransportPathAPI.hh"

namespace AstraSim {
// Factory owns this wrapper; original APIs own every queue and simulated clock.
class MemoryTransportAdapter : public AstraMemoryAPI, public Callable {
  public:
    MemoryTransportAdapter(uint32_t rank, std::string link, AstraMemoryAPI& original,
                           const MemoryTransportConfig& config);
    void attach(TransportPathAPI& path);
    void set_sys(int id, Sys* sys) override;
    MemoryLocationType get_memory_location_type() const override;
    void issue(const MemoryRequest& request, WorkloadLayerHandlerData* handler) override;
    void call(EventType type, CallData* data) override;
    void rethrow_failure() const;
    bool drained() const;
    nlohmann::json summary() const;

  private:
    struct Context;
    void fabric(const std::shared_ptr<Context>& context);
    void finish(const std::shared_ptr<Context>& context);
    uint32_t rank_;
    std::string link_;
    AstraMemoryAPI& original_;
    const MemoryTransportConfig& config_;
    TransportPathAPI* path_ = nullptr;
    Sys* sys_ = nullptr;
    std::map<WorkloadLayerHandlerData*, std::shared_ptr<Context>> pending_;
    nlohmann::json counters_ = nlohmann::json::object();
    std::exception_ptr failure_;
};
}  // namespace AstraSim
#endif
