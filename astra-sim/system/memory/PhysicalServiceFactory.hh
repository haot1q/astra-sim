/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __PHYSICAL_SERVICE_FACTORY_HH__
#define __PHYSICAL_SERVICE_FACTORY_HH__

#include <map>
#include <memory>
#include <vector>

#include "astra-sim/system/MemoryTierRegistry.hh"
#include "astra-sim/system/memory/MovementPathRegistry.hh"
#include "astra-sim/system/memory/PhysicalServiceAdapter.hh"
#include "astra-sim/system/memory/PhysicalServiceConfig.hh"
#include "astra-sim/system/memory/PdPathServiceConfig.hh"
#include "astra-sim/system/memory/UcieLinkRegistry.hh"
#include "astra-sim/system/memory/TransportServiceAdapter.hh"
#include "astra-sim/system/memory/MemoryTransportAdapter.hh"

namespace Analytical { class AnalyticalMemory; }

namespace AstraSim {

struct RankServiceBindings {
    std::vector<MemoryTierBinding> memory;
    UcieLinkRegistry ucie;
    MovementPathRegistry movement;
};

// Must outlive every Sys and pending callback referencing these services.
class PhysicalServiceFactory {
  public:
    PhysicalServiceFactory(const MemoryTierConfigSet& memory,
                           const std::string& binding_path, uint32_t rank_count,
                           const std::string& pd_path_binding_path = "",
                           const std::string& transport_path = "",
                           const std::string& memory_transport_path = "");
    ~PhysicalServiceFactory();
    const RankServiceBindings& at(uint32_t rank) const;
    const ServiceBindingIdentity& identity() const;
    const PhysicalServiceConfig& configuration() const { return config_; }
    void rethrow_failure() const;
    AstraMemoryAPI* pd_interface(uint32_t rank, const std::string& interface_id) const;
    const PdPathServiceConfig& pd_path_config() const;
    TransportServiceAdapter& transport(uint32_t rank, const std::string& link_id);
    const TransportServiceConfig& transport_configuration() const { return transport_config_; }
    void attach_transport_path_api(TransportPathAPI& path);
    bool memory_transport_drained() const;
    void write_memory_transport_summary(const std::string& path, bool failed) const;

  private:
    AstraMemoryAPI* own(nlohmann::json backend, bool physical);
    AstraMemoryAPI* route(uint32_t rank, const std::string& kind,
                          const std::string& ref, uint32_t devices);
    RankServiceBindings rank_bindings(const MemoryTierConfigSet& memory, uint32_t rank);
    void legacy_backends(const MemoryTierConfigSet& memory);

    PhysicalServiceConfig config_;
    PdPathServiceConfig pd_config_;
    TransportServiceConfig transport_config_;
    MemoryTransportConfig memory_transport_config_;
    std::vector<std::unique_ptr<MemoryTransportAdapter>> memory_transports_;
    bool native_;
    std::vector<std::unique_ptr<Analytical::AnalyticalMemory>> memories_;
    std::map<std::string, AstraMemoryAPI*> physical_;
    std::map<ServiceEndpoint, AstraMemoryAPI*> legacy_;
    std::vector<std::unique_ptr<PhysicalServiceAdapter>> adapters_;
    std::vector<RankServiceBindings> ranks_;
    std::map<std::pair<uint32_t, std::string>, AstraMemoryAPI*> interfaces_;
    std::map<std::pair<uint32_t, std::string>, std::unique_ptr<TransportServiceAdapter>> transports_;
};

}  // namespace AstraSim
#endif
