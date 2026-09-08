/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __PHYSICAL_SERVICE_ADAPTER_HH__
#define __PHYSICAL_SERVICE_ADAPTER_HH__

#include <exception>
#include <vector>

#include "astra-sim/system/AstraMemoryAPI.hh"

namespace AstraSim {

// Non-owning, rank-local logical-device router. Physical queues belong to factory.
class PhysicalServiceAdapter : public AstraMemoryAPI {
  public:
    PhysicalServiceAdapter(uint32_t rank, std::vector<AstraMemoryAPI*> devices);
    void set_sys(int id, Sys* sys) override;
    void issue(const MemoryRequest& request, WorkloadLayerHandlerData* handler) override;
    MemoryLocationType get_memory_location_type() const override;
    void rethrow_failure() const;

  private:
    uint32_t rank_;
    std::vector<AstraMemoryAPI*> devices_;
    Sys* sys_ = nullptr;
    std::exception_ptr failure_;
};

}  // namespace AstraSim
#endif
