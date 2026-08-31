/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __ASTRA_MEMORY_API_HH__
#define __ASTRA_MEMORY_API_HH__

#include <cstdint>

namespace AstraSim {

enum class MemoryLocationType : uint8_t {
  INVALID_MEMORY = 0,
  LOCAL_MEMORY = 1,
  REMOTE_MEMORY = 2,
  CXL_MEMORY = 3,
  STORAGE_MEMORY = 4
};

enum class MemoryOperation : uint8_t {
  Read = 1,
  Write = 2,
};

struct MemoryRequest {
  uint64_t bytes;
  MemoryOperation operation;
};

class Sys;
class WorkloadLayerHandlerData;

class AstraMemoryAPI {
  public:
    virtual ~AstraMemoryAPI() = default;
    virtual void set_sys(int id, Sys* sys) = 0;
    virtual void issue(const MemoryRequest& request,
                       WorkloadLayerHandlerData* wlhd) = 0;
    
    virtual MemoryLocationType get_memory_location_type() const = 0;
};

}  // namespace AstraSim

#endif /* __ASTRA_MEMORY_API_HH__ */
