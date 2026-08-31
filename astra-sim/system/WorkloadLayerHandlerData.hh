/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __WORKLOAD_LAYER_HANDLER_DATA_HH__
#define __WORKLOAD_LAYER_HANDLER_DATA_HH__

#include "astra-sim/common/AstraNetworkAPI.hh"
#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/system/BasicEventHandlerData.hh"

namespace AstraSim {

class Workload;
class Callable;

class WorkloadLayerHandlerData : public BasicEventHandlerData, public MetaData {
  public:
    int sys_id;
    Workload* workload;
    Callable* completion_target;
    uint64_t node_id;
    uint32_t device_id;
    MemoryOperation memory_operation;
    uint64_t memory_ready_ns;
    uint64_t memory_start_ns;
    uint64_t memory_finish_ns;
    bool pim_enabled;
    uint32_t pim_channel_id;
    uint64_t pim_runtime;
    WorkloadLayerHandlerData();
};

}  // namespace AstraSim

#endif /* __WORKLOAD_LAYER_HANDLER_DATA_HH__ */
