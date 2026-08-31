/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "WorkloadLayerHandlerData.hh"

using namespace AstraSim;

WorkloadLayerHandlerData::WorkloadLayerHandlerData() {
    workload = nullptr;
    completion_target = nullptr;
    node_id = 0;
    device_id = 0;
    memory_operation = MemoryOperation::Read;
    pim_enabled = false;
    pim_channel_id = 0;
    pim_runtime = 0;
}
