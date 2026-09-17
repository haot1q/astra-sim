/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#ifndef __MEMORY_PREPARATION_WIRE_HH__
#define __MEMORY_PREPARATION_WIRE_HH__

#include <cstdint>
#include <memory>

#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"
#include "extern/helper/json/json.hpp"

namespace AstraSim {

std::shared_ptr<Chakra::ETFeederNode> direct_movement_node(
    const nlohmann::json& sidecar,
    const nlohmann::json& event,
    uint32_t rank,
    uint64_t node_id);

}  // namespace AstraSim

#endif
