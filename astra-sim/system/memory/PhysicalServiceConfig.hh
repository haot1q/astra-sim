/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __PHYSICAL_SERVICE_CONFIG_HH__
#define __PHYSICAL_SERVICE_CONFIG_HH__

#include <cstdint>
#include <map>
#include <string>
#include <tuple>

#include "astra-sim/system/MemoryTierConfig.hh"

namespace AstraSim {

using ServiceEndpoint = std::tuple<std::string, std::string, uint32_t>;
using ServiceAccess = std::tuple<uint32_t, std::string, std::string, uint32_t>;

struct ServiceBindingIdentity {
    std::string binding_digest;
    std::string activation_id;
};

struct PhysicalServiceConfig {
    ServiceBindingIdentity identity;
    std::map<ServiceAccess, std::string> bindings;
    std::map<std::string, nlohmann::json> backends;
};

// Validate the complete rank/endpoint Cartesian product before creating queues.
PhysicalServiceConfig load_physical_service_config(
    const std::string& path, const MemoryTierConfigSet& memory, uint32_t ranks);

}  // namespace AstraSim

#endif
