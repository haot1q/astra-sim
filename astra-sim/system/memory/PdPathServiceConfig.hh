/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __PD_PATH_SERVICE_CONFIG_HH__
#define __PD_PATH_SERVICE_CONFIG_HH__

#include <map>
#include <string>
#include <utility>

#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/system/memory/PhysicalServiceConfig.hh"

namespace AstraSim {

struct PdPathServiceConfig {
    std::string binding_digest;
    std::string profile_digest;
    std::map<std::string, nlohmann::json> declared_backends;
    std::map<std::pair<uint32_t, std::string>, std::string> bindings;
    std::map<std::string, MemoryTimeRounding> rounding;
};

PdPathServiceConfig load_pd_path_service_config(
    const std::string& path, const PhysicalServiceConfig& physical);

}  // namespace AstraSim
#endif
