/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __PD_PATH_STAGES_HH__
#define __PD_PATH_STAGES_HH__

#include "PdPathWork.hh"
#include "PhysicalServiceFactory.hh"

namespace AstraSim {
class Sys;

struct PdPathStage {
    std::string id;
    std::string kind;
    std::string resource;
    std::string physical_resource;
    uint32_t rank;
    uint32_t device;
    MemoryOperation operation;
    uint64_t bytes;
    AstraMemoryAPI* api;
};

// Resolve every actual API before posting any request. Network projection is
// supplied by the frontend's actual NetworkParser, never by the work sidecar.
std::vector<std::vector<PdPathStage>> bind_pd_path_stages(
    const PdPathWork& work, const std::vector<Sys*>& systems,
    PhysicalServiceFactory& services, const nlohmann::json& actual_network);

}  // namespace AstraSim
#endif
