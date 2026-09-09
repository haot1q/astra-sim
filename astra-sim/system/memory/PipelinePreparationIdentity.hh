/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __PIPELINE_PREPARATION_IDENTITY_HH__
#define __PIPELINE_PREPARATION_IDENTITY_HH__

#include <cstdint>
#include <string>
#include <vector>
#include "extern/helper/json/json.hpp"

namespace AstraSim {
class PhysicalServiceFactory;
struct PipelinePreparationIdentity {
    std::vector<uint32_t> ranks;
    std::string digest;
};
// Provenance only: no Page state, capacity, or transfer authority.
PipelinePreparationIdentity validate_pipeline_preparation_identity(
    const nlohmann::json& work, const PhysicalServiceFactory& services);
}
#endif
