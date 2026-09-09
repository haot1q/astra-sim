/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __PD_OWNER_IDENTITY_HH__
#define __PD_OWNER_IDENTITY_HH__

#include <cstdint>
#include <vector>
#include "PhysicalServiceConfig.hh"

namespace AstraSim {
// Check explicit namespace mapping against actual rank owners, without renaming them.
void validate_pd_owner_identity(const nlohmann::json& identity,
    const PhysicalServiceConfig& physical, const std::vector<uint32_t>& ranks);
void validate_pd_context_owner(const nlohmann::json& endpoint, bool mapped,
    const PhysicalServiceConfig& physical, const std::vector<uint32_t>& ranks);
std::vector<uint32_t> validate_pd_pipeline_stage(const nlohmann::json& stage,
    const nlohmann::json& endpoint, const PhysicalServiceConfig& physical);
void validate_pd_pipeline_context(const nlohmann::json& endpoint,
    const PhysicalServiceConfig& physical, const std::vector<uint32_t>& ranks);
void validate_pd_descriptor_geometry(const nlohmann::json& descriptor,
    const nlohmann::json& endpoints);
}
#endif
