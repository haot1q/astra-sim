/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __UCIE_TRANSPORT_HH__
#define __UCIE_TRANSPORT_HH__

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "astra-sim/system/AstraMemoryAPI.hh"

namespace Chakra {
class ETFeederNode;
}

namespace AstraSim {

class Sys;
class WorkloadLayerHandlerData;

inline constexpr const char* kUcieTransportSchema = "ucie-transport-v1";
inline constexpr const char* kUcieSchemaAttr = "ucie_transport_schema_version";
inline constexpr const char* kUcieLinkIdAttr = "ucie_link_id";

struct UcieHopSpec {
    const char* name;
    MemoryOperation operation;
    uint64_t bytes;
};

bool node_requests_ucie_transport(
    const std::shared_ptr<Chakra::ETFeederNode>& node);
std::string ucie_link_id_attr(
    const std::shared_ptr<Chakra::ETFeederNode>& node);
std::vector<UcieHopSpec> ucie_transaction_hops(
    MemoryOperation operation, uint64_t payload_bytes, uint64_t header_bytes);
void issue_ucie_mem(
    Sys* sys,
    const std::shared_ptr<Chakra::ETFeederNode>& node,
    WorkloadLayerHandlerData* wlhd,
    MemoryOperation operation);

}  // namespace AstraSim

#endif /* __UCIE_TRANSPORT_HH__ */
