/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __TRANSPORT_SERVICE_CONFIG_HH__
#define __TRANSPORT_SERVICE_CONFIG_HH__

#include <cstdint>
#include <map>
#include <string>

#include "astra-sim/system/memory/BandwidthResource.hh"
#include "astra-sim/system/memory/PhysicalServiceConfig.hh"

namespace AstraSim {

struct TransportResource {
    BandwidthResourceConfig bandwidth;
    uint64_t latency_ns;
};

struct TransportLink {
    std::string id, source, target, resource;
};

struct TransportServiceConfig {
    std::string physical_network_sha256, projection_digest;
    std::map<std::string, TransportResource> resources;
    std::map<std::string, TransportLink> links;
};

TransportServiceConfig load_transport_service_config(
    const std::string& path, const PhysicalServiceConfig& physical);

}  // namespace AstraSim
#endif
