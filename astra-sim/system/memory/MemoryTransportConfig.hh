#ifndef __MEMORY_TRANSPORT_CONFIG_HH__
#define __MEMORY_TRANSPORT_CONFIG_HH__

#include <cstdint>
#include <map>
#include <string>
#include <tuple>
#include <vector>
#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/system/memory/TransportServiceConfig.hh"

namespace AstraSim {
using MemoryTransportKey = std::tuple<uint32_t, std::string, uint32_t, MemoryOperation>;
struct MemoryTransportOperation {
    bool before;
    std::vector<std::string> links;
};
struct MemoryTransportConfig {
    std::string digest;
    std::map<MemoryTransportKey, MemoryTransportOperation> operations;
};
MemoryTransportConfig load_memory_transport_config(const std::string& path,
    const PhysicalServiceConfig& physical, const TransportServiceConfig& transport,
    const std::string& manifest_digest);
}  // namespace AstraSim
#endif
