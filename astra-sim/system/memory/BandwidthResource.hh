/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __BANDWIDTH_RESOURCE_HH__
#define __BANDWIDTH_RESOURCE_HH__

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "astra-sim/system/AstraMemoryAPI.hh"
#include "extern/helper/json/json.hpp"

namespace AstraSim {

constexpr const char* kBandwidthResourceSchemaVersion =
    "bandwidth-resource-v1";

enum class BandwidthConcurrency : uint8_t {
    Serialized = 1,
    Simultaneous = 2,
};

struct BandwidthResourceConfig {
    uint64_t read_bytes_per_second;
    uint64_t write_bytes_per_second;
    std::optional<uint64_t> shared_bytes_per_second;
    BandwidthConcurrency concurrency;
    uint64_t turnaround_ns;
};

BandwidthResourceConfig parse_bandwidth_resource_config(
    const nlohmann::json& payload,
    const std::string& context);
nlohmann::json bandwidth_resource_config_to_json(
    const BandwidthResourceConfig& config);

class BandwidthResource {
  public:
    explicit BandwidthResource(BandwidthResourceConfig config);

    uint64_t service_time_ns(
        uint64_t bytes,
        MemoryOperation operation,
        uint64_t latency_ns = 0) const;
    std::size_t server_index(MemoryOperation operation) const;
    std::size_t server_count() const;
    uint64_t turnaround_delay_ns(
        MemoryOperation previous,
        MemoryOperation current) const;
    const BandwidthResourceConfig& config() const;

  private:
    BandwidthResourceConfig config_;
};

}  // namespace AstraSim

#endif /* __BANDWIDTH_RESOURCE_HH__ */
