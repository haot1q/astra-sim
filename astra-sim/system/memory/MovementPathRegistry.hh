/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __MOVEMENT_PATH_REGISTRY_HH__
#define __MOVEMENT_PATH_REGISTRY_HH__

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/system/MemoryTierConfig.hh"

namespace AstraSim {

struct MovementBandwidthBinding {
    std::string id;
    uint32_t stack_count;
    uint64_t latency_ns;
    AstraMemoryAPI* api;
};

class MovementPathRegistry {
  public:
    MovementPathRegistry() = default;

    MovementPathRegistry(
        std::string selected_id,
        std::vector<MovementPathCapabilityConfig> capabilities,
        std::vector<MovementBandwidthBinding> resources)
        : selected_id_(std::move(selected_id)) {
        if (selected_id_.empty()) {
            throw std::invalid_argument(
                "movement path selection must not be empty");
        }
        for (auto& capability : capabilities) {
            if (!capabilities_.emplace(capability.id, std::move(capability))
                     .second) {
                throw std::invalid_argument(
                    "duplicate movement path capability");
            }
        }
        if (capabilities_.find(selected_id_) == capabilities_.end()) {
            throw std::invalid_argument(
                "selected movement path capability is not registered");
        }
        for (const auto& resource : resources) {
            if (resource.id.empty() || resource.stack_count == 0 ||
                resource.api == nullptr ||
                !resources_.emplace(resource.id, resource).second) {
                throw std::invalid_argument(
                    "invalid or duplicate movement bandwidth resource");
            }
        }
    }

    bool empty() const {
        return selected_id_.empty();
    }

    const std::string& selected_id() const {
        if (empty()) {
            throw std::logic_error("movement path registry is empty");
        }
        return selected_id_;
    }

    const MovementPathCapabilityConfig& capability(
        const std::string& id) const {
        const auto found = capabilities_.find(id);
        if (found == capabilities_.end()) {
            throw std::out_of_range(
                "unknown movement path capability '" + id + "'");
        }
        return found->second;
    }

    const MovementBandwidthBinding& resource(
        const std::string& id) const {
        const auto found = resources_.find(id);
        if (found == resources_.end()) {
            throw std::out_of_range(
                "unknown movement bandwidth resource '" + id + "'");
        }
        return found->second;
    }

    MovementPathSegmentConfig execution_segment(
        const std::string& id, std::size_t index, const std::string& kind) const {
        const auto& segments = capability(id).segments;
        const bool reverse = id == "host_pcie" &&
            (kind == "page_demote" || kind == "kv_cold_copy_store");
        auto segment = segments.at(reverse ? segments.size() - 1 - index : index);
        if (reverse) {
            segment.operation = segment.operation == "read" ? "write" : "read";
        }
        return segment;
    }

    void validate_host_endpoints(
        const std::string& kind, uint32_t domain, uint32_t source_tier,
        uint32_t source_device, uint32_t destination_tier,
        uint32_t destination_device) const {
        const auto& path = capability("host_pcie");
        const bool promote = kind == "page_promote" || kind == "kv_cold_copy_load";
        if ((!promote && kind != "page_demote" && kind != "kv_cold_copy_store") ||
            domain >= path.host_domain_count ||
            source_tier != (promote ? path.host_cold_tier_id : path.host_hot_tier_id) ||
            destination_tier != (promote ? path.host_hot_tier_id : path.host_cold_tier_id) ||
            source_device != (promote ? 0 : domain) ||
            destination_device != (promote ? domain : 0)) {
            throw std::invalid_argument("Host movement endpoints differ from manifest topology");
        }
    }

    const std::unordered_map<std::string, MovementBandwidthBinding>&
    resources() const {
        return resources_;
    }

  private:
    std::string selected_id_;
    std::unordered_map<std::string, MovementPathCapabilityConfig>
        capabilities_;
    std::unordered_map<std::string, MovementBandwidthBinding> resources_;
};

}  // namespace AstraSim

#endif /* __MOVEMENT_PATH_REGISTRY_HH__ */
