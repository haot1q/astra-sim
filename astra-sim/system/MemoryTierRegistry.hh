/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __MEMORY_TIER_REGISTRY_HH__
#define __MEMORY_TIER_REGISTRY_HH__

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "astra-sim/system/AstraMemoryAPI.hh"

namespace AstraSim {

using MemoryTierId = uint32_t;

struct MemoryTierBinding {
    MemoryTierId tier_id;
    std::string tier_name;
    uint32_t num_devices;
    AstraMemoryAPI* api;
};

class MemoryTierRegistry {
  public:
    explicit MemoryTierRegistry(std::vector<MemoryTierBinding> bindings) {
        if (bindings.empty()) {
            throw std::invalid_argument("memory tier registry must not be empty");
        }
        std::unordered_set<std::string> names;
        for (const auto& binding : bindings) {
            if (binding.tier_id == 0) {
                throw std::invalid_argument("memory tier_id 0 is invalid");
            }
            if (binding.tier_name.empty()) {
                throw std::invalid_argument("memory tier_name must not be empty");
            }
            if (binding.api == nullptr) {
                throw std::invalid_argument(
                    "memory tier '" + binding.tier_name + "' has a null backend");
            }
            if (!names.insert(binding.tier_name).second) {
                throw std::invalid_argument(
                    "duplicate memory tier_name '" + binding.tier_name + "'");
            }
            if (!bindings_.emplace(binding.tier_id, binding).second) {
                throw std::invalid_argument(
                    "duplicate memory tier_id " +
                    std::to_string(binding.tier_id));
            }
            names_by_id_.emplace(binding.tier_id, binding.tier_name);
        }
    }

    AstraMemoryAPI* at(MemoryTierId tier_id, uint32_t device_id = 0) const {
        const auto found = bindings_.find(tier_id);
        if (found == bindings_.end()) {
            throw std::out_of_range(
                "unknown memory tier_id " + std::to_string(tier_id) +
                "; registered tier_ids=" + registered_ids());
        }
        const auto& binding = found->second;
        if (binding.num_devices != 0 && device_id >= binding.num_devices) {
            throw std::out_of_range(
                "memory tier_id " + std::to_string(tier_id) +
                " device_id " + std::to_string(device_id) +
                " is out of range [0," +
                std::to_string(binding.num_devices) + ")");
        }
        return binding.api;
    }

    std::size_t size() const {
        return bindings_.size();
    }

    std::string registered_ids() const {
        std::vector<MemoryTierId> ids;
        ids.reserve(bindings_.size());
        for (const auto& [tier_id, binding] : bindings_) {
            (void)binding;
            ids.push_back(tier_id);
        }
        std::sort(ids.begin(), ids.end());
        std::ostringstream output;
        output << "[";
        for (std::size_t index = 0; index < ids.size(); ++index) {
            if (index != 0) {
                output << ",";
            }
            output << ids[index];
        }
        output << "]";
        return output.str();
    }

  private:
    std::unordered_map<MemoryTierId, MemoryTierBinding> bindings_;
    std::unordered_map<MemoryTierId, std::string> names_by_id_;
};

inline std::vector<MemoryTierBinding> legacy_memory_tier_bindings(
    const std::vector<AstraMemoryAPI*>& memory_apis) {
    std::vector<MemoryTierBinding> bindings;
    bindings.reserve(memory_apis.size());
    std::unordered_set<uint32_t> ids;
    for (auto* api : memory_apis) {
        if (api == nullptr) {
            throw std::invalid_argument("legacy memory backend is null");
        }
        const auto location = api->get_memory_location_type();
        const auto tier_id = static_cast<uint32_t>(location);
        if (location == MemoryLocationType::INVALID_MEMORY || tier_id > 4) {
            throw std::invalid_argument("legacy memory backend has invalid location");
        }
        if (!ids.insert(tier_id).second) {
            throw std::invalid_argument(
                "duplicate legacy memory tier_id " + std::to_string(tier_id));
        }
        static const char* names[] = {"INVALID", "LOCAL", "REMOTE", "CXL", "STORAGE"};
        // Legacy backends do not expose their configured device count through
        // AstraMemoryAPI. Zero preserves the compatibility adapter's existing
        // behavior; native bindings always provide a validated positive count.
        bindings.push_back({tier_id, names[tier_id], 0, api});
    }
    return bindings;
}

}  // namespace AstraSim

#endif /* __MEMORY_TIER_REGISTRY_HH__ */
