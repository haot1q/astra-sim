/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __UCIE_LINK_REGISTRY_HH__
#define __UCIE_LINK_REGISTRY_HH__

#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "astra-sim/system/AstraMemoryAPI.hh"

namespace AstraSim {

struct UcieLinkBinding {
    std::string id;
    uint32_t stack_count;
    uint64_t header_bytes;
    uint64_t latency_ns;
    AstraMemoryAPI* api;
};

class UcieLinkRegistry {
  public:
    UcieLinkRegistry() = default;

    explicit UcieLinkRegistry(std::vector<UcieLinkBinding> bindings) {
        for (const auto& binding : bindings) {
            if (binding.id.empty() || binding.api == nullptr) {
                throw std::invalid_argument(
                    "UCIe link binding must have a non-empty id and backend");
            }
            if (binding.stack_count == 0) {
                throw std::invalid_argument(
                    "UCIe link '" + binding.id + "' stack_count must be positive");
            }
            if (!by_id_.emplace(binding.id, binding).second) {
                throw std::invalid_argument(
                    "duplicate UCIe link id '" + binding.id + "'");
            }
        }
    }

    bool empty() const {
        return by_id_.empty();
    }

    const UcieLinkBinding& at(const std::string& link_id) const {
        const auto found = by_id_.find(link_id);
        if (found == by_id_.end()) {
            throw std::out_of_range("unknown UCIe link_id '" + link_id + "'");
        }
        return found->second;
    }

    const std::unordered_map<std::string, UcieLinkBinding>& all() const {
        return by_id_;
    }

  private:
    std::unordered_map<std::string, UcieLinkBinding> by_id_;
};

}  // namespace AstraSim

#endif /* __UCIE_LINK_REGISTRY_HH__ */
