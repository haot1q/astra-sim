/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <stdexcept>

#include "astra-sim/system/MemoryTierRegistry.hh"

namespace {

class MockMemory : public AstraSim::AstraMemoryAPI {
  public:
    void set_sys(int, AstraSim::Sys*) override {}
    void issue(uint64_t, AstraSim::WorkloadLayerHandlerData*) override {}
    AstraSim::MemoryLocationType get_memory_location_type() const override {
        return AstraSim::MemoryLocationType::INVALID_MEMORY;
    }
};

template <typename Error, typename Fn>
bool throws(Fn fn) {
    try {
        fn();
    } catch (const Error&) {
        return true;
    }
    return false;
}

bool contract_holds() {
    MockMemory hbm;
    MockMemory lpddr;
    MockMemory remote;
    AstraSim::MemoryTierRegistry registry({
        {16, "hbm", 1, &hbm},
        {17, "lpddr", 2, &lpddr},
        {18, "remote", 1, &remote}});
    return registry.size() == 3 && registry.at(16) == &hbm &&
           registry.at(17) == &lpddr && registry.at(18) == &remote &&
           throws<std::invalid_argument>([&]() {
               AstraSim::MemoryTierRegistry bad({{0, "bad", 1, &hbm}});
           }) &&
           throws<std::invalid_argument>([&]() {
               AstraSim::MemoryTierRegistry bad(
                   {{16, "hbm", 1, &hbm}, {16, "lpddr", 1, &lpddr}});
           }) &&
           throws<std::out_of_range>([&]() { registry.at(17, 2); }) &&
           throws<std::out_of_range>([&]() { registry.at(99); });
}

}  // namespace

int main() {
    return contract_holds() ? 0 : 1;
}
