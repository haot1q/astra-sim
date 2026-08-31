/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>

#include "astra-sim/system/memory/BandwidthResource.hh"

namespace {

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
    using AstraSim::BandwidthConcurrency;
    using AstraSim::BandwidthResource;
    using AstraSim::BandwidthResourceConfig;
    using AstraSim::MemoryOperation;

    const BandwidthResource independent({
        2'000'000'000ULL,
        1'000'000'000ULL,
        std::nullopt,
        BandwidthConcurrency::Simultaneous,
        0,
    });
    if (independent.service_time_ns(1'000'000'000ULL,
                                    MemoryOperation::Read, 10) !=
            500'000'010ULL ||
        independent.service_time_ns(1'000'000'000ULL,
                                    MemoryOperation::Write, 10) !=
            1'000'000'010ULL ||
        independent.server_index(MemoryOperation::Read) ==
            independent.server_index(MemoryOperation::Write)) {
        return false;
    }

    const BandwidthResource serialized({
        4'000'000'000ULL,
        2'000'000'000ULL,
        4'000'000'000ULL,
        BandwidthConcurrency::Serialized,
        25,
    });
    if (serialized.server_index(MemoryOperation::Read) !=
            serialized.server_index(MemoryOperation::Write) ||
        serialized.turnaround_delay_ns(
            MemoryOperation::Read, MemoryOperation::Write) != 25 ||
        serialized.turnaround_delay_ns(
            MemoryOperation::Read, MemoryOperation::Read) != 0) {
        return false;
    }

    const BandwidthResource statically_shared({
        2'000'000'000ULL,
        1'000'000'000ULL,
        3'000'000'000ULL,
        BandwidthConcurrency::Simultaneous,
        0,
    });
    const uint64_t interval_ns = 1'000'000'000ULL;
    const uint64_t read_bytes = 2'000'000'000ULL;
    const uint64_t write_bytes = 1'000'000'000ULL;
    if (statically_shared.service_time_ns(
            read_bytes, MemoryOperation::Read) != interval_ns ||
        statically_shared.service_time_ns(
            write_bytes, MemoryOperation::Write) != interval_ns ||
        read_bytes + write_bytes >
            *statically_shared.config().shared_bytes_per_second) {
        return false;
    }

    return throws<std::invalid_argument>([]() {
               BandwidthResource invalid({
                   1, 1, std::nullopt, BandwidthConcurrency::Serialized, 0});
           }) &&
           throws<std::invalid_argument>([]() {
               BandwidthResource invalid({
                   2, 2, 3, BandwidthConcurrency::Simultaneous, 0});
           }) &&
           throws<std::invalid_argument>([]() {
               BandwidthResource invalid({
                   1, 1, std::nullopt, BandwidthConcurrency::Simultaneous, 1});
           }) &&
           throws<std::invalid_argument>([&]() {
               independent.service_time_ns(0, MemoryOperation::Read);
           }) &&
           throws<std::invalid_argument>([&]() {
               independent.service_time_ns(
                   1, static_cast<MemoryOperation>(255));
           }) &&
           throws<std::overflow_error>([]() {
               BandwidthResource resource({
                   1, 1, std::nullopt, BandwidthConcurrency::Simultaneous, 0});
               resource.service_time_ns(
                   std::numeric_limits<uint64_t>::max(),
                   MemoryOperation::Read,
                   std::numeric_limits<uint64_t>::max());
           });
}

}  // namespace

int main() {
    return contract_holds() ? 0 : 1;
}
