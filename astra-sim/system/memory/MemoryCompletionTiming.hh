#ifndef __MEMORY_COMPLETION_TIMING_HH__
#define __MEMORY_COMPLETION_TIMING_HH__

#include <stdexcept>
#include <utility>
#include "astra-sim/system/WorkloadLayerHandlerData.hh"

namespace AstraSim {
inline std::pair<uint64_t, uint64_t> memory_completion_timing(
    const WorkloadLayerHandlerData& data, uint64_t now, bool allow_aggregate) {
    if (!(data.memory_ready_ns <= data.memory_start_ns && data.memory_start_ns <= data.memory_finish_ns &&
          data.memory_finish_ns == now) ||
        data.memory_total_wait_ns.has_value() != data.memory_total_service_ns.has_value() ||
        (!allow_aggregate && data.memory_total_wait_ns)) {
        throw std::logic_error("invalid analytical memory endpoint timing");
    }
    const auto wait = data.memory_total_wait_ns.value_or(data.memory_start_ns - data.memory_ready_ns);
    const auto service = data.memory_total_service_ns.value_or(data.memory_finish_ns - data.memory_start_ns);
    if (wait > UINT64_MAX - service || wait + service != data.memory_finish_ns - data.memory_ready_ns) {
        throw std::logic_error("invalid memory aggregate timing");
    }
    return {wait, service};
}
}  // namespace AstraSim
#endif
