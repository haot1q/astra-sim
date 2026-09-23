// Resource-neutral dependency timing; no compute or DMA service is implied.
#ifndef ASTRA_SIM_DEPENDENCY_DELAY_HH
#define ASTRA_SIM_DEPENDENCY_DELAY_HH

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>

#include "astra-sim/workload/WorkloadFeeder.hh"

namespace AstraSim {

inline std::optional<uint64_t> dependency_delay_ns(
    const std::shared_ptr<Chakra::ETFeederNode>& node) {
    const bool schema = node->has_other_attr("dependency_delay_schema");
    const bool duration = node->has_other_attr("dependency_delay_ns");
    if (!schema && !duration) {
        return std::nullopt;
    }
    if (!schema || !duration) {
        throw std::invalid_argument("incomplete dependency-delay contract");
    }
    const auto raw = node->getChakraNode();
    unsigned schema_count = 0;
    unsigned duration_count = 0;
    for (const auto& attr : raw->attr()) {
        schema_count += attr.name() == "dependency_delay_schema";
        duration_count += attr.name() == "dependency_delay_ns";
    }
    if (raw->attr_size() != 2 || schema_count != 1 || duration_count != 1 ||
        raw->ctrl_deps_size() != 0) {
        throw std::invalid_argument("invalid dependency-delay attributes or payload");
    }
    const auto& version = node->get_other_attr("dependency_delay_schema");
    const auto& delay = node->get_other_attr("dependency_delay_ns");
    if (!version.has_string_val() ||
        version.string_val() != "dependency-delay-v1" ||
        !delay.has_uint64_val() || node->runtime() != 0 ||
        node->type() != ChakraProtoMsg::NodeType::INVALID_NODE ||
        node->is_cpu_op() ||
        node->has_other_attr("memory_movement_schema_version")) {
        throw std::invalid_argument("invalid dependency-delay-v1 contract");
    }
    return delay.uint64_val();
}

inline void validate_dependency_delay_deadline(uint64_t ready, uint64_t duration) {
    if (duration > std::numeric_limits<uint64_t>::max() - ready) {
        throw std::overflow_error("dependency delay completion time overflows");
    }
}

}  // namespace AstraSim
#endif
