/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>

#include "extern/memory_backend/analytical/AnalyticalMemory.hh"
#include "astra-sim/workload/Workload.hh"

namespace Analytical {

struct AnalyticalMemoryTestAccess {
    static std::size_t queue_index(
        const AnalyticalMemory& memory,
        uint32_t device_id,
        AstraSim::MemoryOperation operation) {
        return memory.queue_index(device_id, operation);
    }
};

}  // namespace Analytical

namespace {

using json = nlohmann::json;

std::string write_memory_config(const char* concurrency) {
    const auto path =
        std::filesystem::temp_directory_path() /
        (std::string("astra-memory-operation-") + concurrency + ".json");
    const uint64_t shared_bytes_per_second =
        std::string(concurrency) == "serialized" ? 1'000'000'000ULL
                                                  : 2'000'000'000ULL;
    const json payload = {
        {"memory-type", "PER_NODE_MEMORY_EXPANSION"},
        {"memory-location", "LOCAL_MEMORY"},
        {"mem-latency", 0},
        {"num-devices", 2},
        {"bandwidth-resource",
         {{"schema_version", "bandwidth-resource-v1"},
          {"read_bytes_per_second", 1'000'000'000ULL},
          {"write_bytes_per_second", 1'000'000'000ULL},
          {"shared_bytes_per_second", shared_bytes_per_second},
          {"concurrency", concurrency},
          {"turnaround_ns", 0}}},
    };
    std::ofstream output(path);
    output << payload;
    return path.string();
}

bool per_device_queue_contract() {
    using Analytical::AnalyticalMemory;
    using Analytical::AnalyticalMemoryTestAccess;
    using AstraSim::MemoryOperation;

    const auto serialized_path = write_memory_config("serialized");
    const AnalyticalMemory serialized(serialized_path);
    std::remove(serialized_path.c_str());
    const auto stack0_shared = AnalyticalMemoryTestAccess::queue_index(
        serialized, 0, MemoryOperation::Read);
    if (stack0_shared != AnalyticalMemoryTestAccess::queue_index(
                             serialized, 0, MemoryOperation::Write) ||
        stack0_shared == AnalyticalMemoryTestAccess::queue_index(
                             serialized, 1, MemoryOperation::Read)) {
        return false;
    }

    const auto simultaneous_path = write_memory_config("simultaneous");
    const AnalyticalMemory simultaneous(simultaneous_path);
    std::remove(simultaneous_path.c_str());
    const auto stack0_read = AnalyticalMemoryTestAccess::queue_index(
        simultaneous, 0, MemoryOperation::Read);
    const auto stack0_write = AnalyticalMemoryTestAccess::queue_index(
        simultaneous, 0, MemoryOperation::Write);
    const auto stack1_read = AnalyticalMemoryTestAccess::queue_index(
        simultaneous, 1, MemoryOperation::Read);
    const auto stack1_write = AnalyticalMemoryTestAccess::queue_index(
        simultaneous, 1, MemoryOperation::Write);
    return stack0_read != stack0_write && stack0_read != stack1_read &&
           stack0_read != stack1_write && stack0_write != stack1_read &&
           stack0_write != stack1_write && stack1_read != stack1_write;
}

bool contract_holds() {
    using AstraSim::MemoryOperation;
    using AstraSim::memory_operation_for_node_type;
    if (memory_operation_for_node_type(
            ChakraProtoMsg::NodeType::MEM_LOAD_NODE) !=
            MemoryOperation::Read ||
        memory_operation_for_node_type(
            ChakraProtoMsg::NodeType::MEM_STORE_NODE) !=
            MemoryOperation::Write) {
        return false;
    }
    try {
        memory_operation_for_node_type(
            ChakraProtoMsg::NodeType::COMP_NODE);
    } catch (const std::invalid_argument&) {
        return true;
    }
    return false;
}

}  // namespace

int main() {
    return contract_holds() && per_device_queue_contract() ? 0 : 1;
}
