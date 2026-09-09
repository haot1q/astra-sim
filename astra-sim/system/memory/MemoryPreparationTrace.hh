/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#ifndef __MEMORY_PREPARATION_TRACE_HH__
#define __MEMORY_PREPARATION_TRACE_HH__

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace Chakra {
class ETFeederNode;
}

namespace AstraSim {

struct MemoryPreparationTrace {
    std::string manifest_digest;
    std::string binding_digest;
    std::string activation_id;
    uint32_t rank;
    std::vector<std::shared_ptr<Chakra::ETFeederNode>> nodes;
};

// Preparation artifacts are uncompressed, length-delimited protobuf records.
// Audit every record before dispatch, independent of ETFeeder's ready window.
MemoryPreparationTrace read_memory_preparation_trace(
    const std::string& path, std::size_t expected_nodes);

}  // namespace AstraSim

#endif
