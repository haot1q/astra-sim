/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <stdexcept>

#include "astra-sim/workload/Workload.hh"

namespace {

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
    return contract_holds() ? 0 : 1;
}
