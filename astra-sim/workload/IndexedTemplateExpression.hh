/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __INDEXED_TEMPLATE_EXPRESSION_HH__
#define __INDEXED_TEMPLATE_EXPRESSION_HH__

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "extern/helper/json/json.hpp"

namespace AstraSim {

uint64_t checkedIndexedAdd(
    uint64_t left, uint64_t right, const std::string& operation);
uint64_t checkedIndexedMultiply(
    uint64_t left, uint64_t right, const std::string& operation);
using IndexedVectorBindings =
    std::unordered_map<std::string, std::vector<uint64_t>>;

nlohmann::json evaluateIndexedExpression(
    const nlohmann::json& expression,
    const std::unordered_map<std::string, uint64_t>& bindings,
    uint64_t index);
nlohmann::json evaluateIndexedExpression(
    const nlohmann::json& expression,
    const std::unordered_map<std::string, uint64_t>& bindings,
    const IndexedVectorBindings& vectors,
    uint64_t index);

}  // namespace AstraSim

#endif
