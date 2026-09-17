/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/IndexedTemplateExpression.hh"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace AstraSim {
namespace {

using Json = nlohmann::json;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::invalid_argument("template-v3: " + message);
    }
}

uint64_t unsigned_value(const Json& value, const std::string& operation) {
    require(value.is_number_unsigned(), operation + " must resolve to uint64");
    return value.get<uint64_t>();
}

}  // namespace

uint64_t checkedIndexedAdd(
    uint64_t left, uint64_t right, const std::string& operation) {
    require(right <= std::numeric_limits<uint64_t>::max() - left,
            operation + " overflow");
    return left + right;
}

uint64_t checkedIndexedMultiply(
    uint64_t left, uint64_t right, const std::string& operation) {
    require(left == 0 ||
                right <= std::numeric_limits<uint64_t>::max() / left,
            operation + " overflow");
    return left * right;
}

Json evaluateIndexedExpression(
    const Json& expression,
    const std::unordered_map<std::string, uint64_t>& bindings,
    uint64_t index) {
    if (!expression.is_object()) return expression;
    require(expression.size() == 1,
            "expression must contain one operation");
    const auto& item = *expression.begin();
    const auto operation = expression.begin().key();
    if (operation == "param") {
        require(item.is_string() && !item.get<std::string>().empty(),
                "expression param must be a nonempty string");
        const auto found = bindings.find(item.get<std::string>());
        require(found != bindings.end(), "expression references unknown param");
        return found->second;
    }
    if (operation == "index") {
        require(item.is_boolean() && item.get<bool>(),
                "index expression must be true");
        return index;
    }
    require(item.is_array() && item.size() == 2,
            operation + " expression requires two arguments");
    const auto left = unsigned_value(
        evaluateIndexedExpression(item.at(0), bindings, index), operation);
    const auto right = unsigned_value(
        evaluateIndexedExpression(item.at(1), bindings, index), operation);
    if (operation == "add") {
        return checkedIndexedAdd(left, right, operation);
    }
    if (operation == "mul") {
        return checkedIndexedMultiply(left, right, operation);
    }
    if (operation == "min") return std::min(left, right);
    if (operation == "ceil_div") {
        require(right > 0, "ceil_div divisor must be positive");
        return left / right + static_cast<uint64_t>(left % right != 0);
    }
    if (operation == "sub") {
        require(right <= left, "sub underflow");
        return left - right;
    }
    throw std::invalid_argument(
        "template-v3: unsupported expression " + operation);
}

}  // namespace AstraSim
