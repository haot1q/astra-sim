/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __PD_PATH_WIRE_HH__
#define __PD_PATH_WIRE_HH__

#include <limits>
#include <map>
#include <stdexcept>
#include <string>
#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSim::PdPathWire {
using Json = nlohmann::json;

inline void require(bool condition, const char* diagnostic) {
    if (!condition) throw std::invalid_argument(diagnostic);
}

inline uint64_t number(const Json& raw, bool positive = false) {
    require(raw.is_number_unsigned(), "P/D path requires uint64");
    auto value = raw.get<uint64_t>();
    require(!positive || value > 0, "P/D path requires positive uint64");
    return value;
}

inline uint64_t add(uint64_t a, uint64_t b) {
    require(b <= UINT64_MAX - a, "P/D path byte/time overflow");
    return a + b;
}

inline std::string text(const Json& raw) {
    require(raw.is_string() && !raw.get<std::string>().empty(), "P/D path requires text");
    return raw.get<std::string>();
}

inline std::map<std::string, Json> index(const Json& values, const char* field) {
    std::map<std::string, Json> result;
    for (const auto& row : ServiceBindingJson::array(values)) {
        require(result.emplace(text(row.at(field)), row).second, "duplicate P/D path identity");
    }
    return result;
}
}  // namespace AstraSim::PdPathWire
#endif
