/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __SERVICE_BINDING_JSON_HH__
#define __SERVICE_BINDING_JSON_HH__

#include <cstdint>
#include <initializer_list>
#include <string>

#include "extern/helper/json/json.hpp"

namespace AstraSim::ServiceBindingJson {

using Json = nlohmann::json;
Json read(const std::string& path);
void fields(const Json& raw, std::initializer_list<const char*> expected);
uint32_t uint32(const Json& raw);
std::string name(const Json& raw);
const Json& array(const Json& raw);
std::string digest(const Json& body);

}  // namespace AstraSim::ServiceBindingJson

#endif
