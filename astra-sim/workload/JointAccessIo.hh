/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/
#ifndef __JOINT_ACCESS_IO_HH__
#define __JOINT_ACCESS_IO_HH__

#include <functional>
#include <string>
#include <unordered_map>

#include "astra-sim/workload/JointAccessIndex.hh"

namespace AstraSim::JointAccessIo {
using Json = nlohmann::json;
using Attributes = std::unordered_map<std::string, const ChakraProtoMsg::AttributeProto*>;
uint64_t u64(const Json& value, bool positive = false);
int64_t integer(const Json& value);
std::string text(const Json& value);
std::string hash(const Json& value);
uint64_t add(uint64_t first, uint64_t second);
uint64_t multiply(uint64_t first, uint64_t second);
std::array<uint64_t, 7> file_identity(const std::string& path);
Json read_json(const std::string& path, const JointAccessLimits& limits,
               uint64_t cap, std::string& raw);
Attributes attributes(const google::protobuf::RepeatedPtrField<ChakraProtoMsg::AttributeProto>& attrs);
void scan_et(const std::string& path, const JointAccessLimits& limits,
             const std::function<void(const ChakraProtoMsg::GlobalMetadata&)>& metadata,
             const std::function<void(const ChakraProtoMsg::Node&)>& node);
}  // namespace AstraSim::JointAccessIo
#endif
