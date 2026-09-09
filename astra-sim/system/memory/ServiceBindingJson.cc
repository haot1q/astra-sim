/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "ServiceBindingJson.hh"

#include <fstream>
#include <iomanip>
#include <limits>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <vector>

#include <openssl/evp.h>

namespace AstraSim::ServiceBindingJson {

Json read(const std::string& path) {
    std::ifstream stream(path);
    if (!stream) throw std::invalid_argument("cannot open physical service bindings");
    std::ostringstream contents;
    contents << stream.rdbuf();
    return parse(contents.str());
}

Json parse(const std::string& text) {
    std::vector<std::set<std::string>> keys;
    auto callback = [&keys](int, Json::parse_event_t event, Json& value) {
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        if (event == Json::parse_event_t::key &&
            !keys.back().insert(value.get<std::string>()).second) {
            throw std::invalid_argument("duplicate physical service JSON key");
        }
        if (event == Json::parse_event_t::object_end) keys.pop_back();
        return true;
    };
    return Json::parse(text, callback);
}

void fields(const Json& raw, std::initializer_list<const char*> expected) {
    if (!raw.is_object() || raw.size() != expected.size()) {
        throw std::invalid_argument("physical service object has missing/unknown fields");
    }
    for (const auto* key : expected) {
        if (!raw.contains(key)) {
            throw std::invalid_argument(std::string("missing physical service field ") + key);
        }
    }
}

uint32_t uint32(const Json& raw) {
    if (!raw.is_number_unsigned() || raw.get<uint64_t>() > UINT32_MAX) {
        throw std::invalid_argument("physical service identifier must be uint32");
    }
    return raw.get<uint32_t>();
}

std::string name(const Json& raw) {
    static const std::regex identifier("^[A-Za-z0-9_.:-]+$");
    if (!raw.is_string() || !std::regex_match(raw.get<std::string>(), identifier)) {
        throw std::invalid_argument("physical service name must be a nonempty ASCII identifier");
    }
    return raw.get<std::string>();
}

const Json& array(const Json& raw) {
    if (!raw.is_array()) throw std::invalid_argument("physical service field must be array");
    return raw;
}

namespace {
void reject_floats(const Json& value) {
    if (value.is_number_float()) {
        throw std::invalid_argument("physical service canonical JSON cannot contain floats");
    }
    if (value.is_structured()) {
        for (const auto& child : value) reject_floats(child);
    }
}
}  // namespace

std::string digest(const Json& body, bool ensure_ascii) {
    reject_floats(body);
    const auto canonical = body.dump(-1, ' ', ensure_ascii);
    return digest_bytes(canonical);
}

std::string digest_bytes(const std::string& canonical) {
    unsigned char bytes[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    if (EVP_Digest(canonical.data(), canonical.size(), bytes, &length,
                   EVP_sha256(), nullptr) != 1 || length != 32) {
        throw std::runtime_error("physical service SHA-256 failed");
    }
    std::ostringstream output;
    output << "sha256:" << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < length; ++index) {
        output << std::setw(2) << static_cast<unsigned>(bytes[index]);
    }
    return output.str();
}

}  // namespace AstraSim::ServiceBindingJson
