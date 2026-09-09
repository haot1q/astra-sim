/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#include "MemoryPreparationTrace.hh"

#include <fstream>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>

#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"

namespace AstraSim {
namespace {

// A protocol-input bound, not a Page size or transfer quantum. Oversized records
// must fail before allocation; each eventual IPC receipt has its own pipe limit.
constexpr uint32_t kMaxRecordBytes = 64 * 1024;

std::string read_record(std::ifstream& input) {
    uint32_t size = 0;
    for (unsigned index = 0; index < 5; ++index) {
        const int next = input.get();
        if (next == std::char_traits<char>::eof()) {
            throw std::invalid_argument("truncated preparation ET record length");
        }
        const auto byte = static_cast<unsigned char>(next);
        if (index == 4 && (byte & 0xf0) != 0) {
            throw std::invalid_argument("preparation ET length overflows uint32");
        }
        size |= static_cast<uint32_t>(byte & 0x7f) << (index * 7);
        if ((byte & 0x80) != 0) continue;
        if (size == 0 || size > kMaxRecordBytes) {
            throw std::invalid_argument("preparation ET record exceeds the nonzero 64 KiB bound");
        }
        std::string record(size, '\0');
        input.read(record.data(), static_cast<std::streamsize>(size));
        if (input.gcount() != static_cast<std::streamsize>(size)) {
            throw std::invalid_argument("truncated preparation ET record body");
        }
        return record;
    }
    throw std::invalid_argument("invalid preparation ET record length");
}

MemoryPreparationTrace read_metadata(std::ifstream& input) {
    ChakraProtoMsg::GlobalMetadata metadata;
    if (!metadata.ParseFromString(read_record(input))) {
        throw std::invalid_argument("malformed preparation ET metadata");
    }
    MemoryPreparationTrace result;
    std::optional<uint32_t> rank;
    std::set<std::string> names;
    for (const auto& attr : metadata.attr()) {
        if (attr.name().empty() || !names.insert(attr.name()).second) {
            throw std::invalid_argument("duplicate/empty preparation metadata attribute");
        }
        if (attr.name() == "service_rank") {
            if (!attr.has_uint64_val() || attr.uint64_val() > UINT32_MAX) {
                throw std::invalid_argument("invalid preparation metadata rank");
            }
            rank = static_cast<uint32_t>(attr.uint64_val());
            continue;
        }
        std::string* identity = nullptr;
        if (attr.name() == "tier_manifest_digest") identity = &result.manifest_digest;
        if (attr.name() == "service_binding_digest") identity = &result.binding_digest;
        if (attr.name() == "service_activation_id") identity = &result.activation_id;
        if (attr.name() == "pipeline_stage_digest") identity = &result.pipeline_stage_digest;
        if (identity != nullptr) {
            if (!attr.has_string_val() || attr.string_val().empty()) {
                throw std::invalid_argument("invalid preparation metadata identity");
            }
            *identity = attr.string_val();
        }
    }
    if (!rank.has_value() || result.manifest_digest.empty() ||
        result.binding_digest.empty() || result.activation_id.empty()) {
        throw std::invalid_argument("preparation ET requires complete native service identity");
    }
    result.rank = *rank;
    return result;
}

std::shared_ptr<Chakra::ETFeederNode> read_node(std::ifstream& input) {
    auto node = std::make_shared<ChakraProtoMsg::Node>();
    if (!node->ParseFromString(read_record(input))) {
        throw std::invalid_argument("malformed preparation ET node");
    }
    if ((node->type() != ChakraProtoMsg::MEM_LOAD_NODE &&
         node->type() != ChakraProtoMsg::MEM_STORE_NODE) ||
        !node->data_deps().empty() || !node->ctrl_deps().empty()) {
        throw std::invalid_argument("preparation ET contains compute or graph dependencies");
    }
    std::set<std::string> attributes;
    for (const auto& attr : node->attr()) {
        if (attr.name().empty() || !attributes.insert(attr.name()).second) {
            throw std::invalid_argument("duplicate/empty preparation node attribute");
        }
        if (attr.name() == "tensor_size" &&
            (!attr.has_uint64_val() || attr.uint64_val() == 0)) {
            throw std::invalid_argument("preparation tensor_size requires positive uint64");
        }
        if ((attr.name() == "tensor_loc" || attr.name() == "tensor_device") &&
            !attr.has_uint32_val()) {
            throw std::invalid_argument("preparation tensor location/device requires uint32");
        }
        if (attr.name() == "tensor_loc" && attr.uint32_val() == 0) {
            throw std::invalid_argument("preparation tensor location zero is reserved");
        }
    }
    for (const auto* required : {"tensor_size", "tensor_loc", "tensor_device"}) {
        if (!attributes.count(required)) {
            throw std::invalid_argument("preparation ET is missing required tensor attributes");
        }
    }
    return std::make_shared<Chakra::ETFeederNode>(node);
}

}  // namespace

MemoryPreparationTrace read_memory_preparation_trace(
    const std::string& path, std::size_t expected_nodes) {
    if (expected_nodes == 0) {
        throw std::invalid_argument("preparation ET requires a non-empty event set");
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::invalid_argument("cannot open preparation ET");
    auto result = read_metadata(input);
    std::set<uint64_t> node_ids;
    while (input.peek() != std::char_traits<char>::eof()) {
        if (result.nodes.size() >= expected_nodes) {
            throw std::invalid_argument("preparation ET has extra undeclared records");
        }
        auto node = read_node(input);
        if (!node_ids.insert(node->id()).second) {
            throw std::invalid_argument("duplicate preparation ET node ID");
        }
        result.nodes.push_back(std::move(node));
    }
    if (input.bad() || result.nodes.size() != expected_nodes) {
        throw std::invalid_argument("preparation ET is incomplete or unreadable");
    }
    return result;
}

}  // namespace AstraSim
