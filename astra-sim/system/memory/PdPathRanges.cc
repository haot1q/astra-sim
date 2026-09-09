/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "PdPathWork.hh"

#include <algorithm>
#include <set>

#include "PdPathWire.hh"

namespace AstraSim {
namespace {
using namespace PdPathWire;
namespace Wire = ServiceBindingJson;
using Pair = std::pair<uint32_t, uint32_t>;

std::string object_digest(const char* prefix, const Json& body) {
    return std::string(prefix) + ":" + Wire::digest_bytes(body.dump(-1, ' ', true)).substr(7);
}

void validate_page_slice(const Json& buffer, const Json& catalog, const Json& page,
                         const Json& block) {
    Json key = page.at("key");
    key["schema_version"] = "page-identity-v1";
    require(object_digest("page-v1", key) == buffer.at("page_id"), "P/D Page key mismatch");
    Json block_key;
    for (const auto* field : {"request_id", "logical_block_index", "token_start", "token_capacity",
                              "allocated_bytes", "geometry_digest"}) block_key[field] = block.at(field);
    block_key["layout_policy"] = "simulator_vllm_style_block_v1";
    const auto block_id = object_digest("kv-block-v1", block_key);
    const auto offset = number(buffer.at("block_offset_bytes"));
    const auto bytes = number(buffer.at("length_bytes"), true);
    for (const auto& slice : Wire::array(catalog.at("slices"))) {
        if (slice.at("page_key") != page.at("key") || slice.at("stable_object_id") != block_id) continue;
        const auto start = number(slice.at("object_offset_bytes"));
        if (offset >= start && add(offset - start, bytes) <= number(slice.at("length_bytes"), true) &&
            add(number(slice.at("page_offset_bytes")), offset - start) == buffer.at("page_offset_bytes")) return;
    }
    require(false, "P/D buffer differs from original catalog Page slice");
}

void validate_page_buffer(const Json& buffer, const Json& body, const std::string& side) {
    const auto& context = body.at("context");
    const auto& endpoint = context.at("endpoints").at(side == "source" ? 0 : 1);
    const bool page_on = endpoint.at("manifest").contains("page_tiering") &&
                         !endpoint.at("manifest").at("page_tiering").is_null();
    if (!page_on) {
        require(buffer.at("page_id").is_null(), "P/D Page buffer requires a Page endpoint");
        return;
    }
    require(buffer.at("page_id").is_string(), "P/D Page endpoint requires an original Page buffer");
    const Json* selected = nullptr;
    for (const auto& catalog : Wire::array(endpoint.at("catalogs"))) {
        if (catalog.at("global_rank") == buffer.at("global_rank")) {
            require(selected == nullptr, "P/D duplicate rank catalog");
            selected = &catalog;
        }
    }
    require(selected != nullptr, "P/D buffer has no original rank catalog");
    const auto& catalog = *selected;
    require(catalog.at("owner_id") == buffer.at("owner_id") &&
            catalog.at("catalog_digest") == buffer.at("catalog_digest"), "P/D buffer catalog/owner mismatch");
    const auto index = number(buffer.at("block_index"));
    const auto& blocks = Wire::array(catalog.at("blocks"));
    const auto& expected_blocks = Wire::array(body.at("descriptor").at("blocks"));
    require(index < blocks.size() && index < expected_blocks.size(), "P/D block absent from catalog");
    const auto& block = blocks[index];
    for (const auto* field : {"request_id", "logical_block_index", "token_start", "token_capacity", "allocated_bytes"}) {
        require(block.at(field) == expected_blocks[index].at(field), "P/D catalog block mismatch");
    }
    if (side == "source") require(number(block.at("initialized_tokens")) >=
        number(expected_blocks[index].at("initialized_tokens")), "P/D source block is not initialized");
    const Json* selected_page = nullptr;
    for (const auto& page : Wire::array(catalog.at("pages"))) {
        if (page.at("page_id") == buffer.at("page_id")) {
            require(selected_page == nullptr, "P/D duplicate catalog Page");
            selected_page = &page;
        }
    }
    require(selected_page != nullptr, "P/D Page absent from original rank catalog");
    const auto& page = *selected_page;
    require(page.at("residency") == "hbm" && page.at("transient_state") == "idle" &&
            page.at("active_transaction_id").is_null() && page.at("in_flight_event_ids").empty() &&
            page.at("dirty") == false && page.at("hbm_location") == buffer.at("location"),
            "P/D Page must be prepared at its fixed Home HBM");
    require(add(number(buffer.at("page_offset_bytes")), number(buffer.at("length_bytes"), true)) <=
            number(page.at("allocated_bytes"), true), "P/D buffer exceeds Page bounds");
    validate_page_slice(buffer, catalog, page, block);
}

void validate_side(const Json& range, const std::string& side, const Json& body,
                   const std::map<std::string, Json>& buffers,
                   const std::map<std::string, Json>& registrations) {
    const auto& buffer = buffers.at(text(range.at(side + "_buffer_id")));
    const auto& registration = registrations.at(text(range.at(side + "_registration_id")));
    const auto& descriptor = body.at("descriptor");
    const auto offset = number(range.at(side + "_buffer_offset_bytes"));
    const auto bytes = number(range.at("length_bytes"), true);
    require(buffer.at("global_rank") == range.at(side + "_rank") &&
            buffer.at("instance_id") == descriptor.at(side + "_instance_id") &&
            buffer.at("request_id") == descriptor.at("request_id") &&
            number(buffer.at("generation")) == number(range.at(side + "_generation")),
            "P/D buffer rank/instance/request/generation mismatch");
    require(add(offset, bytes) <= number(buffer.at("length_bytes"), true) &&
            add(offset, number(buffer.at("payload_offset_bytes"))) == number(range.at("payload_offset_bytes")),
            "P/D range exceeds original buffer bounds");
    require(registration.at("buffer_id") == buffer.at("buffer_id") &&
            registration.at("buffer_generation") == buffer.at("generation") &&
            registration.at("run_id") == descriptor.at("run_id") &&
            registration.at("attempt_id") == descriptor.at("attempt_id"),
            "P/D registration identity mismatch");
    const auto registered_start = number(registration.at("offset_bytes"));
    const auto registered_end = add(registered_start, number(registration.at("length_bytes"), true));
    require(offset >= registered_start && add(offset, bytes) <= registered_end &&
            registered_end <= number(buffer.at("length_bytes")), "P/D registration bounds mismatch");
    std::set<std::string> permissions;
    for (const auto& value : Wire::array(registration.at("permissions"))) {
        require(permissions.insert(text(value)).second, "duplicate P/D registration permission");
    }
    require(permissions.count(side == "source" ? "local_read" : "local_write"),
            "P/D registration lacks local permission");
    const auto mode = body.at("profile").at("selection").at("initiation");
    if ((side == "source" && mode == "pull") || (side == "destination" && mode == "push")) {
        require(permissions.count(side == "source" ? "remote_read" : "remote_write"),
                "P/D registration lacks remote permission");
    }
    validate_page_buffer(buffer, body, side);
}

std::map<Pair, uint64_t> pairs(const Json& descriptor) {
    std::map<Pair, uint64_t> result;
    uint64_t total = 0;
    for (const auto& row : Wire::array(descriptor.at("rank_pairs"))) {
        const Pair pair{Wire::uint32(row.at("source_rank")), Wire::uint32(row.at("destination_rank"))};
        const auto bytes = number(row.at("charged_bytes"), true);
        require(pair.first != pair.second && result.emplace(pair, bytes).second,
                "duplicate or overlapping P/D rank pair");
        total = add(total, bytes);
    }
    require(!result.empty() && total == number(descriptor.at("charged_bytes_aggregate"), true),
            "P/D rank pair aggregate mismatch");
    return result;
}
}  // namespace

std::vector<PdPathSlice> validate_pd_path_ranges(const Json& body, uint64_t quantum) {
    require(quantum > 0, "P/D quantum must be positive");
    const auto expected = pairs(body.at("descriptor"));
    auto progress = expected;
    for (auto& [pair, bytes] : progress) bytes = 0;
    const auto& context = body.at("context");
    const auto buffers = index(context.at("buffers"), "buffer_id");
    const auto registrations = index(context.at("registrations"), "registration_id");
    std::vector<PdPathSlice> result;
    const auto& supplied = Wire::array(body.at("slices"));
    uint64_t range_index = 0;
    for (const auto& range : Wire::array(body.at("binding").at("ranges"))) {
        Wire::fields(range, {"source_rank", "destination_rank", "payload_offset_bytes", "length_bytes",
            "source_buffer_id", "source_buffer_offset_bytes", "source_generation", "source_registration_id",
            "destination_buffer_id", "destination_buffer_offset_bytes", "destination_generation",
            "destination_registration_id"});
        Pair pair{Wire::uint32(range["source_rank"]), Wire::uint32(range["destination_rank"])};
        const auto start = number(range["payload_offset_bytes"]);
        const auto end = add(start, number(range["length_bytes"], true));
        require(expected.count(pair) && progress.at(pair) == start && end <= expected.at(pair),
                "P/D range gap, overlap or unknown rank");
        for (const auto* side : {"source", "destination"}) {
            validate_side(range, side, body, buffers, registrations);
        }
        for (auto cursor = start; cursor < end;) {
            require(result.size() < supplied.size(), "P/D slice partition is incomplete");
            const auto bytes = std::min(end - cursor, quantum - cursor % quantum);
            PdPathSlice slice{result.size(), range_index, pair.first, pair.second, cursor, cursor - start, bytes};
            const Json expected_slice = {{"slice_id", slice.id}, {"range_index", range_index},
                {"source_rank", pair.first}, {"destination_rank", pair.second},
                {"payload_offset_bytes", cursor}, {"range_offset_bytes", cursor - start}, {"length_bytes", bytes}};
            require(supplied[result.size()] == expected_slice,
                    "P/D slices differ from canonical owner-range partition");
            result.push_back(slice);
            cursor += bytes;
        }
        progress.at(pair) = end;
        ++range_index;
    }
    require(progress == expected, "P/D ranges do not cover every original rank byte");
    require(result.size() == supplied.size(), "P/D slice partition has extra intervals");
    return result;
}
}  // namespace AstraSim
