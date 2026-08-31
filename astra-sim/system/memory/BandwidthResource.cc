/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/BandwidthResource.hh"

#include <limits>
#include <stdexcept>
#include <unordered_set>
#include <utility>

namespace AstraSim {
namespace {

using json = nlohmann::json;

uint64_t uint64_value(
    const json& payload,
    const char* field,
    const std::string& context,
    bool require_positive) {
    if (!payload.contains(field) || !payload[field].is_number_integer()) {
        throw std::invalid_argument(
            context + "." + field + " must be an integer");
    }
    uint64_t value;
    if (payload[field].is_number_unsigned()) {
        value = payload[field].get<uint64_t>();
    } else {
        const auto signed_value = payload[field].get<int64_t>();
        if (signed_value < 0) {
            throw std::invalid_argument(
                context + "." + field + " must be non-negative");
        }
        value = static_cast<uint64_t>(signed_value);
    }
    if (require_positive && value == 0) {
        throw std::invalid_argument(
            context + "." + field + " must be positive");
    }
    return value;
}

uint64_t positive_uint64(
    const json& payload,
    const char* field,
    const std::string& context) {
    return uint64_value(payload, field, context, true);
}

uint64_t non_negative_uint64(
    const json& payload,
    const char* field,
    const std::string& context) {
    return uint64_value(payload, field, context, false);
}

uint64_t bandwidth_for(
    const BandwidthResourceConfig& config,
    MemoryOperation operation) {
    switch (operation) {
        case MemoryOperation::Read:
            return config.read_bytes_per_second;
        case MemoryOperation::Write:
            return config.write_bytes_per_second;
    }
    throw std::invalid_argument("unknown memory operation");
}

}  // namespace

BandwidthResourceConfig parse_bandwidth_resource_config(
    const json& payload,
    const std::string& context) {
    if (!payload.is_object()) {
        throw std::invalid_argument(context + " must be an object");
    }
    static const std::unordered_set<std::string> allowed = {
        "schema_version",
        "read_bytes_per_second",
        "write_bytes_per_second",
        "shared_bytes_per_second",
        "concurrency",
        "turnaround_ns",
    };
    for (auto entry = payload.begin(); entry != payload.end(); ++entry) {
        if (allowed.find(entry.key()) == allowed.end()) {
            throw std::invalid_argument(
                context + " has unknown field '" + entry.key() + "'");
        }
    }
    if (payload.value("schema_version", "") !=
        kBandwidthResourceSchemaVersion) {
        throw std::invalid_argument(
            context + ".schema_version must be bandwidth-resource-v1");
    }
    const auto read_bandwidth =
        positive_uint64(payload, "read_bytes_per_second", context);
    const auto write_bandwidth =
        positive_uint64(payload, "write_bytes_per_second", context);
    std::optional<uint64_t> shared_bandwidth;
    if (payload.contains("shared_bytes_per_second")) {
        shared_bandwidth =
            positive_uint64(payload, "shared_bytes_per_second", context);
    }
    const auto concurrency_text = payload.value("concurrency", "");
    BandwidthConcurrency concurrency;
    if (concurrency_text == "serialized") {
        concurrency = BandwidthConcurrency::Serialized;
    } else if (concurrency_text == "simultaneous") {
        concurrency = BandwidthConcurrency::Simultaneous;
    } else {
        throw std::invalid_argument(
            context + ".concurrency must be serialized or simultaneous");
    }
    const auto turnaround =
        non_negative_uint64(payload, "turnaround_ns", context);
    const BandwidthResource validated({
        read_bandwidth,
        write_bandwidth,
        shared_bandwidth,
        concurrency,
        turnaround,
    });
    return validated.config();
}

json bandwidth_resource_config_to_json(
    const BandwidthResourceConfig& config) {
    const BandwidthResource validated(config);
    (void)validated;
    json payload = {
        {"schema_version", kBandwidthResourceSchemaVersion},
        {"read_bytes_per_second", config.read_bytes_per_second},
        {"write_bytes_per_second", config.write_bytes_per_second},
        {"concurrency",
         config.concurrency == BandwidthConcurrency::Serialized
             ? "serialized"
             : "simultaneous"},
        {"turnaround_ns", config.turnaround_ns},
    };
    if (config.shared_bytes_per_second.has_value()) {
        payload["shared_bytes_per_second"] =
            *config.shared_bytes_per_second;
    }
    return payload;
}

BandwidthResource::BandwidthResource(BandwidthResourceConfig config)
    : config_(std::move(config)) {
    if (config_.read_bytes_per_second == 0 ||
        config_.write_bytes_per_second == 0) {
        throw std::invalid_argument(
            "direction bandwidth must be a positive number of bytes/s");
    }
    if (config_.shared_bytes_per_second.has_value() &&
        *config_.shared_bytes_per_second == 0) {
        throw std::invalid_argument("shared bandwidth must be positive");
    }
    if (config_.concurrency == BandwidthConcurrency::Serialized) {
        if (!config_.shared_bytes_per_second.has_value()) {
            throw std::invalid_argument(
                "serialized bandwidth resource requires a shared cap");
        }
        if (config_.read_bytes_per_second >
                *config_.shared_bytes_per_second ||
            config_.write_bytes_per_second >
                *config_.shared_bytes_per_second) {
            throw std::invalid_argument(
                "direction bandwidth must not exceed the shared cap");
        }
    } else {
        if (config_.turnaround_ns != 0) {
            throw std::invalid_argument(
                "simultaneous bandwidth resource requires zero turnaround");
        }
        if (config_.shared_bytes_per_second.has_value()) {
            const unsigned __int128 sum =
                static_cast<unsigned __int128>(
                    config_.read_bytes_per_second) +
                config_.write_bytes_per_second;
            if (sum > *config_.shared_bytes_per_second) {
                throw std::invalid_argument(
                    "simultaneous direction caps exceed the shared cap");
            }
        }
    }
}

uint64_t BandwidthResource::service_time_ns(
    uint64_t bytes,
    MemoryOperation operation,
    uint64_t latency_ns) const {
    if (bytes == 0) {
        throw std::invalid_argument("memory request bytes must be positive");
    }
    const uint64_t bandwidth = bandwidth_for(config_, operation);
    const unsigned __int128 transfer_ns =
        static_cast<unsigned __int128>(bytes) * 1'000'000'000ULL /
        bandwidth;
    const unsigned __int128 total_ns = transfer_ns + latency_ns;
    if (total_ns > std::numeric_limits<uint64_t>::max()) {
        throw std::overflow_error("memory request runtime exceeds uint64 ns");
    }
    return static_cast<uint64_t>(total_ns);
}

std::size_t BandwidthResource::server_index(
    MemoryOperation operation) const {
    if (config_.concurrency == BandwidthConcurrency::Serialized) {
        (void)bandwidth_for(config_, operation);
        return 0;
    }
    switch (operation) {
        case MemoryOperation::Read:
            return 0;
        case MemoryOperation::Write:
            return 1;
    }
    throw std::invalid_argument("unknown memory operation");
}

std::size_t BandwidthResource::server_count() const {
    return config_.concurrency == BandwidthConcurrency::Serialized ? 1 : 2;
}

uint64_t BandwidthResource::turnaround_delay_ns(
    MemoryOperation previous,
    MemoryOperation current) const {
    (void)bandwidth_for(config_, previous);
    (void)bandwidth_for(config_, current);
    if (config_.concurrency == BandwidthConcurrency::Serialized &&
        previous != current) {
        return config_.turnaround_ns;
    }
    return 0;
}

const BandwidthResourceConfig& BandwidthResource::config() const {
    return config_;
}

}  // namespace AstraSim
