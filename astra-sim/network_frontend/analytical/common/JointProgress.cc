#include "common/JointProgress.hh"
#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <utility>

namespace AstraSimAnalytical {
namespace {
uint64_t integer(const JointProgress::Json& value) {
    if (value.is_number_unsigned()) return value.get<uint64_t>();
    if (value.is_number_integer() && value.get<int64_t>() >= 0) return value.get<uint64_t>();
    throw std::invalid_argument("joint progress requires nonnegative integer bridge counters");
}
constexpr const char* names[] = {"completed", "issued", "completed_nodes", "admitted_nodes"};
}

JointProgress::JointProgress(double timeout_seconds, Sink sink, Clock clock)
    : sink_(std::move(sink)), clock_(std::move(clock)), interval_(std::min(1.0, timeout_seconds / 4)) {
    if (!std::isfinite(timeout_seconds) || timeout_seconds <= 0 || !sink_ || !clock_)
        throw std::invalid_argument("joint progress requires finite positive timeout and observers");
}

void JointProgress::observe(const Json& run_id, const Json& identity, const Json& status, bool flush) {
    if (!run_id.is_string() || run_id.get_ref<const std::string&>().empty() ||
        !identity.is_object() || identity.size() != 4 || status.at("view_identity") != identity ||
        status.at("bridge").at("failed") != false)
        throw std::invalid_argument("joint progress current identity or runtime failure differs");
    const auto& bridge = status.at("bridge");
    const std::array<uint64_t, 4> counts{integer(bridge.at("completed")), integer(bridge.at("issued")),
        JointRuntimeClient::decimal(status.at("completed_nodes")),
        JointRuntimeClient::decimal(status.at("admitted_nodes"))};
    if (counts[0] > counts[1] || counts[2] > counts[3])
        throw std::invalid_argument("joint progress completion exceeds admission");
    for (size_t i = 0; i < counts.size(); ++i)
        if (counts[i] < observed_[i]) throw std::invalid_argument("joint progress counters decreased");
    if (!identity_.is_null() && identity != identity_) {
        if (run_id != run_id_ || identity.at("session_id") != identity_.at("session_id") ||
            JointRuntimeClient::decimal(identity_.at("view_sequence")) == UINT64_MAX ||
            JointRuntimeClient::decimal(identity.at("view_sequence")) !=
                JointRuntimeClient::decimal(identity_.at("view_sequence")) + 1 ||
            JointRuntimeClient::decimal(identity.at("execution_iteration")) <=
                JointRuntimeClient::decimal(identity_.at("execution_iteration")) ||
            emitted_[0] != observed_[0] || emitted_[2] != observed_[2])
            throw std::invalid_argument("joint progress handoff lost prior identity or pending completion");
    } else if (!identity_.is_null() && run_id != run_id_) {
        throw std::invalid_argument("joint progress run identity changed");
    }
    identity_ = identity;
    run_id_ = run_id;
    observed_ = counts;
    // Compare against the last notification, not just the preceding reply.
    if (counts[0] == emitted_[0] && counts[2] == emitted_[2]) return;
    const auto now = clock_();
    if (emitted_at_ && now < *emitted_at_)
        throw std::invalid_argument("joint progress host clock decreased");
    if (!flush && emitted_at_ && now - *emitted_at_ < interval_) return;
    Json message = identity;
    message["schema"] = "simulator-joint-progress-v1";
    message["run_id"] = run_id;
    for (size_t i = 0; i < counts.size(); ++i) message[names[i]] = std::to_string(counts[i]);
    const auto line = "JOINT_PROGRESS_V1 " + message.dump();
    if (line.size() > 64U * 1024U) throw std::invalid_argument("joint progress protocol line limit");
    sink_(line);
    emitted_ = counts;
    emitted_at_ = now;
}
}
