#include "common/JointSimulationCoordinator.hh"
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <stdexcept>

namespace AstraSimAnalytical {
namespace {
// Same host clock as Python time.monotonic_ns() on the supported Linux frontend.
uint64_t monotonic_ns() {
    timespec now{};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        throw std::runtime_error("joint view timer failed");
    return static_cast<uint64_t>(now.tv_sec) * 1000000000ULL + now.tv_nsec;
}
}

void JointSimulationCoordinator::begin_view_timing() {
    if (!phase_timing_) return;
    if (phase_target_) throw std::runtime_error("predecessor view has unfinished measured work");
    const auto expected = JointRuntimeClient::decimal(status_.at("view_expected_nodes"));
    const auto completed = JointRuntimeClient::decimal(status_.at("completed_nodes"));
    if (expected > UINT64_MAX - completed) throw std::overflow_error("view node count overflow");
    phase_target_ = expected + completed;
    phase_start_ns_ = monotonic_ns();
    observe_view_completion();
}

void JointSimulationCoordinator::observe_view_completion() {
    if (!phase_target_ || !pending_.empty() || !receiving_.empty()) return;
    const auto completed = JointRuntimeClient::decimal(status_.at("completed_nodes"));
    if (completed > *phase_target_) throw std::runtime_error("view completed too many nodes");
    if (completed != *phase_target_ || status_.value("more_pending", false) ||
        status_.at("bridge").at("active") != 0) return;
    const auto end = monotonic_ns();
    const auto& sequence = view_identity_.at("view_sequence");
    const auto path = std::filesystem::path(config_.at("output").get<std::string>()) /
        ("view-timing-" + sequence.get<std::string>() + ".json");
    const Json row{{"schema","simulator-joint-view-time-v1"}, {"run_id",config_.at("run_id")},
        {"view",view_identity_}, {"clock","CLOCK_MONOTONIC"},
        {"ready_host_ns",std::to_string(phase_start_ns_)}, {"complete_host_ns",std::to_string(end)},
        {"completed_nodes",std::to_string(completed)}, {"through_fs",std::to_string(frontier_fs_)}};
    const auto bytes = row.dump() + "\n";
    FILE* file = std::fopen(path.c_str(),"wx");
    if (!file) throw std::runtime_error("cannot create fresh joint view timing receipt");
    const bool written = std::fwrite(bytes.data(),1,bytes.size(),file) == bytes.size();
    const int result = std::fclose(file);
    if (!written || result != 0) throw std::runtime_error("joint view timing receipt write failed");
    phase_target_.reset();
}
}
