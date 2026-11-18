#ifndef ASTRA_ANALYTICAL_JOINT_PROGRESS_HH
#define ASTRA_ANALYTICAL_JOINT_PROGRESS_HH

#include "common/JointRuntimeClient.hh"
#include <array>
#include <chrono>
#include <functional>
#include <optional>
#include <string>

namespace AstraSimAnalytical {
// A bounded observer, never a model owner, clock grant or heartbeat generator.
class JointProgress {
  public:
    using Json = JointRuntimeClient::Json;
    using Time = std::chrono::steady_clock::time_point;
    using Clock = std::function<Time()>;
    using Sink = std::function<void(const std::string&)>;
    JointProgress(double timeout_seconds, Sink sink,
                  Clock clock = std::chrono::steady_clock::now);
    void observe(const Json& run_id, const Json& identity, const Json& status, bool flush = false);
  private:
    Sink sink_;
    Clock clock_;
    std::chrono::duration<double> interval_;
    std::optional<Time> emitted_at_;
    Json identity_, run_id_;
    std::array<uint64_t, 4> observed_{}, emitted_{};
};
}
#endif
