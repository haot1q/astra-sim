#ifndef ASTRA_ANALYTICAL_JOINT_RUNTIME_CLIENT_HH
#define ASTRA_ANALYTICAL_JOINT_RUNTIME_CLIENT_HH

#include "extern/helper/json/json.hpp"
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>
#include <sys/types.h>

namespace AstraSimAnalytical {

// Linux frontend-only owner: no other child-spawning/reaping thread may coexist.
class JointRuntimeClient {
  public:
    using Json = nlohmann::json;
    explicit JointRuntimeClient(const std::string& config_path);
    ~JointRuntimeClient();
    JointRuntimeClient(const JointRuntimeClient&) = delete;
    JointRuntimeClient& operator=(const JointRuntimeClient&) = delete;
    Json exchange(const std::string& command, const Json& body = Json::object());
    void close();
    void stop_owned();
    const Json& config() const noexcept { return config_; }
    static uint64_t decimal(const Json& value);
  private:
    Json config_;
    pid_t child_ = -1;
    int input_ = -1, output_ = -1, old_subreaper_ = 0;
    uint64_t sequence_ = 0, calls_ = 0;
    size_t frame_limit_ = 0;
    int timeout_ms_ = 0;
    bool owned_ = false, terminal_ = false;
    std::string pending_;
    std::ofstream transcript_;
    uint64_t transcript_bytes_ = 0;
    uint64_t log_limit_ = 128U << 20;
    std::string stderr_path_;
    static bool active_;
    static std::vector<pid_t> children();
    void start(const std::string& path);
    std::string transact(const std::string& frame);
    void cleanup();
};
}
#endif
