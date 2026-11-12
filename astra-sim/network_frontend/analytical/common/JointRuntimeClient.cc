#include "common/JointRuntimeClient.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <openssl/evp.h>
#include "astra-sim/workload/JointAccessIo.hh"
#include <limits>
#include <memory>
#include <poll.h>
#include <sstream>
#include <stdexcept>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace AstraSimAnalytical {
bool JointRuntimeClient::active_ = false;

uint64_t JointRuntimeClient::decimal(const Json& value) {
    if (!value.is_string()) throw std::invalid_argument("runtime decimal must be a string");
    const auto s = value.get<std::string>();
    if (s.empty() || s.size() > 20 || (s.size() > 1 && s[0] == '0'))
        throw std::invalid_argument("invalid runtime decimal");
    uint64_t result = 0;
    for (const char c : s) {
        if (c < '0' || c > '9' || result > (UINT64_MAX - (c - '0')) / 10)
            throw std::invalid_argument("runtime decimal overflow");
        result = result * 10 + c - '0';
    }
    return result;
}

std::string JointRuntimeClient::original(const Json& ref, uint64_t cap, bool retain) {
    AstraSim::ServiceBindingJson::fields(ref, {"path", "size_bytes", "sha256"});
    const auto path = ref.at("path").get<std::string>();
    if (std::filesystem::canonical(path).string() != path || !std::filesystem::is_regular_file(path))
        throw std::invalid_argument("activation requires a canonical regular original");
    const auto before = AstraSim::JointAccessIo::file_identity(path);
    const auto expected = decimal(ref.at("size_bytes"));
    if (expected > cap || std::filesystem::file_size(path) != expected)
        throw std::invalid_argument("activation original byte limit");
    std::ifstream input(path, std::ios::binary);
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!hash || EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("activation SHA init failed");
    char buffer[65536];
    uint64_t count = 0;
    std::string contents;
    while (input.read(buffer, sizeof(buffer)) || input.gcount()) {
        count += input.gcount();
        if (count > cap || EVP_DigestUpdate(hash.get(), buffer, input.gcount()) != 1)
            throw std::runtime_error("activation original read exceeded budget");
        if (retain) contents.append(buffer, input.gcount());
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    if (!input.eof() || count != expected || before != AstraSim::JointAccessIo::file_identity(path) ||
        EVP_DigestFinal_ex(hash.get(), digest, &length) != 1 || length != 32)
        throw std::runtime_error("activation original changed or hash failed");
    std::ostringstream hex;
    hex << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < length; ++i) hex << std::setw(2) << static_cast<unsigned>(digest[i]);
    if (hex.str() != ref.at("sha256")) throw std::invalid_argument("activation original SHA differs");
    return contents;
}

std::vector<pid_t> JointRuntimeClient::children() {
    std::ifstream input("/proc/self/task/" + std::to_string(getpid()) + "/children");
    if (!input) throw std::runtime_error("cannot inspect owned child processes");
    std::vector<pid_t> result;
    pid_t pid;
    while (input >> pid) {
        if (pid <= 0 || result.size() == 256) throw std::runtime_error("child ownership budget");
        result.push_back(pid);
    }
    if (!input.eof()) throw std::runtime_error("invalid child identity inventory");
    return result;
}

JointRuntimeClient::JointRuntimeClient(const std::string& path) {
    if (active_ || !children().empty())
        throw std::invalid_argument("joint runtime requires the sole process owner");
    if (std::filesystem::file_size(path) > (1U << 20))
        throw std::invalid_argument("runtime configuration byte limit");
    std::ifstream input(path);
    std::string configuration((1U << 20) + 1, '\0');
    input.read(configuration.data(), configuration.size());
    configuration.resize(input.gcount());
    if (!input.eof() || configuration.size() > (1U << 20))
        throw std::invalid_argument("runtime configuration changed or exceeds limit");
    config_ = AstraSim::ServiceBindingJson::parse(configuration);
    if (config_.at("version") != "joint-runtime-v1")
        throw std::invalid_argument("unknown joint runtime version");
    frame_limit_ = decimal(config_.at("limits").at("frame_bytes"));
    const auto timeout = decimal(config_.at("limits").at("host_timeout_ms"));
    if (frame_limit_ < 1024 || frame_limit_ > (2U << 20) || !timeout || timeout > 600000)
        throw std::invalid_argument("invalid runtime frame/host budget");
    timeout_ms_ = static_cast<int>(timeout);
    log_limit_ = decimal(config_.at("limits").value("log_bytes", Json("134217728")));
    if (!log_limit_ || log_limit_ > (128U << 20)) throw std::invalid_argument("invalid runtime log budget");
    if (prctl(PR_GET_CHILD_SUBREAPER, &old_subreaper_) != 0)
        throw std::runtime_error("cannot read subreaper state");
    if (prctl(PR_SET_CHILD_SUBREAPER, 1) != 0)
        throw std::runtime_error("cannot acquire child ownership");
    active_ = owned_ = true;
    try { start(path); }
    catch (...) {
        try { stop_owned(); }
        catch (const std::exception& error) { std::fprintf(stderr, "joint startup cleanup failed: %s\n", error.what()); }
        catch (...) { std::fprintf(stderr, "joint startup cleanup failed with an unknown exception\n"); }
        throw;
    }
}

void JointRuntimeClient::start(const std::string& path) {
    auto args = config_.at("worker_argv").get<std::vector<std::string>>();
    if (args.empty() || args.size() > 32 || !std::filesystem::path(args[0]).is_absolute())
        throw std::invalid_argument("runtime requires explicit executable argv");
    for (const auto& arg : args)
        if (arg.empty() || arg.size() > 4096 || arg.find('\0') != std::string::npos)
            throw std::invalid_argument("invalid runtime argv");
    args.insert(args.end(), {"--config", path});
    const std::filesystem::path output = config_.at("output").get<std::string>();
    if (!std::filesystem::create_directory(output))
        throw std::invalid_argument("runtime output must be fresh");
    transcript_.open(output / "runtime.jsonl");
    if (!transcript_) throw std::runtime_error("cannot create runtime transcript");
    int to_child[2], from_child[2];
    if (pipe2(to_child, O_CLOEXEC) != 0) throw std::runtime_error("runtime input pipe failed");
    if (pipe2(from_child, O_CLOEXEC) != 0) {
        ::close(to_child[0]); ::close(to_child[1]);
        throw std::runtime_error("runtime output pipe failed");
    }
    stderr_path_ = (output / "facade.stderr").string();
    const int error = ::open(stderr_path_.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (error < 0) {
        ::close(to_child[0]); ::close(to_child[1]);
        ::close(from_child[0]); ::close(from_child[1]);
        throw std::runtime_error("cannot create runtime stderr");
    }
    std::vector<char*> argv;
    for (auto& arg : args) argv.push_back(arg.data());
    argv.push_back(nullptr);
    child_ = fork();
    if (child_ == 0) {
        const rlimit file_limit{log_limit_, log_limit_};
        if (setrlimit(RLIMIT_FSIZE, &file_limit) != 0) _exit(126);
        if (setsid() < 0 || dup2(to_child[0], 0) < 0 || dup2(from_child[1], 1) < 0 || dup2(error, 2) < 0)
            _exit(126);
        execv(argv[0], argv.data());
        _exit(127);
    }
    ::close(to_child[0]); ::close(from_child[1]); ::close(error);
    input_ = to_child[1]; output_ = from_child[0];
    if (child_ < 0) throw std::runtime_error("runtime fork failed");
    if (fcntl(input_, F_SETFL, O_NONBLOCK) < 0 || fcntl(output_, F_SETFL, O_NONBLOCK) < 0)
        throw std::runtime_error("cannot make runtime pipes nonblocking");
}

std::string JointRuntimeClient::transact(const std::string& frame) {
    using Clock = std::chrono::steady_clock;
    const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms_);
    size_t sent = 0;
    sigset_t blocked, previous;
    sigemptyset(&blocked); sigaddset(&blocked, SIGPIPE);
    if (sigprocmask(SIG_BLOCK, &blocked, &previous) != 0) throw std::runtime_error("SIGPIPE guard failed");
    struct Restore {
        sigset_t previous, blocked;
        ~Restore() { timespec zero{}; sigtimedwait(&blocked, nullptr, &zero); sigprocmask(SIG_SETMASK, &previous, nullptr); }
    } restore{previous, blocked};
    while (sent < frame.size() || pending_.find('\n') == std::string::npos) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (left <= 0) throw std::runtime_error("runtime host IPC timeout");
        if (std::filesystem::file_size(stderr_path_) >= log_limit_)
            throw std::runtime_error("runtime stderr budget exhausted");
        // Cancellation observes HUP only: the original workload reader owns stdin bytes.
        pollfd fds[3]{{output_, POLLIN, 0}, {sent < frame.size() ? input_ : -1, POLLOUT, 0},
                      {config_.contains("activation") ? STDIN_FILENO : -1, 0, 0}};
        if (poll(fds, 3, static_cast<int>(std::min<int64_t>(left, 100))) < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("runtime poll failed");
        }
        if (fds[2].revents & (POLLHUP | POLLERR | POLLNVAL))
            throw std::runtime_error("activation host input closed");
        if (fds[1].revents & POLLOUT) {
            const auto count = write(input_, frame.data() + sent, frame.size() - sent);
            if (count > 0) sent += count;
            else if (errno != EAGAIN && errno != EINTR) throw std::runtime_error("runtime write failed");
        }
        if (fds[0].revents & (POLLIN | POLLHUP)) {
            char buffer[4096];
            const auto count = read(output_, buffer, sizeof(buffer));
            if (!count) throw std::runtime_error("runtime unexpected EOF");
            if (count > 0) pending_.append(buffer, count);
            else if (errno != EAGAIN && errno != EINTR) throw std::runtime_error("runtime read failed");
            if (pending_.size() > frame_limit_) throw std::runtime_error("runtime response byte limit");
        }
        if (sent < frame.size() && (fds[1].revents & (POLLERR | POLLHUP)))
            throw std::runtime_error("runtime input closed before complete command");
    }
    const auto end = pending_.find('\n');
    const auto result = pending_.substr(0, end);
    pending_.erase(0, end + 1);
    if (!pending_.empty()) throw std::runtime_error("unsolicited runtime reply");
    return result;
}

JointRuntimeClient::Json JointRuntimeClient::exchange(const std::string& command, const Json& body) {
    if (terminal_ || ++calls_ > decimal(config_.at("limits").at("ipc_calls")))
        throw std::runtime_error("runtime IPC budget or closed session");
    Json request{{"version", "joint-runtime-v1"}, {"run_id", config_.at("run_id")},
        {"session_id", config_.at("session_id")}, {"seq", std::to_string(sequence_)},
        {"command", command}, {"body", body}};
    const auto frame = request.dump() + "\n";
    if (frame.size() > frame_limit_) throw std::runtime_error("runtime command byte limit");
    const auto reply = AstraSim::ServiceBindingJson::parse(transact(frame));
    const auto record = Json{{"command", request}, {"reply", reply}}.dump() + "\n";
    transcript_bytes_ += record.size();
    if (transcript_bytes_ > log_limit_) throw std::runtime_error("runtime transcript budget exhausted");
    transcript_ << record << std::flush;
    if (!transcript_) throw std::runtime_error("runtime transcript write failed");
    if (reply.size() != 5 || reply.at("version") != request["version"] ||
        reply.at("run_id") != request["run_id"] || reply.at("session_id") != request["session_id"] ||
        decimal(reply.at("seq")) != sequence_ || !reply.at("body").is_object())
        throw std::runtime_error("runtime response identity mismatch");
    ++sequence_;
    terminal_ = command == "close" || command == "abort";
    return reply.at("body");
}

void JointRuntimeClient::stop_owned() {
    terminal_ = true;
    cleanup();
}

void JointRuntimeClient::cleanup() {
    if (!owned_) return;
    if (input_ >= 0) { ::close(input_); input_ = -1; }
    if (output_ >= 0) { ::close(output_); output_ = -1; }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    for (size_t round = 0; round < 300; ++round) {
        const auto current = children();
        if (current.empty()) {
            if (prctl(PR_SET_CHILD_SUBREAPER, old_subreaper_) != 0)
                throw std::runtime_error("cannot restore subreaper state");
            active_ = owned_ = false;
            return;
        }
        for (const auto pid : current) {
            // An unreaped direct child cannot have its PID reused. Only this owner waits.
            if (kill(pid, round > 20 ? SIGKILL : SIGTERM) != 0 && errno != ESRCH)
                throw std::runtime_error("cannot stop owned runtime child");
            int status = 0;
            if (waitpid(pid, &status, WNOHANG) < 0 && errno != EINTR)
                throw std::runtime_error("owned child identity was reaped elsewhere");
        }
        if (std::chrono::steady_clock::now() >= deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    throw std::runtime_error("owned runtime children did not terminate");
}

void JointRuntimeClient::close() {
    if (!owned_) return;
    if (!terminal_) exchange("close");
    int status = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms_);
    while (true) {
        if (std::filesystem::file_size(stderr_path_) >= log_limit_)
            throw std::runtime_error("runtime stderr budget exhausted at exit");
        const auto waited = waitpid(child_, &status, WNOHANG);
        if (waited == child_) break;
        if (waited < 0 && errno != EINTR) throw std::runtime_error("runtime exit ownership lost");
        if (std::chrono::steady_clock::now() >= deadline) throw std::runtime_error("runtime exit timeout");
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    child_ = -1;
    const bool clean = WIFEXITED(status) && WEXITSTATUS(status) == 0 && children().empty();
    cleanup();
    if (!clean) throw std::runtime_error("runtime nonzero exit or leaked child");
}

JointRuntimeClient::~JointRuntimeClient() {
    try { cleanup(); }
    catch (const std::exception& e) { std::fprintf(stderr, "joint runtime cleanup failed: %s\n", e.what()); }
}
}
