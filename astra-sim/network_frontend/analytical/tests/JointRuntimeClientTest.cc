#include "common/JointRuntimeClient.hh"
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

using Client = AstraSimAnalytical::JointRuntimeClient;
namespace {
void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
template<class F> void rejects(F function) {
    try { function(); } catch (const std::exception&) { return; }
    throw std::runtime_error("expected runtime rejection");
}
template<class F> void rejects_with(F function, const char* expected) {
    try { function(); }
    catch (const std::exception& error) {
        require(std::string(error.what()) == expected, "wrong runtime failure boundary");
        return;
    }
    throw std::runtime_error("expected specific runtime rejection");
}
bool childless() {
    std::ifstream stream("/proc/self/task/" + std::to_string(getpid()) + "/children");
    std::string value;
    stream >> value;
    return value.empty();
}
bool file_budget_scenario(const std::string& path, const std::string& scenario) {
    if (scenario == "inherited-file-limit") {
        const rlimit bound{4096, 4096};
        require(setrlimit(RLIMIT_FSIZE, &bound) == 0, "test cannot lower its own file limit");
        rejects_with([&] { Client client(path); }, "worker file budget exceeds inherited hard limit");
        return true;
    }
    if (scenario == "invalid-file-limit" || scenario == "invalid-config") {
        rejects([&] { Client client(path); });
        return true;
    }
    if (scenario != "file-limit" && scenario != "stderr-budget" &&
        scenario != "stderr-close" && scenario != "close-orphan" &&
        scenario != "transcript-budget" && scenario != "transcript-close-budget") return false;
    Client client(path);
    if (scenario == "file-limit") {
        rejects([&] { client.exchange("open"); });
    } else if (scenario == "stderr-budget") {
        rejects_with([&] { client.exchange("open"); }, "runtime stderr budget exhausted");
        rejects_with([&] { client.exchange("open"); }, "runtime stderr budget exhausted");
        rejects_with([&] { client.close(); }, "runtime stderr budget exhausted");
    } else if (scenario == "transcript-budget") {
        rejects_with([&] {
            for (unsigned i = 0; i < 10; ++i) client.exchange("open");
        }, "runtime transcript budget exhausted");
        rejects_with([&] { client.exchange("open"); }, "runtime transcript budget exhausted");
    } else if (scenario == "transcript-close-budget") {
        client.exchange("open");
        rejects_with([&] { client.close(); }, "runtime transcript budget exhausted");
    } else {
        client.exchange("open");
        rejects_with([&] { client.close(); }, scenario == "stderr-close" ?
                     "runtime stderr budget exhausted" : "runtime nonzero exit or leaked child");
        if (scenario == "stderr-close") {
            rejects([&] { client.exchange("open"); });
            rejects_with([&] { client.close(); }, "runtime stderr budget exhausted");
        }
    }
    client.stop_owned();
    client.stop_owned();
    return true;
}
}

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    const std::string path = argv[1], scenario = argv[2];
    int original = 0;
    require(prctl(PR_GET_CHILD_SUBREAPER, &original) == 0, "cannot read original subreaper");
    if (scenario == "existing-child") {
        const pid_t child = fork();
        require(child >= 0, "test fork failed");
        if (!child) { pause(); _exit(0); }
        rejects([&] { Client client(path); });
        require(kill(child, 0) == 0, "runtime touched existing child");
        int current = -1;
        prctl(PR_GET_CHILD_SUBREAPER, &current);
        require(current == original, "rejected construction changed subreaper");
        kill(child, SIGTERM);
        int status;
        require(waitpid(child, &status, 0) == child, "test child was reaped by another owner");
    } else {
        const int wanted = scenario == "restore-one" ? 1 : 0;
        require(prctl(PR_SET_CHILD_SUBREAPER, wanted) == 0, "test cannot set subreaper");
        if (file_budget_scenario(path, scenario)) {
            require(childless(), "file budget failure leaked a child");
        } else if (scenario == "supervised-activation" || scenario == "limited-activation") {
            Client client(path);
            if (scenario == "supervised-activation") {
                require(client.exchange("activate").at("echo") == "activate", "cold activation failed");
                rejects_with([&] { client.exchange("status"); }, "runtime host IPC timeout");
            } else {
                rejects_with([&] { client.exchange("activate"); }, "runtime host IPC timeout");
            }
            client.stop_owned();
        } else if (scenario == "legacy-ipc-limit") {
            Client client(path);
            require(client.exchange("open").at("echo") == "open", "first permitted IPC failed");
            rejects_with([&] { client.close(); }, "runtime IPC budget or closed session");
        } else if (scenario == "normal" || scenario == "restore-one") {
            Client client(path);
            rejects([&] { Client second(path); });
            require(client.exchange("open").at("echo") == "open", "runtime reply not observed");
            client.close();
            rejects_with([&] { client.exchange("open"); }, "runtime IPC budget or closed session");
            client.close();
        } else {
            auto client = std::make_shared<Client>(path);
            const bool host_hup = scenario.rfind("host-hup", 0) == 0;
            if (scenario == "host-hup-ready") client->exchange("open");
            auto exchange = [client, scenario] {
                client->exchange(scenario == "host-hup-ready" ? "activate" : "open");
            };
            if (host_hup) {
                bool cancelled = false;
                try { exchange(); }
                catch (const std::runtime_error& error) {
                    cancelled = std::string(error.what()) == "activation host input closed";
                }
                require(cancelled, "held RPC did not observe actual host EOF");
                rejects([&] { client->exchange("abort"); });
            } else rejects(exchange);
            client->stop_owned();
            client->stop_owned();
            require(client.use_count() == 2 && childless(), "explicit stop relied on client destruction");
            int current = -1;
            require(prctl(PR_GET_CHILD_SUBREAPER, &current) == 0 && current == wanted,
                    "explicit stop did not restore subreaper while references remain");
        }
        require(childless(), "runtime leaked a direct or adopted child");
        int current = -1;
        prctl(PR_GET_CHILD_SUBREAPER, &current);
        require(current == wanted, "runtime failed to restore subreaper");
        require(prctl(PR_SET_CHILD_SUBREAPER, original) == 0, "test cannot restore original subreaper");
    }
    for (const auto value : {"01", "-1", "18446744073709551616", "1.2"})
        rejects([&] { Client::decimal(value); });
    rejects([] { Client::decimal(true); });
    std::cout << "joint-runtime-client PASS " << scenario << std::endl;
}
