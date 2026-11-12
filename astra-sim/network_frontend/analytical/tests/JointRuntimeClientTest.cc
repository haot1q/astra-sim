#include "common/JointRuntimeClient.hh"
#include <csignal>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

using Client = AstraSimAnalytical::JointRuntimeClient;
namespace {
void require(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
template<class F> void rejects(F function) {
    try { function(); } catch (const std::exception&) { return; }
    throw std::runtime_error("expected runtime rejection");
}
bool childless() {
    std::ifstream stream("/proc/self/task/" + std::to_string(getpid()) + "/children");
    std::string value;
    stream >> value;
    return value.empty();
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
        if (scenario == "normal" || scenario == "restore-one") {
            Client client(path);
            rejects([&] { Client second(path); });
            require(client.exchange("open").at("echo") == "open", "runtime reply not observed");
            client.close();
            client.close();
        } else {
            auto client = std::make_shared<Client>(path);
            auto exchange = [client] { client->exchange("open"); };
            rejects(exchange);
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
