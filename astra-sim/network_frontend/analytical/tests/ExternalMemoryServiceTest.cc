/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "ExternalMemoryFixture.hh"
#include <algorithm>
#include <limits>

using namespace ExternalMemoryTests;

void successor(const std::filesystem::path& root) {
    Fixture fixture(root);
    fixture.fire();
    fixture.drain_events();
    require(fixture.provider->accepted.size() == 2, "whole reads were not submitted");
    require(!fixture.work().is_finished, "dependent execution completed before external read");
    const auto& request = fixture.provider->accepted.front();
    require(request.operation == MemoryOperation::Read && request.access->binding->at("bytes") == "128",
            "provider did not receive the entire original MEM bill");
    require(request.access->companion->at("pieces").size() >= 2, "original access catalog was lost");
    require(request.access->binding->at("piece_refs").size() == 2, "discontinuous pieces were flattened");
    fixture.finish();
    require(fixture.provider->accepted.size() == 4, "reads/writes were dropped or duplicated");
    require(fixture.provider->accepted.back().operation == MemoryOperation::Write, "store was not external");
    std::size_t original_calls = 0;
    for (const auto& memory : fixture.memories) {
        original_calls += memory->nodes.size();
        for (const auto& external : fixture.provider->accepted)
            require(std::find(memory->nodes.begin(), memory->nodes.end(), external.identity.node) == memory->nodes.end(),
                    "selected node was charged to original memory as well");
    }
    require(original_calls > 0, "unassociated memory path was not exercised");
}

void rejection(const std::filesystem::path& root) {
    Fixture fixture(root);
    fixture.provider->behavior = [](const auto&) { return Bridge::Admission::Rejected; };
    fixture.reach_submit();
    const auto attempts = fixture.provider->attempts;
    fixture.drain_events();
    fixture.work().issue_dep_free_nodes();
    fixture.bridge->resume_ready(fixture.edge());
    require(fixture.provider->attempts == attempts, "unnotified rejection retried");
    require(fixture.provider->accepted.empty(), "rejected request was accepted");
    require(fixture.work().hw_resource->num_cpu_ops == 1 && fixture.work().hw_resource->num_in_flight_cpu_ops == 0,
            "independent actual compute callback was not exercised");
    fixture.provider->behavior = {};
    for (unsigned rank = 0; rank != 2; ++rank) {
        fixture.bridge->notify_capacity(fixture.provider->ingress(rank));
        fixture.bridge->notify_capacity(fixture.provider->ingress(rank));
    }
    fixture.bridge->resume_ready(fixture.edge());
    require(fixture.provider->attempts == attempts + 2, "duplicate capacity notifications duplicated admission");
    require(fixture.provider->observed[0] == fixture.provider->observed[attempts] &&
            fixture.provider->observed[1] == fixture.provider->observed[attempts + 1],
            "refusal/retry changed the original identity");
    fixture.finish();
}

void independent_ingress(const std::filesystem::path& root) {
    Fixture f(root);
    f.provider->behavior = [](const auto& request) {
        return request.identity.rank == 0 ? Bridge::Admission::Rejected : Bridge::Admission::Accepted;
    };
    f.fire(); f.drain_events();
    require(f.provider->accepted.size() == 1 && f.provider->accepted[0].identity.rank == 1,
            "one rejected ingress blocked another rank");
    f.complete(0); f.drain_events(); f.complete(1); f.drain_events();
    require(f.work(1).is_finished && !f.work(0).is_finished, "independent ingress did not finish");
    f.provider->behavior = {};
    f.bridge->notify_capacity(f.provider->ingress(0)); f.bridge->resume_ready(f.edge());
    f.finish(2);
}

void out_of_order(const std::filesystem::path& root) {
    Fixture f(root);
    f.fire(); f.drain_events();
    require(f.provider->accepted.size() == 2, "fixture requires concurrent ranks");
    const auto rank = f.provider->accepted[0].identity.rank;
    f.complete(1); f.drain_events();
    require(f.provider->accepted.size() == 3 && f.provider->accepted.back().identity.rank != rank,
            "out-of-order completion released the wrong successor");
    f.complete(0); f.finish(2);
}

void local_full(const std::filesystem::path& root, bool descriptor_only) {
    Fixture f(root, {descriptor_only ? 16u : 1u, descriptor_only ? 350u : 1048576u, 4096});
    f.fire(); f.drain_events();
    require(f.provider->accepted.size() == 1 && f.bridge->status().waiting_owners == 1,
            "configured local budget did not hold the other rank");
    f.complete(0);
    f.bridge->resume_ready(f.edge());
    require(f.provider->accepted.size() == 2, "local release did not wake another rank");
    f.finish(1);
}

void gate_release(const std::filesystem::path& root) {
    Fixture f(root, {16, 350, 4096});
    f.provider->behavior = [](const auto&) { return Bridge::Admission::Rejected; };
    f.reach_submit();
    require(f.provider->attempts == 1 && f.bridge->status().gates == 1 &&
            f.bridge->status().waiting_owners == 1, "fixture did not exhaust descriptors with a gate");
    require(f.work(0).hw_resource->num_in_flight_cpu_ops == 1,
            "original independent compute must keep the notified rank occupied");
    f.provider->behavior = {};
    f.bridge->notify_capacity(f.provider->ingress(0));
    f.bridge->resume_ready(f.edge());
    require(f.provider->accepted.size() == 1 && f.provider->accepted[0].identity.rank == 1,
            "gate descriptor release did not wake the other local-full rank");
    f.finish();
}

void idle_retire(const std::filesystem::path& root) {
    Fixture f(root);
    require(!f.bridge->status().accepted && !f.bridge->status().failed, "fixture must start idle");
    f.bridge->retire(f.work());
    require(!f.bridge->status().failed, "idle retirement unexpectedly failed");
    rejects([&] { f.bridge->bind(f.work()); }, "retired");
    require(f.provider->attempts == 0, "retired rank was readmitted");
}

void cpu_memory(const std::filesystem::path& root) {
    Fixture f(root);
    f.provider->behavior = [](const auto&) { return Bridge::Admission::Rejected; };
    f.fire(); f.drain_events();
    require(f.work().hw_resource->num_cpu_ops == 0 && f.work().hw_resource->num_in_flight_cpu_ops == 0,
            "rejected CPU MEM occupied hardware");
    f.provider->behavior = {};
    for (unsigned rank = 0; rank != 2; ++rank) f.bridge->notify_capacity(f.provider->ingress(rank));
    f.bridge->resume_ready(f.edge());
    require(f.work().hw_resource->num_cpu_ops == 1 && f.work().hw_resource->num_in_flight_cpu_ops == 1,
            "accepted CPU MEM did not occupy exactly once");
    f.finish();
    require(f.work().hw_resource->num_cpu_ops == 2 && f.work().hw_resource->num_in_flight_cpu_ops == 0,
            "CPU MEM completion did not release original hardware");
}

void invalid_completion(const std::filesystem::path& root, const std::string& defect) {
    Fixture f(root);
    f.fire(); f.drain_events();
    auto identity = f.provider->accepted[0].identity;
    auto fs = Sys::boostedTick() * 1000000;
    auto outcome = Bridge::Outcome::Success;
    auto edge = f.edge();
    if (defect == "wrong-run") identity.run = "foreign";
    else if (defect == "wrong-session") identity.session = "foreign";
    else if (defect == "wrong-rank") identity.rank = 98;
    else if (defect == "wrong-node") identity.node = 999999;
    else if (defect == "stale-iteration") ++identity.iteration;
    else if (defect == "partial") outcome = Bridge::Outcome::Partial;
    else if (defect == "error") outcome = Bridge::Outcome::Error;
    else if (defect == "future") ++edge.visible_ns;
    else if (defect == "early") fs = f.provider->accepted[0].submitted_ns * 1000000 - 1;
    else if (defect == "duplicate") f.complete(0);
    else throw std::runtime_error("invalid negative scenario");
    const auto accepted = f.bridge->status().accepted;
    rejects([&] { f.bridge->complete({identity, fs, outcome}, edge); });
    require(f.bridge->status().failed && f.bridge->status().accepted == accepted,
            "invalid completion consumed or completed pending work");
    const auto attempts = f.provider->attempts;
    rejects([&] { f.bridge->resume_ready(f.edge()); });
    rejects([&] { f.bridge->rethrow_failure(); });
    require(f.provider->attempts == attempts, "sticky error retried a write");
}

void edge_timing(const std::filesystem::path& root, const std::string& scenario) {
    Fixture f(root);
    f.fire(); f.drain_events();
    const auto now = Sys::boostedTick();
    if (scenario == "same-edge") {
        f.complete(0); f.complete(1);
    } else if (scenario == "ceil") {
        f.advance(now + 1);
        f.complete(0, now * 1000000 + 1);
        f.complete(1, (now + 1) * 1000000);
    } else if (scenario == "closed-edge") {
        f.advance(now + 1);
        f.complete(0, now * 1000000, now);
        // The owner must keep the monotonic closed-edge watermark on later calls.
        rejects([&] { f.complete(1); }, "receive edge");
        return;
    }
    f.finish(2);
}

struct HugeError final : std::exception {
    std::shared_ptr<std::string> payload;
    explicit HugeError(std::shared_ptr<std::string> value) : payload(std::move(value)) {}
    const char* what() const noexcept override { return payload->c_str(); }
};

void provider_failure(const std::filesystem::path& root, bool reentry) {
    Fixture f(root, {4, 1048576, 32});
    std::weak_ptr<std::string> released;
    f.provider->behavior = [&](const auto& request) {
        if (reentry) {
            // Even if the provider suppresses the inner error, acceptance stays unknown.
            rejects([&] { f.bridge->complete({request.identity, request.submitted_ns * 1000000,
                                             Bridge::Outcome::Success}, f.edge()); });
            return Bridge::Admission::Rejected;
        }
        auto payload = std::make_shared<std::string>(1024 * 1024, 'x');
        released = payload;
        throw HugeError(std::move(payload));
    };
    rejects([&] { f.reach_submit(); });
    require(f.bridge->status().acceptance_unknown == 1 && !f.bridge->status().accepted,
            "submit failure was not retained as unknown acceptance");
    require(released.expired(), "bridge retained the provider's exception payload");
    std::string first;
    try { f.bridge->rethrow_failure(); } catch (const std::exception& error) { first = error.what(); }
    require(first.size() < 160, "diagnostic escaped the explicit host budget");
    rejects([&] { f.bridge->notify_capacity(f.provider->ingress(0)); });
    try { f.bridge->rethrow_failure(); } catch (const std::exception& error) {
        require(first == error.what(), "later operation replaced the first failure");
    }
    auto summary = f.bridge->abort_local();
    require(summary.acceptance_unknown == 1 && summary.first_unknown.has_value(),
            "abort lost unknown identity");
}

void callback_submit(const std::filesystem::path& root) {
    Fixture f(root);
    f.fire(); f.drain_events();
    // The supplied zero-duration middle compute permits a successor store within call().
    const auto attempts = f.provider->attempts;
    f.complete(0);
    require(f.provider->attempts == attempts + 1, "normal callback successor submission was blocked");
    f.finish(1);
}

void callback_failure(const std::filesystem::path& root) {
    std::shared_ptr<JointSourceContext> retained_context;
    {
        Fixture f(root);
        retained_context = f.context;
        f.provider->behavior = [](const auto& request) {
            if (request.operation == MemoryOperation::Write)
                throw std::runtime_error("callback-store-injected");
            return Bridge::Admission::Accepted;
        };
        f.fire(); f.drain_events();
        const auto original = f.provider->accepted[0].identity;
        rejects([&] { f.complete(0); }, "callback-store-injected");
        const auto status = f.bridge->status();
        const Bridge::NodeKey read{original.rank, original.iteration, original.node};
        require(status.accepted == 1 && status.acceptance_unknown == 1 &&
                status.first_accepted != read && status.first_unknown != read,
                "callback failure did not release the read or preserve the uncertain store");
        require(!f.work().is_finished && !f.work().rank_completion_count,
                "failed successor fabricated rank completion");
        const auto attempts = f.provider->attempts;
        rejects([&] { f.bridge->resume_ready(f.edge()); }, "callback-store-injected");
        require(f.provider->attempts == attempts, "failed callback retried an uncertain write");
    }
    require(retained_context->live_charge_bytes() == 0,
            "callback failure retained an index after all owners retired");
}

void lease_lifetime(const std::filesystem::path& root) {
    Fixture f(root);
    f.fire(); f.drain_events();
    auto lease = f.provider->accepted[0].access;
    const auto* original_json = lease->companion;
    f.bridge->abort_local();
    f.provider->accepted.clear();
    f.systems.clear();
    require(f.context->live_charge_bytes() > 0 && lease->companion == original_json,
            "feeder retirement released or copied the leased index");
    require(lease->binding->at("bytes") == "128", "leased view became invalid");
    lease.reset();
    require(f.context->live_charge_bytes() == 0 && f.context->live_piece_count() == 0,
            "last lease did not release original context charge");
}

void stop_case(const std::filesystem::path& root, const std::string& scenario) {
    Fixture f(root);
    f.fire(); f.drain_events();
    const auto identity = f.provider->accepted[0].identity;
    if (scenario == "close-pending") {
        rejects([&] { f.bridge->close(); }, "not locally drained");
        require(f.bridge->status().accepted == 2, "close cleared pending to pretend drained");
    } else if (scenario == "retire") {
        f.bridge->retire(f.work());
        rejects([&] { f.bridge->bind(f.work()); });
        rejects([&] { f.complete(0); });
    } else {
        const auto summary = f.bridge->abort_local();
        require(summary.accepted == 2 && summary.first_accepted.has_value() && summary.stopped,
                "abort omitted accepted identity/count");
        require(!f.work().is_finished, "local abort fabricated successful rank completion");
        rejects([&] { f.bridge->complete({identity, Sys::boostedTick() * 1000000,
                                          Bridge::Outcome::Success}, f.edge()); });
    }
}

void limits_case(const std::filesystem::path& root) {
    const auto context = JointSourceContext::load((root / "joint-context.json").string());
    const auto provider = std::make_shared<Provider>();
    const auto maximum = std::numeric_limits<uint64_t>::max();
    for (const Bridge::Limits limits : {Bridge::Limits{0, 1048576, 4096}, {2, 0, 4096},
            {2, 1048576, 0}, {2, 1, 4096}, {maximum, 1048576, 4096}, {2, 1048576, maximum}})
        rejects([&] { Bridge invalid(context, "limits", provider, limits); });
    Bridge valid(context, "limits", provider, {4096, maximum, 4096});
    valid.close();  // Only the local bridge drains; no graph has run.
    const auto first = valid.abort_local();
    const auto again = valid.abort_local();
    require(first.stopped && again.stopped && valid.status().stopped &&
            !first.accepted && !first.acceptance_unknown && !first.failed && !valid.status().descriptor_bytes,
            "close then abort returned an inconsistent terminal status");
}

void time_overflow(const std::filesystem::path& root) {
    Fixture f(root);
    f.advance(std::numeric_limits<uint64_t>::max() / 1000000 + 1);
    rejects([&] { f.reach_submit(); }, "submission time");
    require(f.provider->attempts == 0, "overflow reached the external provider");
}

int main(int argc, char** argv) {
    LoggerFactory::init("empty");
    try {
        require(argc == 3, "expected fixture directory and scenario");
        const std::string scenario = argv[2];
        if (scenario == "successor") successor(argv[1]);
        else if (scenario == "rejection") rejection(argv[1]);
        else if (scenario == "out-of-order") out_of_order(argv[1]);
        else if (scenario == "independent-ingress") independent_ingress(argv[1]);
        else if (scenario == "gate-release") gate_release(argv[1]);
        else if (scenario == "idle-retire") idle_retire(argv[1]);
        else if (scenario == "local-full" || scenario == "descriptor-full") local_full(argv[1], scenario == "descriptor-full");
        else if (scenario == "cpu-memory") cpu_memory(argv[1]);
        else if (scenario == "same-edge" || scenario == "ceil" || scenario == "closed-edge") edge_timing(argv[1], scenario);
        else if (scenario == "throw" || scenario == "reentry") provider_failure(argv[1], scenario == "reentry");
        else if (scenario == "callback-submit") callback_submit(argv[1]);
        else if (scenario == "callback-failure") callback_failure(argv[1]);
        else if (scenario == "lease") lease_lifetime(argv[1]);
        else if (scenario == "abort" || scenario == "retire" || scenario == "close-pending") stop_case(argv[1], scenario);
        else if (scenario == "limits") limits_case(argv[1]);
        else if (scenario == "overflow") time_overflow(argv[1]);
        else if (scenario.rfind("wrong-", 0) == 0 || scenario == "stale-iteration" || scenario == "duplicate" ||
                 scenario == "partial" || scenario == "error" || scenario == "future" || scenario == "early")
            invalid_completion(argv[1], scenario);
        else throw std::runtime_error("unknown external-memory scenario");
        // Fixture destructors can enqueue logs. Join their worker before the terminal receipt.
        LoggerFactory::shutdown();
        std::cout << "external-memory-service PASS " << scenario << '\n';
        return 0;
    } catch (const std::exception& error) {
        LoggerFactory::shutdown();
        std::cerr << "external-memory-service FAIL " << error.what() << '\n';
        return 1;
    }
}
