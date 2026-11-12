#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/workload/Workload.hh"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace AstraSimAnalytical {
using NetworkAnalytical::EventPeekStatus;
using NetworkAnalytical::EventStepStatus;
namespace {
struct ProviderForward final : JointSimulationCoordinator::Service::Provider {
    std::weak_ptr<JointSimulationCoordinator> owner;
    explicit ProviderForward(std::weak_ptr<JointSimulationCoordinator> value) : owner(std::move(value)) {}
    JointSimulationCoordinator::Service::Admission try_submit(
            const JointSimulationCoordinator::Service::NodeRequest& request) override {
        auto current = owner.lock();
        if (!current) throw std::runtime_error("joint runtime owner has retired");
        return current->try_submit(request);
    }
};
}

uint64_t JointSimulationCoordinator::fs(uint64_t ns) {
    if (ns > UINT64_MAX / 1000000) throw std::overflow_error("ASTRA ns to fs overflow");
    return ns * 1000000;
}

uint64_t JointSimulationCoordinator::limit(const char* name) const {
    const auto value = JointRuntimeClient::decimal(config_.at("limits").at(name));
    if (!value) throw std::invalid_argument("zero joint runtime budget");
    return value;
}

JointSimulationCoordinator::JointSimulationCoordinator(
        std::shared_ptr<NetworkAnalytical::EventQueue> queue, Json config, Exchange exchange)
    : queue_(std::move(queue)), config_(std::move(config)), exchange_(std::move(exchange)) {
    for (const auto name : {"pending_nodes", "descriptor_bytes", "callbacks", "events", "same_time_events",
                            "no_progress_events", "horizon_fs", "supply_frames"}) limit(name);
    if (limit("pending_nodes") > 256 || limit("callbacks") > 65536 || limit("supply_frames") > 64)
        throw std::invalid_argument("joint runtime host admission limit");
    call("open");
}

void JointSimulationCoordinator::bind(std::shared_ptr<AstraSim::JointSourceContext> context,
        const std::vector<AstraSim::Workload*>& workloads) {
    if (service_ || !context || context->identity().at("mode") != "external_source_service" ||
        context->identity().at("run_id") != config_.at("run_id"))
        throw std::invalid_argument("joint runtime requires its external Source context");
    for (const auto field : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (context->identity().at(field) != status_.at("source_context").at(field))
            throw std::invalid_argument("runtime and original ET Source identity differ");
    service_ = std::make_shared<Service>(context, config_.at("session_id").get<std::string>(),
        std::make_shared<ProviderForward>(shared_from_this()),
        Service::Limits{limit("pending_nodes"), limit("descriptor_bytes"), 4096});
    for (auto* workload : workloads) workload->bind_external_service(service_);
}

JointSimulationCoordinator::Service::Admission JointSimulationCoordinator::try_submit(const Service::NodeRequest& r) {
    rethrow_failure();
    const Service::NodeKey key{r.identity.rank, r.identity.iteration, r.identity.node};
    if (stopped_ || draining_ || !r.access || r.identity.run != config_.at("run_id") ||
        r.identity.session != config_.at("session_id") || pending_.count(key))
        throw std::invalid_argument("invalid or repeated original node promise");
    if (pending_.size() >= limit("pending_nodes") ||
        (pending_.size() + 1) * (sizeof(Service::NodeRequest) + 512) > limit("descriptor_bytes")) {
        if (rejected_.size() >= limit("callbacks") && !rejected_.count(r.admission_key))
            throw std::runtime_error("provider rejected-key budget exhausted");
        rejected_.insert(r.admission_key);
        return Service::Admission::Rejected;
    }
    fs(r.submitted_ns);
    pending_.emplace(key, r);
    return Service::Admission::Accepted;
}

void JointSimulationCoordinator::dispatch(void* argument) noexcept {
    auto* raw = static_cast<Callback*>(argument);
    auto* owner = raw->owner;
    const auto found = owner->callbacks_.find(raw->id);
    auto function = std::move(found->second->function);
    owner->callbacks_.erase(found);
    if (owner->stopped_) return;
    try { function(); }
    catch (const std::exception& error) { if (owner->failure_.empty()) owner->failure_ = std::string(error.what()).substr(0, 4096); }
    catch (...) { if (owner->failure_.empty()) owner->failure_ = "unknown joint callback failure"; }
}

void JointSimulationCoordinator::schedule(uint64_t when_ns, std::function<void()> callback) {
    if (callbacks_.size() >= limit("callbacks") || fs(when_ns) >= limit("horizon_fs") ||
        when_ns < queue_->get_current_time()) throw std::runtime_error("illegal or unbounded ASTRA callback");
    const auto id = serial_++;
    auto record = std::make_unique<Callback>(Callback{this, id, std::move(callback)});
    auto* raw = record.get();
    callbacks_.emplace(id, std::move(record));
    queue_->schedule_event(when_ns, dispatch, raw);
}

void JointSimulationCoordinator::enqueue_receive(uint64_t completed_fs,
        std::function<void(Service::ReceiveEdge)> callback) {
    if (completed_fs > frontier_fs_ || completed_fs >= limit("horizon_fs"))
        throw std::runtime_error("external completion exceeds the authorized frontier");
    uint64_t visible = completed_fs / 1000000 + (completed_fs % 1000000 != 0);
    if (closed_ns_) {
        if (*closed_ns_ == UINT64_MAX) throw std::overflow_error("closed ASTRA edge overflow");
        visible = std::max(visible, *closed_ns_ + 1);
    }
    const Service::ReceiveEdge edge{visible, closed_ns_};
    schedule(visible, [this, edge, callback = std::move(callback)] {
        if (queue_->get_current_time() != edge.visible_ns)
            throw std::runtime_error("receive callback did not execute at its original ASTRA edge");
        callback(edge);
    });
}

void JointSimulationCoordinator::observe_receives(ReceiveObserver observer) {
    if (!observer || receive_observer_ || !pending_.empty() ||
        JointRuntimeClient::decimal(status_.at("admitted_nodes")))
        throw std::invalid_argument("receive observer must bind once before business");
    receive_observer_ = std::move(observer);
}

void JointSimulationCoordinator::consume(Json reply) {
    if (!reply.at("ready").is_boolean() || !reply.at("completions").is_array())
        throw std::invalid_argument("malformed runtime state");
    if (reply["completions"].size() > limit("pending_nodes"))
        throw std::invalid_argument("runtime completion frame exceeds admission");
    for (const auto& row : reply["completions"]) {
        const auto rank = JointRuntimeClient::decimal(row.at("rank"));
        const auto iteration = JointRuntimeClient::decimal(row.at("iteration"));
        const auto node = JointRuntimeClient::decimal(row.at("node"));
        if (rank > UINT32_MAX || iteration > UINT32_MAX) throw std::invalid_argument("node domain overflow");
        const Service::NodeKey key{rank, iteration, node};
        const auto found = pending_.find(key);
        if (found == pending_.end() || !sent_.count(key) || !receiving_.insert(key).second)
            throw std::invalid_argument("unknown, stale, or duplicate whole-node completion");
        const auto request = found->second;
        const auto completed = JointRuntimeClient::decimal(row.at("completed_fs"));
        if (completed < fs(request.submitted_ns)) throw std::invalid_argument("completion precedes node release");
        enqueue_receive(completed, [this, key, request, completed](Service::ReceiveEdge edge) {
            pending_.erase(key);
            sent_.erase(key);
            receiving_.erase(key);
            service_->complete({request.identity, completed, Service::Outcome::Success}, edge);
            if (receive_observer_) receive_observer_(request.identity, completed, edge, queue_->get_current_time());
            const auto rejected = std::move(rejected_);
            rejected_.clear();
            for (const auto& ingress : rejected) service_->notify_capacity(ingress);
            service_->resume_ready(edge);
        });
    }
    status_ = std::move(reply);
}

JointSimulationCoordinator::Json JointSimulationCoordinator::call(const std::string& command, const Json& body) {
    try {
        auto reply = exchange_(command, body);
        consume(reply);
        while (status_.value("more_pending", false)) {
            if (++same_time_ > limit("same_time_events")) throw std::runtime_error("joint reply continuation budget exhausted");
            consume(exchange_("status", Json::object()));
        }
        return reply;
    } catch (const std::exception& error) {
        if (failure_.empty()) failure_ = std::string(error.what()).substr(0, 4096);
        throw;
    }
}

void JointSimulationCoordinator::safe_point() {
    rethrow_failure();
    if (stopped_) throw std::runtime_error("joint runtime has stopped");
    if (!status_.at("ready").get<bool>() || draining_) return;
    Json nodes = Json::array();
    for (const auto& [key, r] : pending_) {
        if (sent_.count(key)) continue;
        nodes.push_back({{"rank", std::to_string(r.identity.rank)}, {"iteration", std::to_string(r.identity.iteration)},
            {"node", std::to_string(r.identity.node)}, {"release_fs", std::to_string(fs(r.submitted_ns))},
            {"operation", r.operation == AstraSim::MemoryOperation::Read ? "read" : "write"},
            {"admission_key", {std::to_string(std::get<0>(r.admission_key)),
                std::to_string(std::get<1>(r.admission_key)), std::to_string(std::get<2>(r.admission_key))}}});
    }
    if (!nodes.empty()) {
        // Reserve sent identities before processing a reply; the provider promise cannot be rolled back.
        for (const auto& [key, _] : pending_) sent_.insert(key);
        const auto reply = call("admit_nodes", {{"nodes", nodes}});
        if (JointRuntimeClient::decimal(reply.at("accepted_prefix")) != nodes.size())
            throw std::runtime_error("runtime rejected a reserved node promise");
    }
    const auto arrival = std::max(frontier_fs_, JointRuntimeClient::decimal(status_.at("business_not_before_fs")));
    const auto domains = status_.at("domains");
    for (const auto& domain : domains) {
        if (status_.at("supply_idle").at(domain.at("id").get<std::string>()).get<bool>()) continue;
        std::string reason;
        do {
            if (++same_time_ > limit("same_time_events")) throw std::runtime_error("joint supply exchange budget exhausted");
            reason = call("supply", {{"domain_id", domain.at("id")}, {"arrival_fs", std::to_string(arrival)},
                {"frames", std::to_string(limit("supply_frames"))}}).at("supply_reason").get<std::string>();
            if (reason != "quota" && reason != "idle" && reason != "blocked")
                throw std::runtime_error("unknown Source supply stopping reason");
        } while (reason == "quota");
    }
    rethrow_failure();
}

void JointSimulationCoordinator::deadline(uint32_t endpoint, uint64_t when_ns, uint64_t generation) {
    if (when_ns <= queue_->get_current_time()) return;
    const auto value = std::make_pair(when_ns, generation);
    if (deadlines_.count(endpoint) && deadlines_.at(endpoint) == value) return;
    deadlines_[endpoint] = value;
    schedule(when_ns, [this, endpoint, value] {
        const auto found = deadlines_.find(endpoint);
        if (found != deadlines_.end() && found->second == value) deadlines_.erase(found);
    });
}

void JointSimulationCoordinator::advance() {
    safe_point();
    std::map<std::string, std::pair<uint64_t, Json>> candidates;
    const auto next = queue_->peek_next_time();
    if (next.status == EventPeekStatus::Ready) candidates["0"] = {fs(next.time), Json()};
    else if (next.status != EventPeekStatus::Empty) throw std::runtime_error("ASTRA queue is not at a safe point");
    for (const auto& row : status_.at("domains"))
        if (!row.at("next_fs").is_null()) candidates["1/" + row.at("id").get<std::string>()] =
            {JointRuntimeClient::decimal(row["next_fs"]), {{"engine", "mw"}, {"id", row["id"]}, {"time_fs", row["next_fs"]}}};
    for (const auto& row : status_.at("endpoints"))
        if (!row.at("next_fs").is_null() && row["state"] != "DRAINED" && row["state"] != "CLOSED")
            candidates["2/" + row.at("id").get<std::string>()] = {JointRuntimeClient::decimal(row["next_fs"]),
                {{"engine", "cx"}, {"id", row["id"]}, {"time_fs", row["next_fs"]}}};
    if (candidates.empty()) throw std::runtime_error("unfinished joint run has no authorized next event");
    const auto earliest = std::min_element(candidates.begin(), candidates.end(),
        [](const auto& a, const auto& b) { return a.second.first < b.second.first; })->second.first;
    if (earliest < frontier_fs_ || earliest >= limit("horizon_fs"))
        throw std::runtime_error("joint time regression or horizon exceeded");
    if (earliest != frontier_fs_) {
        if (earliest) closed_ns_ = (earliest - 1) / 1000000;
        tie_.clear(); same_time_ = 0;
    }
    frontier_fs_ = earliest;
    if (++events_ > limit("events") || ++same_time_ > limit("same_time_events"))
        throw std::runtime_error("joint event budget exhausted");
    auto chosen = candidates.end();
    for (auto it = candidates.begin(); it != candidates.end(); ++it)
        if (it->second.first == earliest && it->first > tie_) { chosen = it; break; }
    if (chosen == candidates.end()) {
        tie_.clear();
        for (auto it = candidates.begin(); it != candidates.end(); ++it)
            if (it->second.first == earliest) { chosen = it; break; }
    }
    tie_ = chosen->first;
    if (tie_ == "0") {
        if (queue_->step_at(earliest / 1000000) != EventStepStatus::Executed)
            throw std::runtime_error("ASTRA exact callback step failed");
    } else {
        if (config_.contains("hold") && tie_ == "2/" + config_["hold"].at("endpoint").get<std::string>() && status_["ready"] == true) {
            const auto release = JointRuntimeClient::decimal(config_["hold"].at("release_fs"));
            if (!hold_set_ || (!hold_released_ && earliest >= release)) {
                if (!hold_released_ && earliest > release && hold_set_)
                    throw std::runtime_error("hold release was not an original CX edge");
                hold_released_ = earliest >= release;
                call("cx_control", {{"id", config_["hold"]["endpoint"]},
                    {"values", {{"hold_rsp", hold_released_ ? 0 : config_["hold"].at("value").get<int>()}}}});
                hold_set_ = true;
            }
        }
        call("step_at", chosen->second.second);
    }
    rethrow_failure();
    const auto progress = JointRuntimeClient::decimal(status_.at("completed_nodes")) +
        JointRuntimeClient::decimal(status_.at("admitted_nodes"));
    idle_events_ = progress == progress_ ? idle_events_ + 1 : 0;
    progress_ = progress;
    if (idle_events_ > limit("no_progress_events")) throw std::runtime_error("joint no-business-progress budget exhausted");
}

void JointSimulationCoordinator::finish() {
    safe_point();
    if (!pending_.empty() || !callbacks_.empty() || !queue_->finished())
        throw std::runtime_error("ASTRA promises or callbacks remain at joint close");
    if (service_) service_->close();
    draining_ = true;
    call("drain");
    const auto drained = [this] {
        return status_.at("native_state") == "COMPLETE" && std::all_of(
            status_.at("endpoints").begin(), status_.at("endpoints").end(),
            [](const auto& endpoint) { return endpoint.at("state") == "DRAINED"; });
    };
    while (!drained()) advance();
    call("close");
    stopped_ = true;
}

void JointSimulationCoordinator::abort() {
    if (stopped_) return;
    stopped_ = true;
    if (service_) service_->abort_local();
    exchange_("abort", Json::object());
}

void JointSimulationCoordinator::rethrow_failure() const {
    if (!failure_.empty()) throw std::runtime_error(failure_);
    if (service_) service_->rethrow_failure();
}
}
