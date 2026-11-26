#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/workload/Workload.hh"
#include "astra-sim/workload/JointAccessIo.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <iostream>
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

bool JointSimulationCoordinator::enforce_node_idle_limit() const {
    // Supervised jobs report completed memory transactions to the host watchdog.
    // A single business node can contain arbitrarily many such transactions.
    return !awaiting_ && !supervised_progress_;
}

bool JointSimulationCoordinator::admits_events(uint64_t count) const {
    if (count > UINT64_MAX - events_) return false;
    return whole_job_ || events_ + count <= limit("events");
}

std::optional<uint64_t> JointSimulationCoordinator::graph_file_limit() const {
    return whole_job_ ? std::nullopt : std::optional<uint64_t>(256U << 20);
}

JointSimulationCoordinator::JointSimulationCoordinator(
        std::shared_ptr<NetworkAnalytical::EventQueue> queue, Json config, Exchange exchange)
    : queue_(std::move(queue)), config_(std::move(config)), exchange_(std::move(exchange)) {
    whole_job_ = AstraSim::JointAccessIo::whole_job_runtime(config_);
    for (const auto name : {"pending_nodes", "descriptor_bytes", "callbacks", "events", "same_time_events",
                            "no_progress_events", "horizon_fs", "supply_frames"}) limit(name);
    if (limit("pending_nodes") > 256 || limit("callbacks") > 65536 || limit("supply_frames") > 64)
        throw std::invalid_argument("joint runtime host admission limit");
    const auto engine = config_.value("memory_engine", std::string("split"));
    if (engine != "split" && engine != "in-process")
        throw std::invalid_argument("unknown joint memory engine");
    in_process_ = engine == "in-process";
    phase_timing_ = config_.value("measure_view_time", false);
    const auto mode = config_.value("cx_advance_mode", std::string("step"));
    if (mode != "step" && mode != "safe-before-v1") throw std::invalid_argument("invalid CX advance mode");
    const auto mw_mode = config_.value("mw_advance_mode", std::string("step"));
    if (mw_mode != "step" && mw_mode != "safe-before-v1") throw std::invalid_argument("invalid Akita advance mode");
    if (mw_mode == "safe-before-v1" || (whole_job_ && in_process_)) {
        const auto& value = config_.at("open").at("engine_budget").at("events_per_call");
        if (!value.is_number_unsigned() || value.get<uint64_t>() == 0)
            throw std::invalid_argument("invalid Akita per-call event budget");
        memory_events_per_call_ = value.get<uint64_t>();
        if (mw_mode == "safe-before-v1") mw_per_call_ = memory_events_per_call_;
    }
    awaiting_ = config_.contains("activation");
    if (config_.contains("successor")) {
        AstraSim::ServiceBindingJson::fields(config_.at("successor"), {"schema", "manifest_root"});
        if (!awaiting_ || config_["successor"]["schema"] != "simulator-successive-views-v1")
            throw std::invalid_argument("successor requires first activation and explicit capability");
    }
    if (awaiting_) {
        AstraSim::ServiceBindingJson::fields(config_.at("activation"), {"schema", "manifest_path"});
        if (config_.contains("source") || config_["activation"]["schema"] != "simulator-first-activation-v1")
            throw std::invalid_argument("invalid deferred activation configuration");
        status_ = {{"ready", false}, {"domains", Json::array()}, {"endpoints", Json::array()},
            {"completions", Json::array()}, {"admitted_nodes", "0"}, {"completed_nodes", "0"}};
    } else call("open");
}

void JointSimulationCoordinator::bind(std::shared_ptr<AstraSim::JointSourceContext> context,
        const std::vector<AstraSim::Workload*>& workloads) {
    if (service_ || !context || whole_job_ != context->limits().whole_job || context->identity().at("mode") != "external_source_service" ||
        context->identity().at("run_id") != config_.at("run_id"))
        throw std::invalid_argument("joint runtime requires its external Source context");
    for (const auto field : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (!awaiting_ && context->identity().at(field) != status_.at("source_context").at(field))
            throw std::invalid_argument("runtime and original ET Source identity differ");
    service_ = std::make_shared<Service>(context, config_.at("session_id").get<std::string>(),
        std::make_shared<ProviderForward>(shared_from_this()),
        Service::Limits{limit("pending_nodes"), limit("descriptor_bytes"), 4096});
    for (auto* workload : workloads) workload->bind_external_service(service_);
    context_ = context->identity();
    workloads_ = workloads;
}

JointSimulationCoordinator::Json JointSimulationCoordinator::activate(const Json& ref) {
    if (!awaiting_ || !activation_.is_null() || !service_ || !pending_.empty() ||
        ref.at("path") != config_.at("activation").at("manifest_path"))
        throw std::invalid_argument("activation requires the original unactivated service");
    activation_ = AstraSim::ServiceBindingJson::parse(JointRuntimeClient::original(ref, 1U << 20, true));
    if ((whole_job_ && activation_.at("schema") != "simulator-joint-activation-v2") ||
        (!whole_job_ && activation_.value("schema", std::string()) == "simulator-joint-activation-v2"))
        throw std::invalid_argument("activation host budget policy differs from Source context");
    const auto& facts = activation_.at("first_business");
    const auto& control = facts.at("control");
    const auto epoch = fs(queue_->get_current_time());
    if (epoch != frontier_fs_ || fs(JointRuntimeClient::decimal(facts.at("prepared_at_ns"))) != epoch ||
        control.at("kind") != "forward" || facts.at("model_sequence") != "0" || facts.at("model_rounds_before") != "0" ||
        JointRuntimeClient::decimal(control.at("iteration")) + 1 != JointRuntimeClient::decimal(facts.at("execution_iteration")))
        throw std::invalid_argument("activation differs from original forward safe point");
    for (const auto field : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (facts.at(field) != context_.at(field)) throw std::invalid_argument("activation Source context differs");
    if (control.at("acks").size() != workloads_.size() || activation_.at("graphs").size() != workloads_.size())
        throw std::invalid_argument("activation lacks complete original ranks");
    for (size_t i = 0; i < workloads_.size(); ++i) {
        const auto* workload = workloads_[i];
        const auto& ack = control.at("acks").at(i);
        const auto& graph = activation_.at("graphs").at(i);
        if (!workload->is_finished || !workload->pending_workloads.empty() ||
            JointRuntimeClient::decimal(ack.at("rank")) != static_cast<uint64_t>(workload->sys->id) ||
            graph.at("rank") != ack.at("rank") || ack.at("iteration") != control.at("iteration") ||
            workload->iteration != JointRuntimeClient::decimal(ack.at("iteration")) ||
            fs(JointRuntimeClient::decimal(ack.at("finish_ns"))) > epoch)
            throw std::invalid_argument("activation does not match the actual drained rank");
        for (const auto kind : {"et", "companion"}) JointRuntimeClient::original(graph.at(kind), graph_file_limit());
    }
    return start_service(ref, facts, epoch);
}

JointSimulationCoordinator::Json JointSimulationCoordinator::start_service(
        const Json& ref, const Json& facts, uint64_t epoch) {
    call("activate", {{"manifest", ref}, {"epoch_fs", std::to_string(epoch)}});
    for (const auto field : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (!context_.is_null() && status_.at("source_context").at(field) != context_.at(field))
            throw std::invalid_argument("activated factory Source context differs");
    while (!status_.at("ready").get<bool>()) advance();
    Json ready;
    enqueue_receive(JointRuntimeClient::decimal(status_.at("business_not_before_fs")), [&](Service::ReceiveEdge edge) {
        ready = {{"schema", "simulator-joint-activation-ready-v1"}, {"run_id", config_.at("run_id")},
            {"session_id", config_.at("session_id")}, {"manifest_sha256", ref.at("sha256")},
            {"execution_iteration", facts.at("execution_iteration")}, {"epoch_fs", std::to_string(epoch)},
            {"business_not_before_fs", status_.at("business_not_before_fs")}, {"anchor_ns", std::to_string(edge.visible_ns)}};
    });
    while (ready.is_null()) advance();
    awaiting_ = false;
    if (in_process_) {
        memory_controls_fs_.emplace();
        const auto origin = JointRuntimeClient::decimal(ready.at("business_not_before_fs"));
        for (const auto& row : config_.value("software_controls", Json::array())) {
            const auto offset = JointRuntimeClient::decimal(row.at("offset_fs"));
            if (offset >= limit("horizon_fs") - origin)
                throw std::invalid_argument("software control exceeds the global horizon");
            memory_controls_fs_->insert(origin + offset);
        }
    }
    view_identity_ = {{"session_id", config_.at("session_id")}, {"manifest_sha256", ref.at("sha256")},
        {"view_sequence", "0"}, {"execution_iteration", facts.at("execution_iteration")}};
    idle_events_ = 0;
    begin_view_timing();
    return ready;
}

JointSimulationCoordinator::Service::Admission JointSimulationCoordinator::try_submit(const Service::NodeRequest& r) {
    rethrow_failure();
    const Service::NodeKey key{r.identity.rank, r.identity.iteration, r.identity.node};
    if (awaiting_ || handoff_ || stopped_ || draining_ || !r.access || r.identity.run != config_.at("run_id") ||
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

void JointSimulationCoordinator::observe_progress(double timeout_seconds, JointProgress::Sink sink,
                                                  JointProgress::Clock clock) {
    if (progress_observer_ || !pending_.empty() ||
        JointRuntimeClient::decimal(status_.at("admitted_nodes")))
        throw std::invalid_argument("progress observer must bind once before business");
    progress_observer_ = std::make_unique<JointProgress>(timeout_seconds, std::move(sink), std::move(clock));
    if (whole_job_ && in_process_) {
        if (const char* deadline = std::getenv("CX_EXPERIMENT_DEADLINE_NS")) {
            const auto ns = JointRuntimeClient::decimal(Json(deadline));
            supervised_progress_ = ns > 0 && ns <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        }
    }
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
    if (progress_observer_ && !awaiting_ && !handoff_ && !view_identity_.is_null())
        progress_observer_->observe(config_.at("run_id"), view_identity_, status_);
}

JointSimulationCoordinator::Json JointSimulationCoordinator::call(const std::string& command, const Json& body,
        const std::function<void(const Json&)>& before_consume) {
    try {
        auto reply = exchange_(command, body);
        if (before_consume) before_consume(reply);
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
    if (awaiting_ || handoff_ || !status_.at("ready").get<bool>() || draining_) return;
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
        const auto id = domain.at("id").get<std::string>();
        if (status_.at("supply_idle").at(id).get<bool>() || (in_process_ && waiting_for_supply(id))) continue;
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
    Candidates candidates;
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
    const auto previous_tie = earliest == frontier_fs_ ? tie_ : std::string();
    auto chosen = candidates.end();
    for (auto it = candidates.begin(); it != candidates.end(); ++it)
        if (it->second.first == earliest && it->first > previous_tie) { chosen = it; break; }
    if (chosen == candidates.end()) {
        for (auto it = candidates.begin(); it != candidates.end(); ++it)
            if (it->second.first == earliest) { chosen = it; break; }
    }
    if (in_process_ && chosen->first != "0" && !awaiting_ && !handoff_ && !draining_ &&
        status_.at("ready").get<bool>() && status_.at("native_state") == "READY") {
        advance_memory(candidates, chosen->first, earliest);
        return;
    }
    if (!in_process_ && mw_per_call_ && chosen->first.rfind("1/", 0) == 0 &&
        advance_mw(candidates, chosen->first, earliest)) return;
    if (!in_process_ && advance_cx(candidates, chosen->first, earliest)) return;
    if (earliest != frontier_fs_) {
        if (earliest) closed_ns_ = (earliest - 1) / 1000000;
        same_time_ = 0;
    }
    frontier_fs_ = earliest;
    if (!admits_events(1) || same_time_ >= limit("same_time_events"))
        throw std::runtime_error("joint event budget exhausted");
    ++events_;
    ++same_time_;
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
    if (enforce_node_idle_limit() && idle_events_ > limit("no_progress_events")) throw std::runtime_error("joint no-business-progress budget exhausted");
}

void JointSimulationCoordinator::finish() {
    safe_point();
    observe_view_completion();
    if (phase_target_) throw std::runtime_error("joint view ended before its measured scope drained");
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
    if (whole_job_) std::clog << "joint-coordinator-events " << events_ << '\n';
    if (mw_per_call_) std::clog << "joint-mw-batch-fallbacks " << Json(mw_fallbacks_).dump() << '\n';
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
