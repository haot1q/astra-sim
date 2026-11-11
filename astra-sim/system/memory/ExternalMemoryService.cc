/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "astra-sim/system/memory/ExternalMemoryService.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/workload/Workload.hh"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

namespace AstraSim {
bool ExternalMemoryService::Identity::operator==(const Identity& other) const {
    return std::tie(run, session, rank, iteration, node) ==
           std::tie(other.run, other.session, other.rank, other.iteration, other.node);
}

ExternalMemoryService::NodeKey ExternalMemoryService::key(const Identity& id) {
    return {id.rank, id.iteration, id.node};
}

ExternalMemoryService::ExternalMemoryService(std::shared_ptr<JointSourceContext> context,
    std::string session, std::shared_ptr<Provider> provider, Limits limits)
    : context_(std::move(context)), provider_(std::move(provider)), session_(std::move(session)), limits_(limits) {
    if (!context_ || !provider_ || context_->identity().at("mode") != "external_source_service" ||
        session_.empty() || session_.size() > context_->limits().string_bytes ||
        !limits.pending_nodes || limits.pending_nodes > pending_.max_size() ||
        !limits.descriptor_bytes || limits.descriptor_bytes > std::numeric_limits<std::size_t>::max() ||
        !limits.diagnostic_bytes || limits.diagnostic_bytes > diagnostic_.max_size()) {
        throw std::invalid_argument("external service: invalid owner or host limits");
    }
    run_ = context_->identity().at("run_id").get<std::string>();
    constexpr uint64_t owner_charge = sizeof(Owners::value_type) + 3 * sizeof(void*);
    if (context_->rank_count() > limits.descriptor_bytes / owner_charge)
        throw std::invalid_argument("external service: rank descriptor budget");
    charge_ = context_->rank_count() * owner_charge;
    // Fixed-size host accounting includes identity text; never copies the leased JSON.
    pending_charge_ = sizeof(PendingTable::value_type) + 3 * sizeof(void*) + run_.size() + session_.size();
    if (pending_charge_ > limits.descriptor_bytes - charge_ ||
        gate_charge_ > limits.descriptor_bytes - charge_ - pending_charge_)
        throw std::invalid_argument("external service: single descriptor cannot fit");
    for (const auto& rank : context_->identity().at("ranks"))
        owners_.emplace(static_cast<uint32_t>(std::stoul(rank.get<std::string>())), Owner{});
}

void ExternalMemoryService::remember(Failure code, const char* message, std::optional<NodeKey> node) {
    if (failure_ != Failure::None) return;
    failure_ = code;
    failed_node_ = node;
    std::size_t size = 0;
    while (size < limits_.diagnostic_bytes && message[size]) ++size;
    diagnostic_.assign(message, size);
    if (size == limits_.diagnostic_bytes) diagnostic_.back() = '~';
}

void ExternalMemoryService::rethrow_failure() const {
    if (failure_ == Failure::None) return;
    std::string identity;
    if (failed_node_) identity = " rank=" + std::to_string(std::get<0>(*failed_node_)) +
        " iteration=" + std::to_string(std::get<1>(*failed_node_)) + " node=" + std::to_string(std::get<2>(*failed_node_));
    throw std::runtime_error("external service failure " + std::to_string(static_cast<int>(failure_)) +
                             identity + ": " + diagnostic_);
}

[[noreturn]] void ExternalMemoryService::fail(Failure code, const char* message, std::optional<NodeKey> node) {
    remember(code, message, node);
    rethrow_failure();
    std::terminate();
}

void ExternalMemoryService::check() {
    rethrow_failure();
    if (submitting_) {
        for (auto& [id, pending] : pending_)
            if (pending.state == State::Submitting) pending.state = State::AcceptanceUnknown;
        fail(Failure::Protocol, "provider reentry has unknown acceptance");
    }
    if (stopped_) fail(Failure::Stopped, "bridge is stopped");
}

ExternalMemoryService::Owner& ExternalMemoryService::owner(Workload& workload) {
    const auto found = owners_.find(workload.sys->id);
    if (found == owners_.end() || found->second.retired || found->second.workload != &workload)
        fail(Failure::Owner, "unregistered or retired owner");
    return found->second;
}

void ExternalMemoryService::bind(Workload& workload) {
    check();
    auto found = owners_.find(workload.sys->id);
    if (workload.sys->joint_source_context != context_ || found == owners_.end() ||
        found->second.workload || found->second.retired)
        fail(Failure::Owner, "rank binding is foreign, duplicate, or retired");
    found->second.workload = &workload;
}

void ExternalMemoryService::released() {
    for (auto& [rank, entry] : owners_) {
        if (entry.local_wait) { entry.local_wait = false; entry.wake = true; }
    }
}

void ExternalMemoryService::retire(Workload& workload) noexcept {
    auto found = owners_.find(workload.sys->id);
    if (found == owners_.end() || found->second.workload != &workload) return;
    found->second = {nullptr, true, false, false};
    // Destructor paths only retain bounded identities, never a dangling callback target.
    for (const auto& [id, pending] : pending_) {
        if (std::get<0>(id) == static_cast<uint32_t>(workload.sys->id) && failure_ == Failure::None) {
            try { remember(Failure::Owner, "owner retired with unfinished external memory", id); }
            catch (...) { failure_ = Failure::Owner; failed_node_ = id; }
            break;
        }
    }
    for (auto it = gates_.begin(); it != gates_.end();) {
        if (std::get<0>(*it) == static_cast<uint32_t>(workload.sys->id)) {
            it = gates_.erase(it); charge_ -= gate_charge_;
        } else ++it;
    }
    released();
}

ExternalMemoryService::Admission ExternalMemoryService::submit(Workload& workload, uint64_t node,
    std::shared_ptr<const JointNodeAccess> access, MemoryOperation operation) {
    check();
    auto& entry = owner(workload);
    if (!access || workload.is_finished) fail(Failure::Owner, "invalid source lease or finished owner");
    const auto rank = static_cast<uint32_t>(workload.sys->id);
    const AdmissionKey ingress{rank, access->tier_id, access->device_id};
    const NodeKey id{rank, workload.iteration, node};
    if (pending_.count(id)) fail(Failure::Protocol, "duplicate pending node", id);
    if (gates_.count(ingress)) return Admission::Rejected;
    if (pending_.size() >= limits_.pending_nodes ||
        pending_charge_ + gate_charge_ > limits_.descriptor_bytes - charge_) {
        entry.local_wait = true;
        return Admission::Rejected;
    }
    NodeRequest request{{run_, session_, rank, workload.iteration, node}, operation,
                        ingress, Sys::boostedTick(), std::move(access)};
    if (request.submitted_ns > std::numeric_limits<uint64_t>::max() / 1000000)
        fail(Failure::Protocol, "submission time cannot be represented in fs", id);
    auto [position, inserted] = pending_.emplace(id, Pending{std::move(request), State::Submitting});
    charge_ += pending_charge_;
    submitting_ = true;
    Admission result;
    try {
        result = provider_->try_submit(position->second.request);
    } catch (const std::exception& error) {
        position->second.state = State::AcceptanceUnknown;
        submitting_ = false;
        fail(Failure::Provider, error.what(), id);
    } catch (...) {
        position->second.state = State::AcceptanceUnknown;
        submitting_ = false;
        fail(Failure::Provider, "non-standard provider exception", id);
    }
    submitting_ = false;
    if (failure_ != Failure::None) {
        position->second.state = State::AcceptanceUnknown;
        rethrow_failure();
    }
    if (result == Admission::Accepted) position->second.state = State::Accepted;
    else if (result == Admission::Rejected) {
        pending_.erase(position); charge_ -= pending_charge_;
        gates_.insert(ingress); charge_ += gate_charge_;
        released();
    } else {
        position->second.state = State::AcceptanceUnknown;
        fail(Failure::Provider, "invalid admission result", id);
    }
    return result;
}

void ExternalMemoryService::validate_edge(ReceiveEdge edge) {
    if (edge.visible_ns != Sys::boostedTick() ||
        (edge.last_closed_ns && (*edge.last_closed_ns >= edge.visible_ns ||
          (last_closed_ns_ && *edge.last_closed_ns < *last_closed_ns_))) ||
        (last_closed_ns_ && !edge.last_closed_ns))
        fail(Failure::Protocol, "invalid current receive edge");
    if (edge.last_closed_ns) last_closed_ns_ = edge.last_closed_ns;
}

void ExternalMemoryService::complete(const Response& response, ReceiveEdge edge) {
    check();
    validate_edge(edge);
    const auto id = key(response.identity);
    const auto found = pending_.find(id);
    if (found == pending_.end() || found->second.state != State::Accepted ||
        !(found->second.request.identity == response.identity))
        fail(Failure::Completion, "unknown, stale, or mismatched completion", id);
    const auto& request = found->second.request;
    if (response.outcome != Outcome::Success)
        fail(Failure::Completion, response.outcome == Outcome::Partial ? "partial completion" : "error completion", id);
    const auto rounded_ns = response.completed_fs / 1000000 + (response.completed_fs % 1000000 != 0);
    const auto visible_ns = edge.last_closed_ns ? std::max(rounded_ns, *edge.last_closed_ns + 1) : rounded_ns;
    if (response.completed_fs < request.submitted_ns * 1000000 || visible_ns != edge.visible_ns)
        fail(Failure::Completion, "completion violates actual or visible time", id);
    auto& entry = owners_.at(response.identity.rank);
    if (!entry.workload || entry.retired || entry.workload->is_finished ||
        entry.workload->iteration != response.identity.iteration)
        fail(Failure::Owner, "completion owner retired or changed iteration", id);
    auto* workload = entry.workload;
    auto handler = std::make_unique<WorkloadLayerHandlerData>();
    handler->sys_id = workload->sys->id;
    handler->workload = workload;
    handler->node_id = response.identity.node;
    handler->device_id = std::get<2>(request.admission_key);
    handler->memory_operation = request.operation;
    pending_.erase(found); charge_ -= pending_charge_;
    released();
    try {
        workload->call(EventType::General, handler.release());
    } catch (const std::exception& error) { fail(Failure::Owner, error.what(), id); }
      catch (...) { fail(Failure::Owner, "non-standard callback exception", id); }
    rethrow_failure();
}

void ExternalMemoryService::notify_capacity(AdmissionKey key) {
    check();
    const auto found = owners_.find(std::get<0>(key));
    if (found == owners_.end() || found->second.retired || !found->second.workload)
        fail(Failure::Owner, "capacity notification for unknown or retired rank");
    if (gates_.erase(key)) {
        charge_ -= gate_charge_;
        released();
        found->second.wake = true;
    }
}

void ExternalMemoryService::resume_ready(ReceiveEdge edge) {
    check();
    validate_edge(edge);
    std::vector<Workload*> ready;
    for (auto& [rank, entry] : owners_) {
        if (entry.wake && entry.workload && !entry.retired) ready.push_back(entry.workload);
        entry.wake = false;
    }
    for (auto* workload : ready) {
        try { if (!workload->is_finished) workload->issue_dep_free_nodes(); }
        catch (const std::exception& error) { fail(Failure::Owner, error.what()); }
        catch (...) { fail(Failure::Owner, "non-standard ready callback exception"); }
        rethrow_failure();
    }
}

bool ExternalMemoryService::pending_for(const Workload& owner) const {
    for (const auto& [id, pending] : pending_)
        if (std::get<0>(id) == static_cast<uint32_t>(owner.sys->id)) return true;
    return false;
}

ExternalMemoryService::Status ExternalMemoryService::status() const {
    Status result{0, 0, gates_.size(), 0, charge_, failure_ != Failure::None, stopped_};
    for (const auto& [id, pending] : pending_) {
        if (pending.state == State::Accepted) {
            ++result.accepted;
            if (!result.first_accepted) result.first_accepted = id;
        } else {
            ++result.acceptance_unknown;
            if (!result.first_unknown) result.first_unknown = id;
        }
    }
    for (const auto& [rank, entry] : owners_) if (entry.local_wait || entry.wake) ++result.waiting_owners;
    return result;
}

void ExternalMemoryService::close() {
    check();
    if (!pending_.empty() || !gates_.empty() || status().waiting_owners)
        fail(Failure::Protocol, "bridge not locally drained");
    stopped_ = true;
}

ExternalMemoryService::Status ExternalMemoryService::abort_local() noexcept {
    if (submitting_) {
        // Do not invalidate the active provider call's request or map iterator.
        for (auto& [id, pending] : pending_)
            if (pending.state == State::Submitting) pending.state = State::AcceptanceUnknown;
        if (failure_ == Failure::None) failure_ = Failure::Protocol;
        return status();
    }
    if (!aborted_.stopped) {
        aborted_ = status();
        aborted_.stopped = true;
        stopped_ = true;
        pending_.clear(); gates_.clear(); charge_ = 0;
        for (auto& [rank, entry] : owners_) entry = {nullptr, true, false, false};
    }
    return aborted_;
}
}  // namespace AstraSim
