/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "PdPathExecutor.hh"

#include <algorithm>
#include "MemoryMovementExecutor.hh"
#include "PdPathWire.hh"
#include "astra-sim/system/RecvPacketEventHandlerData.hh"
#include "astra-sim/system/SendPacketEventHandlerData.hh"
#include "astra-sim/system/Sys.hh"

namespace AstraSim {
namespace {
using namespace PdPathWire;
}

PdPathExecutor::PdPathExecutor(const std::vector<Sys*>& systems, PhysicalServiceFactory& services,
                               NativeTagRegistry& tags, Json actual_network)
    : systems_(systems), services_(services), tags_(tags), network_(std::move(actual_network)) {}

void PdPathExecutor::submit(const std::string& path, uint64_t ready_ns) {
    rethrow_failure();
    auto work = load_pd_path_work(path, services_.configuration(), services_.pd_path_config());
    const auto id = text(work.descriptor.at("transfer_id"));
    require(!used_transfers_.count(id) && next_work_ < UINT64_MAX, "duplicate P/D path transfer");
    auto stages = bind_pd_path_stages(work, systems_, services_, network_);
    auto transfer = std::make_unique<Transfer>();
    transfer->work = std::move(work);
    transfer->stages = std::move(stages);
    transfer->slices.resize(transfer->work.slices.size());
    transfer->ready_ns = ready_ns;
    const auto key = next_work_++;
    transfers_.emplace(key, std::move(transfer));
    used_transfers_.insert(id);
    try {
        auto* start = callback(key, 0, CallbackKind::Start);
        auto& active = *transfers_.at(key);
        ++active.pending;
        const auto now = static_cast<uint64_t>(Sys::boostedTick());
        systems_.at(start->sys_id)->register_event(this, EventType::General, start,
                                                  ready_ns > now ? ready_ns - now : 0);
    } catch (...) {
        if (!failure_) failure_ = std::current_exception();
        throw;
    }
}

PdPathExecutor::Callback* PdPathExecutor::callback(uint64_t work, uint64_t slice, CallbackKind kind) {
    const auto& transfer = *transfers_.at(work);
    auto* value = new Callback;
    value->work_id = work;
    value->slice = slice;
    value->stage = transfer.slices.at(slice).stage;
    value->kind = kind;
    value->sys_id = transfer.stages.at(slice).at(value->stage).rank;
    value->device_id = transfer.stages.at(slice).at(value->stage).device;
    value->completion_target = this;
    return value;
}

void PdPathExecutor::pump(uint64_t work) {
    auto& transfer = *transfers_.at(work);
    if (transfer.cancelled) return;
    const auto window = number(transfer.work.route.at("in_flight_bytes"), true);
    for (size_t index = 0; index < transfer.slices.size(); ++index) {
        auto& state = transfer.slices[index];
        const auto& slice = transfer.work.slices[index];
        auto& occupied = transfer.active_bytes[slice.source_rank];
        if (state.started || slice.bytes > window - occupied) continue;
        occupied += slice.bytes;
        state.started = true;
        issue(work, index);
    }
}

void PdPathExecutor::issue(uint64_t work, uint64_t slice) {
    auto& transfer = *transfers_.at(work);
    auto& state = transfer.slices.at(slice);
    const auto& stage = transfer.stages.at(slice).at(state.stage);
    state.ready_ns = Sys::boostedTick();
    if (stage.kind == "network") {
        issue_network(work, slice);
        return;
    }
    auto* handler = callback(work, slice, CallbackKind::Memory);
    ++transfer.pending;
    if (stage.bytes == 0) {
        handler->memory_ready_ns = handler->memory_start_ns = handler->memory_finish_ns = state.ready_ns;
        systems_.at(stage.rank)->register_event(this, EventType::General, handler, 0);
    } else {
        stage.api->issue({stage.bytes, stage.operation}, handler);
    }
}

void PdPathExecutor::issue_network(uint64_t work, uint64_t index) {
    auto& transfer = *transfers_.at(work);
    auto& state = transfer.slices.at(index);
    const auto& slice = transfer.work.slices.at(index);
    state.sent = state.received = false;
    state.lease = tags_.acquire(slice.source_rank, slice.destination_rank, slice.bytes);
    transfer.pending += 2;
    sim_request request{};
    request.srcRank = slice.source_rank;
    request.dstRank = slice.destination_rank;
    request.tag = state.lease.tag;
    request.reqType = UINT8;
    request.reqCount = slice.bytes;
    auto* recv = new RecvPacketEventHandlerData;
    recv->callable = this;
    recv->wlhd = callback(work, index, CallbackKind::Receive);
    recv->event = EventType::PacketReceived;
    systems_.at(slice.destination_rank)->front_end_sim_recv(0, Sys::dummy_data, slice.bytes,
        UINT8, slice.source_rank, state.lease.tag, &request, Sys::FrontEndSendRecvType::NATIVE,
        &Sys::handleEvent, recv, state.lease.generation);
    auto* send = new SendPacketEventHandlerData;
    send->callable = this;
    send->wlhd = callback(work, index, CallbackKind::Send);
    send->event = EventType::PacketSent;
    systems_.at(slice.source_rank)->front_end_sim_send(0, Sys::dummy_data, slice.bytes,
        UINT8, slice.destination_rank, state.lease.tag, &request, Sys::FrontEndSendRecvType::NATIVE,
        &Sys::handleEvent, send, state.lease.generation);
}

void PdPathExecutor::call(EventType event, CallData* data) {
    std::unique_ptr<Callback> handler(static_cast<Callback*>(data));
    try {
        tags_.rethrow_failure();
        rethrow_failure();
        services_.rethrow_failure();
        require(handler != nullptr, "missing P/D path callback");
        auto& transfer = *transfers_.at(handler->work_id);
        auto& state = transfer.slices.at(handler->slice);
        require(transfer.pending > 0 && handler->stage == state.stage, "stale P/D path callback");
        --transfer.pending;
        if (handler->kind == CallbackKind::Start) {
            require(event == EventType::General, "invalid P/D start event");
            pump(handler->work_id);
        } else if (handler->kind == CallbackKind::Memory) {
            require(event == EventType::General, "invalid P/D memory event");
            completed(handler->work_id, handler->slice, *handler);
        } else {
            const bool send = handler->kind == CallbackKind::Send;
            require(event == (send ? EventType::PacketSent : EventType::PacketReceived), "invalid P/D network event");
            auto& flag = send ? state.sent : state.received;
            require(!flag, "duplicate P/D network completion");
            flag = true;
            (send ? state.send_finish_ns : state.recv_finish_ns) = Sys::boostedTick();
            if (state.sent && state.received) {
                tags_.release(state.lease);
                completed(handler->work_id, handler->slice, *handler);
            }
        }
        finish_if_ready(handler->work_id);
    } catch (...) {
        if (!failure_) failure_ = std::current_exception();
        throw;
    }
}

Json PdPathExecutor::identity(const Transfer& transfer) const {
    Json result = {{"work_digest", transfer.work.digest},
        {"descriptor_digest", transfer.work.descriptor_digest},
        {"service_binding_digest", services_.identity().binding_digest},
        {"service_activation_id", services_.identity().activation_id}};
    for (const auto* name : {"run_id", "attempt_id", "transfer_id", "request_id"}) {
        result[name] = transfer.work.descriptor.at(name);
    }
    return result;
}

void PdPathExecutor::completed(uint64_t work, uint64_t index, Callback& handler) {
    auto& transfer = *transfers_.at(work);
    auto& state = transfer.slices.at(index);
    const auto& slice = transfer.work.slices.at(index);
    const auto& stage = transfer.stages.at(index).at(state.stage);
    auto receipt = identity(transfer);
    receipt.update({{"schema_version", "pd-path-stage-receipt-v1"}, {"slice_id", slice.id},
        {"stage_index", state.stage}, {"stage_id", stage.id}, {"resource_id", stage.resource},
        {"physical_resource_ref", stage.physical_resource}, {"kind", stage.kind},
        {"source_rank", slice.source_rank}, {"destination_rank", slice.destination_rank},
        {"rank", stage.rank}, {"device_id", stage.device}, {"payload_bytes", slice.bytes},
        {"billed_bytes", stage.bytes}, {"ready_ns", state.ready_ns},
        {"finish_ns", static_cast<uint64_t>(Sys::boostedTick())}, {"status", "success"}});
    if (stage.kind == "network") {
        receipt["start_ns"] = nullptr;  // Native network service start is not observable.
        receipt["send_finish_ns"] = state.send_finish_ns;
        receipt["recv_finish_ns"] = state.recv_finish_ns;
    } else {
        require(handler.memory_ready_ns == state.ready_ns && handler.memory_start_ns >= state.ready_ns &&
                handler.memory_finish_ns >= handler.memory_start_ns && handler.memory_finish_ns == Sys::boostedTick(),
                "P/D actual memory timing mismatch");
        receipt["start_ns"] = handler.memory_start_ns;
        receipt["send_finish_ns"] = receipt["recv_finish_ns"] = nullptr;
    }
    emit_memory_protocol_line("PD_PATH_STAGE_COMPLETE " + receipt.dump());
    if (transfer.cancelled) return;
    if (++state.stage < transfer.stages.at(index).size()) {
        issue(work, index);
    } else {
        state.finished = true;
        transfer.active_bytes.at(slice.source_rank) -= slice.bytes;
        pump(work);
    }
}

void PdPathExecutor::finish_if_ready(uint64_t work) {
    tags_.rethrow_failure();
    rethrow_failure();
    services_.rethrow_failure();
    auto& transfer = *transfers_.at(work);
    if (transfer.pending != 0) return;
    const auto complete = std::all_of(transfer.slices.begin(), transfer.slices.end(),
                                     [](const auto& slice) { return slice.finished; });
    require(complete || transfer.cancelled, "P/D path stalled without a pending service");
    auto receipt = identity(transfer);
    receipt.update({{"schema_version", "pd-path-receipt-v1"}, {"ready_ns", transfer.ready_ns},
        {"finish_ns", static_cast<uint64_t>(Sys::boostedTick())},
        {"charged_bytes", transfer.work.descriptor.at("charged_bytes_aggregate")},
        {"slice_count", transfer.slices.size()}, {"status", complete ? "success" : "cancelled"}});
    emit_memory_protocol_line("PD_PATH_COMPLETE " + receipt.dump());
    ++completed_count_;
    transfers_.erase(work);
}

void PdPathExecutor::cancel(const std::string& transfer_id) {
    rethrow_failure();
    for (auto& [key, transfer] : transfers_) {
        if (transfer->work.descriptor.at("transfer_id") != transfer_id) continue;
        require(!transfer->cancelled, "duplicate P/D path cancellation");
        transfer->cancelled = true;
        finish_if_ready(key);
        return;
    }
    throw std::invalid_argument("unknown P/D path cancellation");
}

bool PdPathExecutor::drained() const { return transfers_.empty(); }
void PdPathExecutor::rethrow_failure() const { if (failure_) std::rethrow_exception(failure_); }
}  // namespace AstraSim
