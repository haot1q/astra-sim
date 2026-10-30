#include "MemoryTransportAdapter.hh"

#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/MemoryCompletionTiming.hh"
#include "astra-sim/system/memory/TransportServiceAdapter.hh"

namespace AstraSim {
namespace {
void add(nlohmann::json& row, const char* key, uint64_t increment) {
    const auto previous = row.value(key, uint64_t{0});
    if (increment > UINT64_MAX - previous) throw std::overflow_error("memory transport counter overflow");
    row[key] = previous + increment;
}
}
struct MemoryTransportAdapter::Context {
    WorkloadLayerHandlerData* handler;
    Callable* target;
    MemoryRequest request;
    MemoryTransportOperation operation;
    TransportPathTiming fabric_time{}, link_time{};
    uint64_t ready;
    std::string counter_key;
};

MemoryTransportAdapter::MemoryTransportAdapter(uint32_t rank, std::string link,
    AstraMemoryAPI& original, const MemoryTransportConfig& config)
    : rank_(rank), link_(std::move(link)), original_(original), config_(config) {}

void MemoryTransportAdapter::attach(TransportPathAPI& path) {
    if (path_ && path_ != &path) throw std::logic_error("memory transport path already attached");
    path_ = &path;
}

void MemoryTransportAdapter::set_sys(int id, Sys* sys) {
    if (id < 0 || uint32_t(id) != rank_ || !sys || (sys_ && sys_ != sys)) {
        throw std::invalid_argument("memory transport Sys ownership mismatch");
    }
    original_.set_sys(id, sys);
    sys_ = sys;
}

MemoryLocationType MemoryTransportAdapter::get_memory_location_type() const {
    return original_.get_memory_location_type();
}

void MemoryTransportAdapter::issue(const MemoryRequest& request, WorkloadLayerHandlerData* handler) {
    bool created = false;
    try {
        rethrow_failure();
        if (!sys_ || !path_ || !handler || handler->sys_id < 0 || uint32_t(handler->sys_id) != rank_ ||
            !handler->completion_target || handler->pim_enabled || pending_.count(handler)) {
            throw std::invalid_argument("invalid memory transport request ownership");
        }
        handler->memory_total_wait_ns.reset();
        handler->memory_total_service_ns.reset();
        const auto found = config_.operations.find({rank_, link_, handler->device_id, request.operation});
        if (found == config_.operations.end()) { original_.issue(request, handler); return; }
        path_->validate_path(rank_, request.bytes, found->second.links);
        auto context = std::make_shared<Context>(Context{handler, handler->completion_target,
            request, found->second, {}, {}, Sys::boostedTick(), std::to_string(handler->device_id) +
                (request.operation == MemoryOperation::Read ? ":read" : ":write")});
        pending_.emplace(handler, context);
        created = true;
        handler->completion_target = this;
        if (context->operation.before) fabric(context);
        else original_.issue(request, handler);
    } catch (...) {
        const auto found = pending_.find(handler);
        if (created && found != pending_.end()) {
            handler->completion_target = found->second->target;
            pending_.erase(found);
        }
        failure_ = std::current_exception();
        throw;
    }
}

void MemoryTransportAdapter::fabric(const std::shared_ptr<Context>& context) {
    path_->submit_path({rank_, context->request.bytes, context->operation.links,
        [this, context](const TransportPathTiming& timing) {
            try {
                context->fabric_time = timing;
                if (context->operation.before) original_.issue(context->request, context->handler);
                else finish(context);
            } catch (...) { failure_ = std::current_exception(); throw; }
        }, [this, context](const TransportReceipt& receipt) {
            auto& row = counters_[context->counter_key]["resources"][receipt.resource_id];
            if (row.is_null()) row = nlohmann::json::object();
            add(row, "completed_bytes", receipt.payload_bytes);
            add(row, "wait_ns", receipt.start_ns - receipt.ready_ns);
            add(row, "service_ns", receipt.finish_ns - receipt.start_ns);
        }});
}

void MemoryTransportAdapter::call(EventType type, CallData* data) {
    (void)type;
    try {
        auto* handler = static_cast<WorkloadLayerHandlerData*>(data);
        const auto context = pending_.at(handler);
        const auto [wait, service] = memory_completion_timing(*handler, Sys::boostedTick(), false);
        context->link_time = {handler->memory_ready_ns, handler->memory_start_ns,
                              handler->memory_finish_ns, wait, service};
        if (context->operation.before) finish(context);
        else fabric(context);
    } catch (...) { failure_ = std::current_exception(); throw; }
}

void MemoryTransportAdapter::finish(const std::shared_ptr<Context>& context) {
    auto& handler = *context->handler;
    const auto& first = context->operation.before && !context->operation.links.empty()
        ? context->fabric_time : context->link_time;
    handler.memory_ready_ns = context->ready;
    handler.memory_start_ns = first.first_start_ns;
    handler.memory_finish_ns = Sys::boostedTick();
    auto wait = context->link_time.total_wait_ns, service = context->link_time.total_service_ns;
    if (context->fabric_time.total_wait_ns > UINT64_MAX - wait ||
        context->fabric_time.total_service_ns > UINT64_MAX - service) {
        throw std::overflow_error("memory transport aggregate overflow");
    }
    handler.memory_total_wait_ns = wait + context->fabric_time.total_wait_ns;
    handler.memory_total_service_ns = service + context->fabric_time.total_service_ns;
    memory_completion_timing(handler, Sys::boostedTick(), true);
    auto& row = counters_[context->counter_key];
    if (row.is_null()) row = nlohmann::json::object();
    add(row, "completed_atomics", 1);
    add(row, "completed_bytes", context->request.bytes);
    add(row, "wait_ns", *handler.memory_total_wait_ns);
    add(row, "service_ns", *handler.memory_total_service_ns);
    handler.completion_target = context->target;
    pending_.erase(&handler);
    context->target->call(EventType::General, &handler);  // May destroy handler or reenter issue.
}

void MemoryTransportAdapter::rethrow_failure() const {
    if (failure_) std::rethrow_exception(failure_);
}
bool MemoryTransportAdapter::drained() const { return !failure_ && pending_.empty(); }
nlohmann::json MemoryTransportAdapter::summary() const {
    return {{"rank", rank_}, {"logical_ucie_link", link_}, {"operations", counters_},
            {"pending_atomics", pending_.size()}};
}
}  // namespace AstraSim
