/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "TransportServiceAdapter.hh"

#include <stdexcept>
#include <limits>
#include <utility>

#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include "astra-sim/system/Sys.hh"

namespace AstraSim {
TransportRequest::TransportRequest(uint32_t rank, std::string id, uint64_t bytes,
                                   Callable& completion)
    : id_(ServiceBindingJson::name(id)), bytes_(bytes), completion_(completion) {
    sys_id = rank;
    device_id = 0;
    completion_target = this;
}

void TransportRequest::call(EventType type, CallData* data) {
    if (!service_) throw std::logic_error("transport callback before submission");
    try {
        service_->rethrow_failure();
        if (data != this || phase_ == Phase::Complete || phase_ == Phase::Failed) {
            throw std::logic_error("invalid transport completion context");
        }
        if (phase_ == Phase::Service) {
            if (memory_finish_ns <= memory_start_ns || memory_finish_ns != Sys::boostedTick()) {
                throw std::logic_error("transport service callback at invalid tick");
            }
            finish_ns_ = memory_finish_ns;
            if (service_->policy_ == TransportLatencyPolicy::Propagation && service_->latency_ns_) {
                if (service_->latency_ns_ > std::numeric_limits<uint64_t>::max() - finish_ns_) {
                    throw std::overflow_error("transport arrival exceeds uint64 ns");
                }
                finish_ns_ += service_->latency_ns_;
                phase_ = Phase::Propagation;
                service_->sys_->register_event(this, type, data, service_->latency_ns_);
                return;
            }
        } else if (Sys::boostedTick() != finish_ns_) {
            throw std::logic_error("transport arrival callback at invalid tick");
        }
        phase_ = Phase::Complete;
        completed_ = true;
        completion_.call(type, data);  // Preserve the caller's derived handler.
    } catch (...) {
        completed_ = false;
        phase_ = Phase::Failed;
        service_->failure_ = std::current_exception();
        throw;
    }
}

TransportReceipt TransportRequest::receipt() const {
    if (!completed_) throw std::logic_error("transport has no completed receipt");
    return {id_, service_->link_.id, service_->link_.resource, bytes_,
            memory_ready_ns, memory_start_ns, finish_ns_, memory_finish_ns};
}

TransportServiceAdapter::TransportServiceAdapter(
    uint32_t rank, TransportLink link, TransportResource resource, PhysicalServiceAdapter& adapter,
    TransportLatencyPolicy policy)
    : rank_(rank), link_(std::move(link)), service_time_(resource.bandwidth),
      latency_ns_(resource.latency_ns), policy_(policy), adapter_(adapter) {}

void TransportServiceAdapter::set_sys(Sys* sys) {
    adapter_.set_sys(rank_, sys);
    sys_ = sys;
}

void TransportServiceAdapter::issue(TransportRequest& request) {
    rethrow_failure();
    if (request.service_ || request.sys_id != rank_ || request.device_id != 0 ||
        request.pim_enabled || request.service_device_id || request.completion_target != &request) {
        throw std::invalid_argument("invalid or reused transport request context");
    }
    // Validate before queueing; queued start-time overflow remains a backend error.
    service_time_.service_time_ns(request.bytes_, MemoryOperation::Read, latency_ns_,
                                  MemoryTimeRounding::Ceil);
    request.service_ = this;
    adapter_.issue({request.bytes_, MemoryOperation::Read, MemoryTimeRounding::Ceil}, &request);
}

void TransportServiceAdapter::rethrow_failure() const {
    if (failure_) std::rethrow_exception(failure_);
    adapter_.rethrow_failure();
}
}  // namespace AstraSim
