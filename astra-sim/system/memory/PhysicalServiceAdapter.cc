/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "PhysicalServiceAdapter.hh"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "astra-sim/system/WorkloadLayerHandlerData.hh"

namespace AstraSim {

PhysicalServiceAdapter::PhysicalServiceAdapter(
    uint32_t rank, std::vector<AstraMemoryAPI*> devices, MemoryTimeRounding rounding)
    : rank_(rank), rounding_(rounding), devices_(std::move(devices)) {
    if (devices_.empty() ||
        std::any_of(devices_.begin(), devices_.end(), [](auto* api) { return api == nullptr; })) {
        throw std::invalid_argument("physical service adapter requires complete device bindings");
    }
    if (rounding != MemoryTimeRounding::Floor && rounding != MemoryTimeRounding::Ceil) {
        throw std::invalid_argument("invalid physical service rounding mode");
    }
}

void PhysicalServiceAdapter::set_sys(int id, Sys* sys) {
    if (id < 0 || static_cast<uint32_t>(id) != rank_ || sys == nullptr ||
        (sys_ != nullptr && sys_ != sys)) {
        throw std::invalid_argument("physical service adapter Sys ownership mismatch");
    }
    sys_ = sys;
    for (auto* device : devices_) device->set_sys(id, sys);
}

void PhysicalServiceAdapter::issue(
    const MemoryRequest& request, WorkloadLayerHandlerData* handler) {
    try {
        rethrow_failure();
        if (!sys_ || !handler || handler->sys_id < 0 ||
            static_cast<uint32_t>(handler->sys_id) != rank_ ||
            handler->device_id >= devices_.size() || handler->pim_enabled) {
            throw std::invalid_argument("invalid rank/device or unsupported native PIM service request");
        }
        if (request.operation != MemoryOperation::Read && request.operation != MemoryOperation::Write) {
            throw std::invalid_argument("invalid physical memory operation");
        }
        handler->service_device_id = 0;
        auto physical_request = request;
        physical_request.rounding = rounding_;
        devices_.at(handler->device_id)->issue(physical_request, handler);
    } catch (...) {
        // Sys::call_events catches callbacks. Preserve failure for the frontend
        // to rethrow rather than allowing a malformed request to report success.
        failure_ = std::current_exception();
        throw;
    }
}

MemoryLocationType PhysicalServiceAdapter::get_memory_location_type() const {
    return devices_.front()->get_memory_location_type();
}

void PhysicalServiceAdapter::rethrow_failure() const {
    if (failure_) std::rethrow_exception(failure_);
}

}  // namespace AstraSim
