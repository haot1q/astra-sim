// Dependency delays reuse the original event queue without hardware occupancy.
#include "astra-sim/workload/Workload.hh"
#include "astra-sim/workload/DependencyDelay.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"

namespace AstraSim {

void Workload::issue_dependency_delay(
    const std::shared_ptr<Chakra::ETFeederNode>& node, uint64_t duration_ns) {
    validate_dependency_delay_deadline(Sys::boostedTick(), duration_ns);
    if (duration_ns == 0) {
        skip_invalid(node);
        return;
    }
    if (!pending_dependency_delays_.insert(node->id()).second) {
        throw std::logic_error("dependency delay submitted twice");
    }
    auto handler = std::make_unique<WorkloadLayerHandlerData>();
    handler->node_id = node->id();
    sys->register_event(this, EventType::General, handler.get(), duration_ns);
    handler.release();
}

}  // namespace AstraSim
