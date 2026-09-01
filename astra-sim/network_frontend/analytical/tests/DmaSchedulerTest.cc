/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "astra-sim/system/memory/DmaScheduler.hh"

namespace {

using AstraSim::DmaJob;
using AstraSim::DmaPriorityClass;
using AstraSim::DmaScheduler;
using AstraSim::DmaSchedulerConfig;

template <typename Error, typename Fn> bool throws(Fn fn) {
    try {
        fn();
    } catch (const Error&) {
        return true;
    }
    return false;
}

DmaJob job(std::string event_id,
           DmaPriorityClass priority,
           std::vector<std::string> dependencies = {},
           std::string path_id = "base_die_local") {
    const std::vector<std::string> resources =
        path_id == "base_die_local"
            ? std::vector<std::string>{"lpddr_read", "base_die_dma",
                                       "local_stack_fabric", "hbm_write"}
            : std::vector<std::string>{"lpddr_read", "base_to_gpu_link",
                                       "gpu_dma", "gpu_to_base_link",
                                       "hbm_write"};
    return {
        std::move(event_id),
        0,
        0,
        4096,
        "page_promote",
        "whole_object",
        priority,
        std::move(path_id),
        2,
        0,
        1,
        0,
        resources,
        std::move(dependencies),
        true,
        true,
    };
}

bool single_engine_and_dependency_contract() {
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    scheduler.submit(job("background", DmaPriorityClass::BackgroundFill));
    scheduler.submit(job("critical", DmaPriorityClass::DecodeCritical));
    scheduler.submit(job("dependent", DmaPriorityClass::Demand, {"critical"}));

    auto first = scheduler.dispatch_ready(10);
    if (first.size() != 1 || first[0].job.event_id != "critical" ||
        first[0].engine_id != 0 || scheduler.pending_count() != 2) {
        return false;
    }
    const auto first_receipt = scheduler.complete("critical", 30);
    if (first_receipt.queue_wait_ns != 10 || first_receipt.service_ns != 20) {
        return false;
    }
    auto second = scheduler.dispatch_ready(30);
    if (second.size() != 1 || second[0].job.event_id != "dependent") {
        return false;
    }
    scheduler.complete("dependent", 40);
    auto third = scheduler.dispatch_ready(40);
    if (third.size() != 1 || third[0].job.event_id != "background") {
        return false;
    }
    scheduler.complete("background", 50);
    return scheduler.drained();
}

bool multi_engine_and_tie_break_contract() {
    DmaScheduler scheduler({"gpu_routed", 2, 8, 2});
    auto second = job("event-b", DmaPriorityClass::Demand, {}, "gpu_routed");
    auto first = job("event-a", DmaPriorityClass::Demand, {}, "gpu_routed");
    scheduler.submit(std::move(second));
    scheduler.submit(std::move(first));
    const auto dispatches = scheduler.dispatch_ready(0);
    return dispatches.size() == 2 && dispatches[0].job.event_id == "event-b" &&
           dispatches[1].job.event_id == "event-a" &&
           dispatches[0].engine_id != dispatches[1].engine_id;
}

bool bounded_priority_burst_contract() {
    DmaScheduler scheduler({"base_die_local", 1, 2, 1});
    scheduler.submit(job("high-1", DmaPriorityClass::DecodeCritical));
    scheduler.submit(job("high-2", DmaPriorityClass::PrefillCritical));
    scheduler.submit(job("high-3", DmaPriorityClass::Demand));
    scheduler.submit(job("low", DmaPriorityClass::BackgroundFill));

    const auto first = scheduler.dispatch_ready(0);
    scheduler.complete(first[0].job.event_id, 1);
    const auto second = scheduler.dispatch_ready(1);
    scheduler.complete(second[0].job.event_id, 2);
    const auto third = scheduler.dispatch_ready(2);
    return first[0].job.event_id == "high-1" &&
           second[0].job.event_id == "high-2" && third[0].job.event_id == "low";
}

bool fail_closed_contract() {
    return throws<std::invalid_argument>([]() {
               DmaScheduler invalid({"unknown", 1, 1, 1});
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler invalid({"base_die_local", 0, 1, 1});
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler invalid({"base_die_local", 2, 1, 3});
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler scheduler({"base_die_local", 1, 1, 1});
               auto invalid = job("cross-pair", DmaPriorityClass::Demand);
               invalid.destination_device_id = 1;
               scheduler.submit(std::move(invalid));
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler scheduler({"base_die_local", 1, 1, 1});
               scheduler.submit(job("duplicate", DmaPriorityClass::Demand));
               scheduler.submit(job("duplicate", DmaPriorityClass::Demand));
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler scheduler({"base_die_local", 1, 1, 1});
               scheduler.complete("missing", 1);
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler scheduler({"base_die_local", 1, 1, 1});
               scheduler.submit(
                   job("dangling", DmaPriorityClass::Demand, {"missing"}));
               scheduler.validate_dependencies();
           }) &&
           throws<std::invalid_argument>([]() {
               DmaScheduler scheduler({"base_die_local", 1, 1, 1});
               scheduler.submit(
                   job("cycle-a", DmaPriorityClass::Demand, {"cycle-b"}));
               scheduler.submit(
                   job("cycle-b", DmaPriorityClass::Demand, {"cycle-a"}));
               scheduler.validate_dependencies();
           });
}

bool page_movement_storm_limit_contract() {
    DmaScheduler scheduler({"base_die_local", 2, 8, 1});
    auto background = job("background", DmaPriorityClass::BackgroundFill);
    background.page_movement = false;
    auto first_page = job("page-1", DmaPriorityClass::PrefillCritical);
    auto second_page = job("page-2", DmaPriorityClass::Demand);
    auto ordinary_load = job("load", DmaPriorityClass::Demand);
    ordinary_load.page_movement = false;
    scheduler.submit(std::move(background));
    scheduler.submit(std::move(first_page));
    scheduler.submit(std::move(second_page));
    scheduler.submit(std::move(ordinary_load));

    const auto first = scheduler.dispatch_ready(0);
    if (first.size() != 2 || first[0].job.event_id != "page-1" ||
        first[1].job.event_id != "load") {
        return false;
    }
    scheduler.complete("page-1", 1);
    scheduler.complete("load", 1);
    const auto second = scheduler.dispatch_ready(1);
    return second.size() == 2 && second[0].job.event_id == "page-2" &&
           second[1].job.event_id == "background";
}

bool forward_reference_and_unique_dependency_contract() {
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    scheduler.submit(job("dependent", DmaPriorityClass::Demand,
                         {"dependency-a", "dependency-b", "dependency-a"}));
    scheduler.submit(job("dependency-a", DmaPriorityClass::Demand));
    scheduler.submit(job("dependency-b", DmaPriorityClass::Demand));
    scheduler.validate_dependencies();

    const auto first = scheduler.dispatch_ready(0);
    if (first.size() != 1 || first[0].job.event_id != "dependency-a") {
        return false;
    }
    scheduler.complete("dependency-a", 1);
    const auto second = scheduler.dispatch_ready(1);
    if (second.size() != 1 || second[0].job.event_id != "dependency-b") {
        return false;
    }
    scheduler.complete("dependency-b", 2);
    const auto third = scheduler.dispatch_ready(2);
    return third.size() == 1 && third[0].job.event_id == "dependent";
}

bool future_ready_time_contract() {
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    auto future = job("future", DmaPriorityClass::DecodeCritical);
    future.ready_ns = 10;
    auto current = job("current", DmaPriorityClass::BackgroundFill);
    current.page_movement = false;
    scheduler.submit(std::move(future));
    scheduler.submit(std::move(current));

    const auto first = scheduler.dispatch_ready(0);
    if (first.size() != 1 || first[0].job.event_id != "current") {
        return false;
    }
    scheduler.complete("current", 1);
    if (!scheduler.dispatch_ready(9).empty()) {
        return false;
    }
    const auto second = scheduler.dispatch_ready(10);
    return second.size() == 1 && second[0].job.event_id == "future" &&
           second[0].start_ns == 10;
}

bool dependency_validation_is_dirty_only_after_submission() {
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    scheduler.submit(job("valid", DmaPriorityClass::Demand));
    scheduler.validate_dependencies();
    scheduler.validate_dependencies();
    if (scheduler.diagnostics().full_dependency_validations != 1) {
        return false;
    }
    scheduler.submit(job("dangling", DmaPriorityClass::Demand, {"missing"}));
    return throws<std::invalid_argument>(
               [&scheduler]() { scheduler.validate_dependencies(); }) &&
           scheduler.diagnostics().full_dependency_validations == 2;
}

bool cycle_added_after_successful_validation_is_rejected() {
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    scheduler.submit(job("root", DmaPriorityClass::Demand));
    scheduler.validate_dependencies();
    scheduler.submit(job("cycle-a", DmaPriorityClass::Demand, {"cycle-b"}));
    scheduler.submit(job("cycle-b", DmaPriorityClass::Demand, {"cycle-a"}));
    return throws<std::invalid_argument>(
               [&scheduler]() { scheduler.validate_dependencies(); }) &&
           scheduler.diagnostics().full_dependency_validations == 2;
}

bool long_chain_uses_direct_dependency_release() {
    constexpr std::size_t kJobCount = 4096;
    DmaScheduler scheduler({"base_die_local", 1, 8, 1});
    for (std::size_t index = 0; index < kJobCount; ++index) {
        const auto event_id = "chain-" + std::to_string(index);
        const auto dependencies =
            index == 0 ? std::vector<std::string>{}
                       : std::vector<std::string>{"chain-" +
                                                  std::to_string(index - 1)};
        scheduler.submit(job(event_id, DmaPriorityClass::Demand, dependencies));
    }
    scheduler.validate_dependencies();
    for (std::size_t index = 0; index < kJobCount; ++index) {
        scheduler.validate_dependencies();
        const auto dispatches = scheduler.dispatch_ready(index);
        const auto expected = "chain-" + std::to_string(index);
        if (dispatches.size() != 1 || dispatches[0].job.event_id != expected) {
            return false;
        }
        scheduler.complete(expected, index + 1);
    }
    const auto& diagnostics = scheduler.diagnostics();
    return scheduler.drained() &&
           diagnostics.full_dependency_validations == 1 &&
           diagnostics.dependency_release_visits == kJobCount - 1 &&
           diagnostics.ready_queue_pops == kJobCount;
}

}  // namespace

int main() {
    return single_engine_and_dependency_contract() &&
                   multi_engine_and_tie_break_contract() &&
                   bounded_priority_burst_contract() &&
                   page_movement_storm_limit_contract() &&
                   forward_reference_and_unique_dependency_contract() &&
                   future_ready_time_contract() &&
                   dependency_validation_is_dirty_only_after_submission() &&
                   cycle_added_after_successful_validation_is_rejected() &&
                   long_chain_uses_direct_dependency_release() &&
                   fail_closed_contract()
               ? 0
               : 1;
}
