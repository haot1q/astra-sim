/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __NATIVE_TAG_EVENTS_TEST_HH__
#define __NATIVE_TAG_EVENTS_TEST_HH__

#include "astra-sim/system/NativeTagRegistry.hh"
#include "astra-sim/system/Sys.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace NativeTagEventsTest {
using namespace AstraSim;
inline void count(void* raw) { ++*static_cast<unsigned*>(raw); }
inline void fail_callback(void*) { throw std::runtime_error("original native callback failed"); }

inline void post(Sys* sys, bool send, int tag, uint64_t generation, unsigned* calls) {
    sim_request request{};
    request.srcRank = 0;
    request.dstRank = 1;
    request.tag = tag;
    request.reqType = UINT8;
    request.reqCount = 8;
    if (send) sys->front_end_sim_send(0, Sys::dummy_data, 8, UINT8, 1, tag, &request,
        Sys::FrontEndSendRecvType::NATIVE, count, calls, generation);
    else sys->front_end_sim_recv(0, Sys::dummy_data, 8, UINT8, 0, tag, &request,
        Sys::FrontEndSendRecvType::NATIVE, count, calls, generation);
}

struct ConflictingEvent : Callable {
    Sys* sys;
    unsigned calls = 0;
    explicit ConflictingEvent(Sys* owner) : sys(owner) {}
    void call(EventType, CallData*) override { post(sys, true, 1000000000, 0, &calls); }
};

inline void run(const std::string& mode, const std::vector<Sys*>& systems,
                const std::shared_ptr<NetworkAnalytical::EventQueue>& events) {
    NativeTagRegistry tags;
    for (auto* sys : systems) sys->bind_native_tags(&tags);
    if (mode == "callback_failure") {
        unsigned calls = 0;
        sim_request request{};
        post(systems[1], false, 0, 0, &calls);
        systems[0]->front_end_sim_send(0, Sys::dummy_data, 8, UINT8, 1, 0, &request,
            Sys::FrontEndSendRecvType::NATIVE, fail_callback, nullptr);
        while (!events->finished()) {
            events->proceed();
            tags.rethrow_failure();
        }
        throw std::runtime_error("original callback failure was lost");
    }
    if (mode == "collision_event") {
        tags.acquire(0, 1, 8, 0);
        ConflictingEvent conflict(systems[0]);
        systems[0]->register_event(&conflict, EventType::General, nullptr, 1);
        events->proceed();
        tags.rethrow_failure();
        throw std::runtime_error("conflicting Sys event did not fail closed");
    }
    if (mode != "ordinary_late") throw std::invalid_argument("unknown tag test mode");
    unsigned calls = 0;
    post(systems[0], true, 1000000000, 0, &calls);
    while (!events->finished()) events->proceed();
    if (calls != 1 || tags.drained()) throw std::runtime_error("late-receive occupancy lost");
    bool rejected = false;
    try { tags.acquire(0, 1, 8, 0); } catch (const std::invalid_argument&) { rejected = true; }
    if (!rejected) throw std::runtime_error("unmatched ordinary send tag was reused");
    post(systems[1], false, 0, 0, &calls);
    while (!events->finished()) events->proceed();
    if (calls != 2 || !tags.drained()) throw std::runtime_error("ordinary transfer failed to drain");
    const auto lease = tags.acquire(0, 1, 8, 0);
    post(systems[1], false, lease.tag, lease.generation, &calls);
    post(systems[0], true, lease.tag, lease.generation, &calls);
    while (!events->finished()) events->proceed();
    tags.release(lease);
    tags.rethrow_failure();
    if (calls != 4 || !tags.drained()) throw std::runtime_error("leased callbacks did not drain");
}
}  // namespace NativeTagEventsTest
#endif
