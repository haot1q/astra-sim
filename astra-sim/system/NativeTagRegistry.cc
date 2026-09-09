/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include "NativeTagRegistry.hh"

#include <stdexcept>

namespace AstraSim {
namespace {
void require(bool condition, const char* message) {
    if (!condition) throw std::invalid_argument(message);
}
}

int NativeTagRegistry::normalize(int tag) {
    require(tag >= 0, "negative NATIVE network tag");
    return tag % kTagCount;
}

NativeTagLease NativeTagRegistry::acquire(int source, int destination, uint64_t bytes,
                                        std::optional<int> preferred) {
    rethrow_failure();
    require(source >= 0 && destination >= 0 && source != destination && bytes > 0,
            "invalid NATIVE tag lease endpoints/bytes");
    require(next_generation_ < UINT64_MAX, "NATIVE tag generation exhausted");
    int tag = preferred ? normalize(*preferred) : cursor_;
    for (int count = 0; count < kTagCount; ++count) {
        const Key key{source, destination, tag};
        if (!leases_.count(key) && !ordinary_.count(key)) {
            NativeTagLease lease{next_generation_++, source, destination, tag};
            leases_.emplace(key, LeaseState{lease, bytes});
            cursor_ = (tag + 1) % kTagCount;
            return lease;
        }
        require(!preferred, "NATIVE normalized tag collides with active traffic");
        tag = (tag + 1) % kTagCount;
    }
    throw std::runtime_error("NATIVE tag space exhausted");
}

NativeTagRegistry::LeaseState& NativeTagRegistry::state(const NativeTagLease& lease) {
    const auto found = leases_.find({lease.source, lease.destination, lease.tag});
    require(found != leases_.end() && found->second.lease.generation == lease.generation,
            "stale NATIVE tag lease");
    return found->second;
}

void NativeTagRegistry::release(const NativeTagLease& lease) {
    auto& value = state(lease);
    require(value.completed[0] && value.completed[1], "NATIVE tag release before both callbacks");
    leases_.erase({lease.source, lease.destination, lease.tag});
}

void NativeTagRegistry::discard_unsubmitted(const NativeTagLease& lease) {
    auto& value = state(lease);
    require(!value.posted[0] && !value.posted[1], "NATIVE tag with posted work cannot be discarded");
    leases_.erase({lease.source, lease.destination, lease.tag});
}

NativeCallback NativeTagRegistry::observe(int source, int destination, int tag, uint64_t bytes,
                                         bool send, uint64_t generation, NativeCallback callback) {
    rethrow_failure();
    require(source >= 0 && destination >= 0 && callback.function && next_callback_ < UINT64_MAX,
            "invalid NATIVE callback");
    const Key key{source, destination, normalize(tag)};
    const auto found = leases_.find(key);
    if (generation) {
        require(found != leases_.end() && found->second.lease.generation == generation &&
                found->second.bytes == bytes && !found->second.posted[send],
                "NATIVE callback does not match lease or is duplicate");
    } else {
        require(found == leases_.end(), "ordinary NATIVE traffic collides with leased tag");
    }
    const auto id = next_callback_++;
    auto value = std::make_unique<CallbackState>(CallbackState{this, id, key, send, generation, bytes, callback});
    auto* argument = value.get();
    callbacks_.emplace(id, std::move(value));
    if (generation) found->second.posted[send] = true;
    else ++ordinary_[key][bytes].posted[send];
    return {&NativeTagRegistry::complete, argument};
}

void NativeTagRegistry::complete(void* argument) {
    auto* value = static_cast<CallbackState*>(argument);
    if (!value) throw std::invalid_argument("missing NATIVE callback state");
    auto* owner = value->owner;
    try {
        owner->finish(value->id);
    } catch (...) {
        owner->record_failure(std::current_exception());
        // Analytical network callbacks are noexcept. The run owner rethrows
        // this latch at its event-loop boundary instead of terminating here.
    }
}

void NativeTagRegistry::finish(uint64_t id) {
    auto found = callbacks_.find(id);
    require(found != callbacks_.end(), "duplicate NATIVE callback");
    auto value = std::move(found->second);
    callbacks_.erase(found);
    if (value->generation) {
        auto& lease = leases_.at(value->key);
        require(lease.lease.generation == value->generation && !lease.completed[value->send],
                "stale NATIVE completion");
        lease.completed[value->send] = true;
    } else {
        auto active = ordinary_.find(value->key);
        require(active != ordinary_.end() && active->second.count(value->bytes), "missing ordinary NATIVE occupancy");
        auto& messages = active->second.at(value->bytes);
        require(messages.completed[value->send] < messages.posted[value->send], "duplicate ordinary completion");
        ++messages.completed[value->send];
        // A completed send may still be waiting for a not-yet-posted receive.
        // The native matcher groups by (src, dst, normalized tag, count).
        if (messages.posted[0] == messages.posted[1] &&
            messages.completed[0] == messages.posted[0] && messages.completed[1] == messages.posted[1]) {
            active->second.erase(value->bytes);
            if (active->second.empty()) ordinary_.erase(active);
        }
    }
    value->original.function(value->original.argument);
}

bool NativeTagRegistry::drained() const { return leases_.empty() && callbacks_.empty() && ordinary_.empty(); }
void NativeTagRegistry::rethrow_failure() const { if (failure_) std::rethrow_exception(failure_); }
void NativeTagRegistry::record_failure(std::exception_ptr failure) {
    if (!failure_) failure_ = failure;
}
}  // namespace AstraSim
