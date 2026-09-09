/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#ifndef __NATIVE_TAG_REGISTRY_HH__
#define __NATIVE_TAG_REGISTRY_HH__

#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <tuple>

namespace AstraSim {

struct NativeTagLease {
    uint64_t generation;
    int source;
    int destination;
    int tag;
};

struct NativeCallback {
    void (*function)(void*);
    void* argument;
};

// One run-owned registry observes ordinary NATIVE traffic as well as P/D leases.
// It owns callback wrappers, not payload, Sys, or transfer lifetime.
class NativeTagRegistry {
  public:
    static constexpr int kTagCount = 500000000;
    static int normalize(int tag);
    NativeTagLease acquire(int source, int destination, uint64_t bytes,
                           std::optional<int> preferred = std::nullopt);
    void release(const NativeTagLease& lease);
    void discard_unsubmitted(const NativeTagLease& lease);
    NativeCallback observe(int source, int destination, int tag, uint64_t bytes,
                           bool send, uint64_t generation, NativeCallback callback);
    bool drained() const;
    void rethrow_failure() const;
    void record_failure(std::exception_ptr failure);

  private:
    using Key = std::tuple<int, int, int>;
    struct LeaseState {
        NativeTagLease lease;
        uint64_t bytes;
        bool posted[2] = {false, false};
        bool completed[2] = {false, false};
    };
    struct CallbackState {
        NativeTagRegistry* owner;
        uint64_t id;
        Key key;
        bool send;
        uint64_t generation;
        uint64_t bytes;
        NativeCallback original;
    };
    struct OrdinaryMessages {
        uint64_t posted[2] = {0, 0};
        uint64_t completed[2] = {0, 0};
    };
    static void complete(void* argument);
    void finish(uint64_t id);
    LeaseState& state(const NativeTagLease& lease);
    std::map<Key, LeaseState> leases_;
    std::map<Key, std::map<uint64_t, OrdinaryMessages>> ordinary_;
    std::map<uint64_t, std::unique_ptr<CallbackState>> callbacks_;
    uint64_t next_generation_ = 1;
    uint64_t next_callback_ = 1;
    int cursor_ = 0;
    std::exception_ptr failure_;
};

}  // namespace AstraSim
#endif
