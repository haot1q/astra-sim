/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include <iostream>
#include <stdexcept>
#include "astra-sim/system/NativeTagRegistry.hh"

namespace {
void require(bool value) { if (!value) throw std::runtime_error("NATIVE tag test failed"); }
template <class Function> void rejects(Function function) {
    bool caught = false;
    try { function(); } catch (const std::invalid_argument&) { caught = true; }
    require(caught);
}
void completed(void* value) { ++*static_cast<int*>(value); }
}

int main() {
    using namespace AstraSim;
    try {
        NativeTagRegistry tags;
        int calls = 0;
        NativeCallback callback{completed, &calls};
        auto ordinary = tags.observe(0, 1, 1000000000, 8, true, 0, callback);
        rejects([&] { tags.acquire(0, 1, 8, 0); });
        auto lease = tags.acquire(0, 1, 8);
        require(lease.tag != 0);
        rejects([&] { tags.observe(0, 1, lease.tag, 8, false, 0, callback); });
        auto recv = tags.observe(0, 1, lease.tag, 8, false, lease.generation, callback);
        rejects([&] { tags.discard_unsubmitted(lease); });
        recv.function(recv.argument);
        rejects([&] { tags.release(lease); });
        auto send = tags.observe(0, 1, lease.tag, 8, true, lease.generation, callback);
        send.function(send.argument);
        tags.release(lease);
        rejects([&] { tags.release(lease); });
        ordinary.function(ordinary.argument);
        require(calls == 3 && !tags.drained());
        rejects([&] { tags.acquire(0, 1, 8, 0); });
        auto delayed_recv = tags.observe(0, 1, 0, 8, false, 0, callback);
        delayed_recv.function(delayed_recv.argument);
        require(calls == 4 && tags.drained());
        auto reused = tags.acquire(0, 1, 8, lease.tag);
        require(reused.generation != lease.generation);
        rejects([&] { tags.observe(0, 1, lease.tag, 8, true, lease.generation, callback); });
        tags.discard_unsubmitted(reused);
        require(tags.drained());
        std::cout << "PASS normalized NATIVE tag lifecycle" << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
