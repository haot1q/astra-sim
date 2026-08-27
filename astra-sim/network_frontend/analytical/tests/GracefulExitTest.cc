/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "common/GracefulExit.hh"

namespace {

constexpr bool contract_holds() {
    AstraSimAnalytical::GracefulExit state(4);

    if (state.requested() || state.should_exit(4)) {
        return false;
    }

    state.request();
    return state.requested() && !state.should_exit(0) &&
           !state.should_exit(3) && state.should_exit(4) &&
           !state.should_exit(5);
}

static_assert(contract_holds());

}  // namespace

int main() {
    return contract_holds() ? 0 : 1;
}
