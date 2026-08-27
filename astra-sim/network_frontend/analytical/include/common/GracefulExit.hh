/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#pragma once

#include <cstddef>

namespace AstraSimAnalytical {

class GracefulExit {
  public:
    explicit constexpr GracefulExit(std::size_t total_systems)
        : total_systems_(total_systems) {}

    constexpr void request() {
        requested_ = true;
    }

    [[nodiscard]] constexpr bool requested() const {
        return requested_;
    }

    [[nodiscard]] constexpr bool should_exit(
        std::size_t finished_systems) const {
        return requested_ && finished_systems == total_systems_;
    }

  private:
    std::size_t total_systems_;
    bool requested_ = false;
};

}  // namespace AstraSimAnalytical
