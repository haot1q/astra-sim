/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __TRANSPORT_PATH_API_HH__
#define __TRANSPORT_PATH_API_HH__

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace AstraSim {
struct TransportReceipt;

struct TransportPathTiming {
    uint64_t first_ready_ns, first_start_ns, finish_ns;
    uint64_t total_wait_ns, total_service_ns;
};

// The caller validates the full H1 path before projecting its charged links.
// Submission takes ownership of these values/callbacks; captured consumers must
// outlive completion. Observers see only completed real services, never estimates.
struct TransportPathRequest {
    uint32_t rank;
    uint64_t bytes;
    std::vector<std::string> service_link_ids;
    std::function<void(const TransportPathTiming&)> complete;
    std::function<void(const TransportReceipt&)> observe_hop;
};

// No queue, clock or capacity owner. Failure is terminal for this path executor;
// its owning frontend must drain callbacks and check rethrow_failure before exit.
class TransportPathAPI {
  public:
    virtual ~TransportPathAPI() = default;
    virtual void validate_path(uint32_t rank, uint64_t bytes,
                               const std::vector<std::string>& links) const = 0;
    virtual void submit_path(TransportPathRequest request) = 0;
    virtual bool drained() const = 0;
    virtual void rethrow_failure() = 0;
};
}  // namespace AstraSim
#endif
