/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#pragma once

#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/CallbackTracker.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include <astra-network-analytical/common/EventQueue.h>

namespace AstraSimAnalytical {

// Owns only live message contexts; all service queues belong to the original factory.
class PhysicalRouteTransport {
  public:
    using Key = CallbackTracker::Key;
    PhysicalRouteTransport(const std::string& path, const std::string& summary,
        AstraSim::PhysicalServiceFactory& services, const std::vector<AstraSim::Sys*>& systems,
        std::shared_ptr<NetworkAnalytical::EventQueue> events);
    ~PhysicalRouteTransport();
    void validate(int source, int target, uint64_t bytes) const;
    void submit(Key key, void (*arrival)(void*));
    bool drained() const;
    void rethrow_failure();
    void write_summary();

  private:
    struct Message;
    void advance(Message& message);
    void record(const AstraSim::TransportReceipt& receipt);
    static void continue_message(void* data);
    AstraSim::PhysicalServiceFactory& services_;
    const std::vector<AstraSim::Sys*>& systems_;
    std::shared_ptr<NetworkAnalytical::EventQueue> events_;
    std::map<std::pair<int, int>, std::vector<std::string>> routes_;
    std::map<uint64_t, std::unique_ptr<Message>> messages_;
    nlohmann::json summary_;
    std::string summary_path_;
    std::exception_ptr failure_;
    uint64_t next_id_ = 0;
    bool summary_written_ = false;
};
}  // namespace AstraSimAnalytical
