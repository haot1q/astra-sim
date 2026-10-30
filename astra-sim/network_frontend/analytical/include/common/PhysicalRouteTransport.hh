/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#pragma once

#include <cstdint>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/CallbackTracker.hh"
#include "astra-sim/system/memory/PhysicalServiceFactory.hh"
#include "astra-sim/system/memory/TransportPathAPI.hh"
#include <astra-network-analytical/common/EventQueue.h>

namespace AstraSimAnalytical {

// Owns only live path contexts; all service queues belong to the original factory.
class PhysicalRouteTransport : public AstraSim::TransportPathAPI {
  public:
    using Key = CallbackTracker::Key;
    PhysicalRouteTransport(const std::string& path, const std::string& summary,
        AstraSim::PhysicalServiceFactory& services, const std::vector<AstraSim::Sys*>& systems,
        std::shared_ptr<NetworkAnalytical::EventQueue> events);
    ~PhysicalRouteTransport();
    void validate(int source, int target, uint64_t bytes) const;
    void submit(Key key, void (*arrival)(void*));
    void submit_path(AstraSim::TransportPathRequest request) override;
    bool drained() const override;
    void rethrow_failure() override;
    void write_summary();
    void abort(std::exception_ptr failure);
    void validate_path(uint32_t rank, uint64_t bytes, const std::vector<std::string>& links) const override;

  private:
    struct Path;
    struct PipelineLimits {
        uint64_t block_bytes, message_blocks, message_hops, inflight_blocks, inflight_hops;
    };
    void check_budget(uint64_t blocks, uint64_t hops) const;
    uint64_t block_count(uint64_t bytes, size_t hops) const;
    void publish(std::vector<std::unique_ptr<Path>> paths);
    void submit_blocks(Key key, void (*arrival)(void*));
    void advance(Path& path);
    void record(const AstraSim::TransportReceipt& receipt);
    static void continue_path(void* data);
    AstraSim::PhysicalServiceFactory& services_;
    const std::vector<AstraSim::Sys*>& systems_;
    std::shared_ptr<NetworkAnalytical::EventQueue> events_;
    std::map<std::pair<int, int>, std::vector<std::string>> routes_;
    std::map<uint64_t, std::unique_ptr<Path>> paths_;
    nlohmann::json summary_;
    std::string summary_path_;
    std::exception_ptr failure_;
    uint64_t next_id_ = 0;
    uint64_t pending_messages_ = 0;
    bool summary_written_ = false;
    std::optional<PipelineLimits> pipeline_;
    uint64_t reserved_hops_ = 0;
};
}  // namespace AstraSimAnalytical
