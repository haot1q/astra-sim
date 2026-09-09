/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __PD_CACHE_PREPARATION_HH__
#define __PD_CACHE_PREPARATION_HH__

#include <exception>
#include <map>
#include <set>
#include <string>
#include <vector>
#include "PhysicalServiceFactory.hh"
#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"

namespace AstraSim {
class Sys;

// Child of the original preparation owner; no Cache tags or capacity ledger here.
class PdCachePreparation : public Callable {
  public:
    PdCachePreparation(const std::vector<Sys*>& systems, PhysicalServiceFactory& services,
                       const MemoryTierConfigSet& memory);
    void submit(const std::string& path);
    void cancel(const std::string& digest);
    void call(EventType event, CallData* data) override;
    bool drained() const { return !failure_ && work_.empty(); }
    void rethrow_failure() const;
    std::size_t completed_count() const { return completed_count_; }

  private:
    using Json = nlohmann::json;
    struct Work {
        Json document;
        Json identity;
        std::vector<uint32_t> ranks;
        uint32_t tier, device;
        std::string link;
        uint64_t ready, issued = 0, pending = 0, completed = 0;
        bool cancelled = false;
    };
    struct Callback : WorkloadLayerHandlerData {
        std::string digest;
        std::size_t item;
        bool start;
    };
    Work read_work(const std::string& path) const;
    void validate_location(const Json& work, Work& result) const;
    void start(Work& work, const std::string& digest);
    void complete(Work& work, const Callback& callback);
    void finish(const std::string& digest);
    std::vector<Sys*> systems_;
    PhysicalServiceFactory& services_;
    const MemoryTierConfigSet& memory_;
    std::map<std::string, Work> work_;
    std::set<std::string> used_;
    std::exception_ptr failure_;
    std::size_t completed_count_ = 0;
};
}  // namespace AstraSim
#endif
