/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#ifndef __PD_LOCAL_PREPARATION_EXECUTOR_HH__
#define __PD_LOCAL_PREPARATION_EXECUTOR_HH__

#include <cstddef>
#include <exception>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "MemoryMovementExecutor.hh"
#include "extern/helper/json/json.hpp"

namespace AstraSim {
class Sys;
class PhysicalServiceFactory;

class PdLocalPreparationExecutor : public MemoryPreparationOwner {
  public:
    explicit PdLocalPreparationExecutor(const std::vector<Sys*>& systems,
        PhysicalServiceFactory* services);
    bool submit_command(const std::string& command);
    void complete_memory_preparation(const MemoryPreparationReceipt& receipt) override;
    bool drained() const;
    std::size_t completed_count() const {
        return completed_count_;
    }
    void rethrow_failure() const;

  private:
    struct Submission {
        uint32_t rank;
        std::string logical_event_id;
        std::string physical_event_id;
        nlohmann::json identity;
        std::shared_ptr<Chakra::ETFeederNode> node;
    };
    std::vector<Submission> read_work(const std::string& path) const;
    void submit(const std::string& path);
    std::vector<Sys*> systems_;
    PhysicalServiceFactory* services_ = nullptr;
    std::map<std::string, Submission> pending_;
    std::set<std::string> used_event_ids_;
    std::set<std::string> used_preparations_;
    std::size_t completed_count_ = 0;
    std::exception_ptr failure_;
};

}  // namespace AstraSim
#endif
