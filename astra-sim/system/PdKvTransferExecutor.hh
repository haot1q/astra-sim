/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#ifndef __PD_KV_TRANSFER_EXECUTOR_HH__
#define __PD_KV_TRANSFER_EXECUTOR_HH__

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "astra-sim/system/Callable.hh"

namespace AstraSim {

class Sys;

class PdKvTransferExecutor : public Callable {
  public:
    explicit PdKvTransferExecutor(const std::vector<Sys*>& systems);

    void submit(const std::string& descriptor_path, uint64_t ready_ns);
    void call(EventType event, CallData* data) override;
    bool drained() const;

  private:
    struct RankPair {
        int source_rank;
        int destination_rank;
        uint64_t bytes;
        int tag;
    };

    struct Transfer {
        std::string run_id;
        std::string attempt_id;
        std::string transfer_id;
        std::string request_id;
        std::string descriptor_digest;
        std::string transport;
        int source_instance_id;
        int destination_instance_id;
        uint64_t charged_bytes;
        uint64_t ready_ns;
        uint64_t start_ns;
        std::vector<RankPair> rank_pairs;
        std::vector<bool> send_complete;
        std::vector<bool> recv_complete;
    };

    void finish_if_complete(uint64_t transfer_index);

    std::vector<Sys*> systems_;
    std::map<uint64_t, std::unique_ptr<Transfer>> transfers_;
    uint64_t next_transfer_index_ = 0;
};

}  // namespace AstraSim

#endif  // __PD_KV_TRANSFER_EXECUTOR_HH__
