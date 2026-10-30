/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __COMMUNICATOR_GROUP_HH__
#define __COMMUNICATOR_GROUP_HH__

#include <assert.h>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

#include "astra-sim/system/Common.hh"

namespace AstraSim {

class Sys;
class CollectivePlan;
class CommunicatorGroup {
  public:
    struct ScopeIdentity {
        uint64_t group_index;
        uint64_t group_count;
    };
    CommunicatorGroup(int id, std::vector<int> involved_NPUs, Sys* generator);
    CommunicatorGroup(ScopeIdentity scope, std::vector<int> involved_NPUs);
    bool identity_only() const;
    static int scoped_stream_id(ScopeIdentity scope, uint64_t sequence);
    void require_streams(uint64_t count) const;
    int next_stream_id();
    CollectivePlan* get_collective_plan(ComType comm_type);
    void set_id(int id);
    ~CommunicatorGroup();

    std::vector<int> involved_NPUs;
    int num_streams;

  private:
    int id;
    Sys* generator;
    std::map<ComType, CollectivePlan*> comm_plans;
    std::optional<ScopeIdentity> scope_;
    uint64_t scope_sequence_ = 0;
};

}  // namespace AstraSim

#endif /* __COMMUNICATOR_GROUP_HH__ */
