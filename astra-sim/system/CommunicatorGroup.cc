/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/CommunicatorGroup.hh"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

#include "astra-sim/system/CollectivePlan.hh"
#include "astra-sim/system/Sys.hh"

using namespace AstraSim;

CommunicatorGroup::CommunicatorGroup(int id,
                                     std::vector<int> involved_NPUs,
                                     Sys* generator) {
    set_id(id);
    this->involved_NPUs = involved_NPUs;
    this->generator = generator;
    std::sort(involved_NPUs.begin(), involved_NPUs.end());
    assert(std::find(involved_NPUs.begin(), involved_NPUs.end(),
                     generator->id) != involved_NPUs.end());
}

CommunicatorGroup::~CommunicatorGroup() {
    for (auto cg : comm_plans) {
        CollectivePlan* cp = cg.second;
        delete cp;
    }
}

CommunicatorGroup::CommunicatorGroup(ScopeIdentity scope,
                                     std::vector<int> involved_NPUs)
    : involved_NPUs(std::move(involved_NPUs)), num_streams(0), id(0),
      generator(nullptr), scope_(scope) {
    scoped_stream_id(scope, 0);
    auto& members = this->involved_NPUs;
    std::sort(members.begin(), members.end());
    if (members.empty() || members.front() < 0 ||
        std::adjacent_find(members.begin(), members.end()) != members.end()) {
        throw std::invalid_argument("collective scope requires unique nonnegative members");
    }
}

bool CommunicatorGroup::identity_only() const {
    return scope_.has_value();
}

int CommunicatorGroup::scoped_stream_id(ScopeIdentity scope, uint64_t sequence) {
    constexpr uint64_t width = Sys::FrontEndSendRecvType::RENDEZVOUS -
                               Sys::FrontEndSendRecvType::COLLECTIVE;
    if (!scope.group_count || scope.group_count > width ||
        scope.group_index >= scope.group_count ||
        sequence > (width - 1 - scope.group_index) / scope.group_count) {
        throw std::invalid_argument("collective scope identity exhausted or invalid");
    }
    return static_cast<int>(scope.group_index + scope.group_count * sequence);
}

void CommunicatorGroup::require_streams(uint64_t count) const {
    if (!scope_) throw std::invalid_argument("collective scope preflight requires named mode");
    if (count == 0 || count - 1 > std::numeric_limits<uint64_t>::max() - scope_sequence_) {
        throw std::invalid_argument("collective scope stream count is invalid");
    }
    scoped_stream_id(*scope_, scope_sequence_ + count - 1);
}

int CommunicatorGroup::next_stream_id() {
    if (!scope_) return num_streams++;
    const int result = scoped_stream_id(*scope_, scope_sequence_);
    ++scope_sequence_;
    return result;
}

void CommunicatorGroup::set_id(int id) {
    assert(id > 0);
    this->id = id;
    this->num_streams = id * 1000000;
}

CollectivePlan* CommunicatorGroup::get_collective_plan(ComType comm_type) {
    if (identity_only()) {
        throw std::logic_error("identity-only collective scope cannot replace topology");
    }
    if (comm_plans.find(comm_type) != comm_plans.end()) {
        return comm_plans[comm_type];
    }

    if (static_cast<uint64_t>(generator->total_nodes) == involved_NPUs.size()) {
        LogicalTopology* logical_topology =
            generator->get_logical_topology(comm_type);
        std::vector<CollectiveImpl*> collective_implementation =
            generator->get_collective_implementation(comm_type);
        std::vector<bool> dimensions_involved(10, true);
        bool should_be_removed = false;
        comm_plans[comm_type] =
            new CollectivePlan(logical_topology, collective_implementation,
                               dimensions_involved, should_be_removed);
        return comm_plans[comm_type];
    } else {
        LogicalTopology* logical_topology = new RingTopology(
            RingTopology::Dimension::Local, generator->id, involved_NPUs);
        std::vector<CollectiveImpl*> collective_implementation{
            new CollectiveImpl(CollectiveImplType::Ring)};
        std::vector<bool> dimensions_involved(1, true);
        bool should_be_removed = true;
        comm_plans[comm_type] =
            new CollectivePlan(logical_topology, collective_implementation,
                               dimensions_involved, should_be_removed);
        return comm_plans[comm_type];
    }
    assert(false);
    return nullptr;
}
