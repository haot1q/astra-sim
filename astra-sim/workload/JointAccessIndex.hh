/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#ifndef __JOINT_ACCESS_INDEX_HH__
#define __JOINT_ACCESS_INDEX_HH__

#include <array>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "extern/helper/json/json.hpp"
#include "et_def.pb.h"

namespace AstraSim {
class MemoryTierRegistry;

struct JointAccessLimits {
    uint64_t context_bytes = 65536;
    uint64_t association_bytes = 67108864;
    uint64_t json_depth = 32;
    uint64_t string_bytes = 512;
    uint64_t table_records = 262144;
    uint64_t pieces = 1048576;
    uint64_t et_bytes = 268435456;
    uint64_t frame_bytes = 4194304;
    uint64_t et_nodes = 1000000;
    uint64_t pending_graphs_per_rank = 2;
    uint64_t live_index_charge_bytes = 268435456;
    uint64_t live_pieces = 1048576;
};

// One run owns admission charges; these are host limits, not simulated resources.
class JointSourceContext {
  public:
    static std::shared_ptr<JointSourceContext> load(const std::string& path);
    const JointAccessLimits& limits() const { return limits_; }
    const nlohmann::json& identity() const { return identity_; }
    bool admits_rank(uint32_t rank) const { return ranks_.count(rank) != 0; }
    std::size_t rank_count() const { return ranks_.size(); }
    uint64_t live_charge_bytes() const { return live_bytes_; }
    uint64_t live_piece_count() const { return live_pieces_; }

  private:
    friend class JointAccessIndex;
    void reserve(uint64_t bytes, uint64_t pieces);
    void release(uint64_t bytes, uint64_t pieces) noexcept;
    nlohmann::json identity_;
    JointAccessLimits limits_;
    std::set<uint32_t> ranks_;
    uint64_t live_bytes_ = 0;
    uint64_t live_pieces_ = 0;
};

struct JointNodeAccess {
    // Non-owning views remain valid until the owning feeder/index is retired.
    const nlohmann::json* companion;
    const nlohmann::json* binding;
    uint32_t tier_id;
    uint32_t device_id;
};

class JointAccessIndex {
  public:
    static std::unique_ptr<JointAccessIndex> load_and_validate(
        const std::string& path, std::shared_ptr<JointSourceContext> context,
        uint32_t rank, uint32_t iteration, const MemoryTierRegistry* tiers);
    // Legacy inputs keep their reader and limits; new metadata requires opt-in.
    static void probe_legacy(const std::string& path);
    static void reject_legacy_reference(const ChakraProtoMsg::Node& node);
    ~JointAccessIndex();
    JointAccessIndex(const JointAccessIndex&) = delete;
    JointAccessIndex& operator=(const JointAccessIndex&) = delete;
    const JointNodeAccess* lookup(const ChakraProtoMsg::Node& node,
                                 uint32_t rank, uint32_t iteration) const;
    std::size_t binding_count() const { return nodes_.size(); }
    void verify_file_identity(const std::string& path) const;

  private:
    JointAccessIndex() = default;
    void validate_business(const JointAccessLimits& limits);
    void validate_et(const std::string& path, const MemoryTierRegistry* tiers,
                     const std::string& companion_hash);
    nlohmann::json document_;
    std::shared_ptr<JointSourceContext> context_;
    std::unordered_map<uint64_t, JointNodeAccess> nodes_;
    uint32_t rank_ = 0;
    uint32_t iteration_ = 0;
    uint64_t charge_bytes_ = 0;
    uint64_t charge_pieces_ = 0;
    std::array<uint64_t, 7> et_identity_{};
};
}  // namespace AstraSim
#endif
