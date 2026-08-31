/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/system/memory/UcieTransport.hh"

#include "astra-sim/system/Callable.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "extern/graph_frontend/chakra/src/feeder/et_feeder_node.h"

#include <stdexcept>

namespace AstraSim {
namespace {

using Chakra::ETFeederNode;

const ChakraProtoMsg::AttributeProto& required_other_attr(
    const std::shared_ptr<ETFeederNode>& node, const std::string& name) {
    if (!node->has_other_attr(name)) {
        throw std::invalid_argument(
            "UCIe node is missing required attribute '" + name + "'");
    }
    return node->get_other_attr(name);
}

std::string required_string_attr(
    const std::shared_ptr<ETFeederNode>& node, const std::string& name) {
    const auto& attr = required_other_attr(node, name);
    if (!attr.has_string_val() || attr.string_val().empty()) {
        throw std::invalid_argument(
            "UCIe attribute '" + name + "' must be a non-empty string");
    }
    return attr.string_val();
}

struct IssuedHop {
    AstraMemoryAPI* api;
    MemoryOperation operation;
    uint64_t bytes;
    uint32_t device_id;
};

class UcieTransaction : public Callable {
  public:
    UcieTransaction(
        WorkloadLayerHandlerData* wlhd, std::vector<IssuedHop> hops)
        : wlhd_(wlhd),
          hops_(std::move(hops)),
          original_target_(wlhd->completion_target),
          index_(0) {
        if (hops_.empty()) {
            throw std::invalid_argument("UCIe transaction has no hops");
        }
        issue_current();
    }

    void call(EventType type, CallData* data) override {
        (void)type;
        (void)data;
        ++index_;
        if (index_ < hops_.size()) {
            issue_current();
            return;
        }
        wlhd_->completion_target = original_target_;
        Callable* target = original_target_ != nullptr
            ? original_target_
            : static_cast<Callable*>(wlhd_->workload);
        if (target == nullptr) {
            throw std::logic_error("UCIe transaction has no completion target");
        }
        target->call(EventType::General, wlhd_);
        delete this;
    }

  private:
    void issue_current() {
        const auto& hop = hops_[index_];
        wlhd_->completion_target = this;
        wlhd_->device_id = hop.device_id;
        hop.api->issue({hop.bytes, hop.operation}, wlhd_);
    }

    WorkloadLayerHandlerData* wlhd_;
    std::vector<IssuedHop> hops_;
    Callable* original_target_;
    std::size_t index_;
};

}  // namespace

bool node_requests_ucie_transport(const std::shared_ptr<ETFeederNode>& node) {
    const bool has_schema = node->has_other_attr(kUcieSchemaAttr);
    const bool has_link = node->has_other_attr(kUcieLinkIdAttr);
    if (has_schema != has_link) {
        throw std::invalid_argument(
            "UCIe attrs must be paired: schema_version and link_id");
    }
    return has_schema;
}

std::string ucie_link_id_attr(const std::shared_ptr<ETFeederNode>& node) {
    const auto schema = required_string_attr(node, kUcieSchemaAttr);
    if (schema != kUcieTransportSchema) {
        throw std::invalid_argument(
            "unsupported UCIe schema_version '" + schema + "'");
    }
    return required_string_attr(node, kUcieLinkIdAttr);
}

std::vector<UcieHopSpec> ucie_transaction_hops(
    MemoryOperation operation,
    uint64_t payload_bytes,
    uint64_t header_bytes) {
    if (operation != MemoryOperation::Read &&
        operation != MemoryOperation::Write) {
        throw std::invalid_argument("UCIe operation must be read or write");
    }
    std::vector<UcieHopSpec> hops;
    if (operation == MemoryOperation::Read) {
        if (header_bytes != 0) {
            hops.push_back({"read_request", MemoryOperation::Write, header_bytes});
        }
        if (payload_bytes != 0) {
            hops.push_back(
                {"read_response", MemoryOperation::Read, payload_bytes});
        }
    } else if (header_bytes + payload_bytes != 0) {
        hops.push_back(
            {"write_request", MemoryOperation::Write,
             header_bytes + payload_bytes});
    }
    if (hops.empty()) {
        throw std::invalid_argument(
            "UCIe transaction must move at least one header or payload byte");
    }
    return hops;
}

void issue_ucie_mem(
    Sys* sys,
    const std::shared_ptr<ETFeederNode>& node,
    WorkloadLayerHandlerData* wlhd,
    MemoryOperation operation) {
    if (sys == nullptr || wlhd == nullptr) {
        throw std::invalid_argument("UCIe issue requires sys and handler data");
    }
    const auto& link = sys->ucie_link(ucie_link_id_attr(node));
    const uint32_t device_id = node->tensor_device();
    if (device_id >= link.stack_count) {
        throw std::out_of_range(
            "UCIe link '" + link.id + "' device_id is out of range");
    }
    const uint64_t payload = node->tensor_size();
    if (payload == 0) {
        throw std::invalid_argument("UCIe HBM payload must be positive");
    }
    AstraMemoryAPI* hbm = sys->memory_api(node->tensor_loc(), device_id);
    std::vector<IssuedHop> hops;
    if (operation == MemoryOperation::Read) {
        if (link.header_bytes != 0) {
            hops.push_back(
                {link.api, MemoryOperation::Write, link.header_bytes, device_id});
        }
        hops.push_back({hbm, MemoryOperation::Read, payload, device_id});
        hops.push_back({link.api, MemoryOperation::Read, payload, device_id});
    } else {
        hops.push_back(
            {link.api, MemoryOperation::Write, link.header_bytes + payload,
             device_id});
        hops.push_back({hbm, MemoryOperation::Write, payload, device_id});
    }
    new UcieTransaction(wlhd, std::move(hops));
}

}  // namespace AstraSim
