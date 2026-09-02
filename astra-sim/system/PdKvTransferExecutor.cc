/******************************************************************************
This source code is licensed under the MIT license found in the LICENSE file.
*******************************************************************************/

#include "astra-sim/system/PdKvTransferExecutor.hh"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>

#include "astra-sim/system/RecvPacketEventHandlerData.hh"
#include "astra-sim/system/SendPacketEventHandlerData.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "extern/helper/json/json.hpp"

namespace AstraSim {
namespace {

using json = nlohmann::json;

constexpr uint64_t kCallbackDirectionBit = uint64_t{1} << 63;

uint64_t require_uint(const json& value, const char* field) {
    if (!value.contains(field) || !value[field].is_number_unsigned()) {
        throw std::runtime_error(std::string("P/D descriptor requires unsigned ") + field);
    }
    return value[field].get<uint64_t>();
}

int require_int(const json& value, const char* field) {
    if (!value.contains(field) || !value[field].is_number_integer()) {
        throw std::runtime_error(std::string("P/D descriptor requires integer ") + field);
    }
    return value[field].get<int>();
}

std::string require_string(const json& value, const char* field) {
    if (!value.contains(field) || !value[field].is_string() ||
        value[field].get<std::string>().empty()) {
        throw std::runtime_error(std::string("P/D descriptor requires string ") + field);
    }
    return value[field].get<std::string>();
}

}  // namespace

PdKvTransferExecutor::PdKvTransferExecutor(const std::vector<Sys*>& systems)
    : systems_(systems) {}

void PdKvTransferExecutor::submit(const std::string& descriptor_path,
                                  uint64_t ready_ns) {
    std::ifstream stream(descriptor_path);
    if (!stream.good()) {
        throw std::runtime_error("cannot open P/D descriptor: " + descriptor_path);
    }
    json root;
    stream >> root;
    if (!root.is_object() || root.size() != 2 ||
        !root.contains("descriptor") || !root.contains("descriptor_digest")) {
        throw std::runtime_error("P/D sidecar fields do not match v1 contract");
    }
    const auto& descriptor = root["descriptor"];
    if (require_string(descriptor, "schema_version") != "pd-kv-transfer-v1") {
        throw std::runtime_error("unsupported P/D descriptor schema");
    }
    if (require_string(descriptor, "transport") != "astra_analytical_p2p_v1") {
        throw std::runtime_error("unsupported P/D transfer transport");
    }
    if (!descriptor.contains("rank_pairs") || !descriptor["rank_pairs"].is_array() ||
        descriptor["rank_pairs"].empty()) {
        throw std::runtime_error("P/D descriptor rank_pairs must be non-empty");
    }

    auto transfer = std::make_unique<Transfer>();
    transfer->run_id = require_string(descriptor, "run_id");
    transfer->attempt_id = require_string(descriptor, "attempt_id");
    transfer->transfer_id = require_string(descriptor, "transfer_id");
    transfer->request_id = require_string(descriptor, "request_id");
    transfer->descriptor_digest = require_string(root, "descriptor_digest");
    transfer->transport = require_string(descriptor, "transport");
    transfer->source_instance_id = require_int(descriptor, "source_instance_id");
    transfer->destination_instance_id = require_int(descriptor, "destination_instance_id");
    transfer->charged_bytes = require_uint(descriptor, "charged_bytes_aggregate");
    transfer->ready_ns = ready_ns;
    transfer->start_ns = std::max<uint64_t>(ready_ns, Sys::boostedTick());

    uint64_t bytes_sum = 0;
    std::set<int> tags;
    for (const auto& item : descriptor["rank_pairs"]) {
        RankPair pair{
            require_int(item, "source_rank"),
            require_int(item, "destination_rank"),
            require_uint(item, "charged_bytes"),
            require_int(item, "tag"),
        };
        if (pair.source_rank < 0 || pair.destination_rank < 0 ||
            pair.source_rank >= static_cast<int>(systems_.size()) ||
            pair.destination_rank >= static_cast<int>(systems_.size()) ||
            pair.bytes == 0 || !tags.insert(pair.tag).second) {
            throw std::runtime_error("invalid P/D rank pair");
        }
        bytes_sum += pair.bytes;
        transfer->rank_pairs.push_back(pair);
    }
    if (bytes_sum != transfer->charged_bytes) {
        throw std::runtime_error("P/D rank-pair bytes differ from aggregate");
    }

    const uint64_t transfer_index = next_transfer_index_++;
    transfer->send_complete.assign(transfer->rank_pairs.size(), false);
    transfer->recv_complete.assign(transfer->rank_pairs.size(), false);
    transfers_.emplace(transfer_index, std::move(transfer));
    auto& active = *transfers_.at(transfer_index);

    for (uint64_t pair_index = 0; pair_index < active.rank_pairs.size(); ++pair_index) {
        const auto& pair = active.rank_pairs[pair_index];
        auto* recv_data = new RecvPacketEventHandlerData;
        recv_data->callable = this;
        recv_data->wlhd = new WorkloadLayerHandlerData;
        recv_data->wlhd->node_id = (transfer_index << 32) | pair_index;
        recv_data->event = EventType::PacketReceived;
        sim_request recv_request;
        systems_[pair.destination_rank]->front_end_sim_recv(
            0, Sys::dummy_data, pair.bytes, UINT8, pair.source_rank, pair.tag,
            &recv_request, Sys::FrontEndSendRecvType::NATIVE,
            &Sys::handleEvent, recv_data);

        auto* send_data = new SendPacketEventHandlerData;
        send_data->callable = this;
        send_data->wlhd = new WorkloadLayerHandlerData;
        send_data->wlhd->node_id = kCallbackDirectionBit |
            (transfer_index << 32) | pair_index;
        send_data->event = EventType::PacketSent;
        sim_request send_request;
        systems_[pair.source_rank]->front_end_sim_send(
            0, Sys::dummy_data, pair.bytes, UINT8, pair.destination_rank, pair.tag,
            &send_request, Sys::FrontEndSendRecvType::NATIVE,
            &Sys::handleEvent, send_data);
    }
}

void PdKvTransferExecutor::call(EventType event, CallData* data) {
    auto* callback = static_cast<WorkloadLayerHandlerData*>(data);
    if (callback == nullptr) {
        throw std::runtime_error("P/D callback data is missing");
    }
    const uint64_t encoded = callback->node_id;
    const bool is_send = (encoded & kCallbackDirectionBit) != 0;
    const uint64_t transfer_index = (encoded & ~kCallbackDirectionBit) >> 32;
    const uint64_t pair_index = encoded & 0xffffffffULL;
    delete callback;

    auto found = transfers_.find(transfer_index);
    if (found == transfers_.end() || pair_index >= found->second->rank_pairs.size()) {
        throw std::runtime_error("stale P/D transfer callback");
    }
    if ((is_send && event != EventType::PacketSent) ||
        (!is_send && event != EventType::PacketReceived)) {
        throw std::runtime_error("P/D callback event type changed");
    }
    auto& completed = is_send ? found->second->send_complete
                              : found->second->recv_complete;
    if (completed[pair_index]) {
        throw std::runtime_error("duplicate P/D transfer callback");
    }
    completed[pair_index] = true;
    finish_if_complete(transfer_index);
}

void PdKvTransferExecutor::finish_if_complete(uint64_t transfer_index) {
    auto& transfer = *transfers_.at(transfer_index);
    const auto all_complete = [](const std::vector<bool>& values) {
        return std::all_of(values.begin(), values.end(), [](bool value) { return value; });
    };
    if (!all_complete(transfer.send_complete) ||
        !all_complete(transfer.recv_complete)) {
        return;
    }
    const uint64_t finish_ns = std::max<uint64_t>(transfer.start_ns, Sys::boostedTick());
    json receipt = {
        {"schema_version", "pd-kv-transfer-receipt-v1"},
        {"run_id", transfer.run_id},
        {"attempt_id", transfer.attempt_id},
        {"transfer_id", transfer.transfer_id},
        {"request_id", transfer.request_id},
        {"descriptor_digest", transfer.descriptor_digest},
        {"source_instance_id", transfer.source_instance_id},
        {"destination_instance_id", transfer.destination_instance_id},
        {"charged_bytes", transfer.charged_bytes},
        {"completed_rank_pairs", transfer.rank_pairs.size()},
        {"ready_ns", transfer.ready_ns},
        {"start_ns", transfer.start_ns},
        {"finish_ns", finish_ns},
        {"transport", transfer.transport},
        {"status", "success"},
    };
    std::cout << "PD_KV_TRANSFER_COMPLETE " << receipt.dump() << std::endl;
    transfers_.erase(transfer_index);
}

bool PdKvTransferExecutor::drained() const {
    return transfers_.empty();
}

}  // namespace AstraSim
