/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/workload/Workload.hh"

#include "astra-sim/common/Logging.hh"
#include "astra-sim/system/IntData.hh"
#include "astra-sim/system/MemEventHandlerData.hh"
#include "astra-sim/system/RecvPacketEventHandlerData.hh"
#include "astra-sim/system/SendPacketEventHandlerData.hh"
#include "astra-sim/system/WorkloadLayerHandlerData.hh"
#include "astra-sim/system/AstraMemoryAPI.hh"
#include "astra-sim/system/memory/MemoryMovementExecutor.hh"
#include <json/json.hpp>

#include <algorithm>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

using namespace std;
using namespace AstraSim;
using namespace Chakra;
using json = nlohmann::json;
using AstraSim::MemoryLocationType;

typedef ChakraProtoMsg::NodeType ChakraNodeType;
typedef ChakraProtoMsg::CollectiveCommType ChakraCollectiveCommType;

MemoryOperation AstraSim::memory_operation_for_node_type(
    ChakraProtoMsg::NodeType node_type) {
    if (node_type == ChakraNodeType::MEM_LOAD_NODE ||
        node_type == ChakraNodeType::PIM_COMP_NODE) {
        return MemoryOperation::Read;
    }
    if (node_type == ChakraNodeType::MEM_STORE_NODE) {
        return MemoryOperation::Write;
    }
    throw invalid_argument("node type is not a supported memory operation");
}

Workload::Workload(Sys* sys, string et_filename, string comm_group_filename) {
    string workload_filename = et_filename + "." + to_string(sys->id) + ".et";
    // Check if workload filename exists
    if (access(workload_filename.c_str(), R_OK) < 0) {
        string error_msg;
        if (errno == ENOENT) {
            error_msg =
                "workload file: " + workload_filename + " does not exist";
        } else if (errno == EACCES) {
            error_msg = "workload file: " + workload_filename +
                        " exists but is not readable";
        } else {
            error_msg =
                "Unknown workload file: " + workload_filename + " access error";
        }
        LoggerFactory::get_logger("workload")->critical(error_msg);
        exit(EXIT_FAILURE);
    }
    this->sys = sys;
    this->et_feeder = load_et_feeder(workload_filename);
    this->comm_group = nullptr;
    this->hw_resource = new HardwareResource(1);
    initialize_comm_group(comm_group_filename);
    this->is_finished = false;
    this->iteration = 0;
    this->filename = et_filename;
    this->is_sleep = false;
}

ETFeeder* Workload::load_et_feeder(const string& workload_filename) {
    auto* feeder = new ETFeeder(workload_filename);
    try {
        sys->validate_tier_manifest_digest(feeder->tierManifestDigest());
    } catch (...) {
        delete feeder;
        throw;
    }
    return feeder;
}

Workload::~Workload() {
    if (this->comm_group != nullptr) {
        delete this->comm_group;
    }
    if (this->et_feeder != nullptr) {
        delete this->et_feeder;
    }
    if (this->hw_resource != nullptr) {
        delete this->hw_resource;
    }
}

void Workload::initialize_comm_group(string comm_group_filename) {
    // communicator group input file is not given
    if (comm_group_filename.find("empty") != std::string::npos) {
        comm_group = nullptr;
        return;
    }

    ifstream inFile;
    json j;
    inFile.open(comm_group_filename);
    inFile >> j;

    for (json::iterator it = j.begin(); it != j.end(); ++it) {
        bool in_comm_group = false;

        for (auto id : it.value()) {
            if (id == sys->id) {
                in_comm_group = true;
            }
        }

        if (in_comm_group) {
            std::vector<int> involved_NPUs;
            for (auto id : it.value()) {
                involved_NPUs.push_back(id);
            }
            comm_group = new CommunicatorGroup(1, involved_NPUs, sys);
            // Note: All NPUs should create comm group with identical ids if
            // they want to communicate with each other
        }
    }
}

void Workload::issue_dep_free_nodes() {
    std::queue<shared_ptr<Chakra::ETFeederNode>> push_back_queue;
    shared_ptr<Chakra::ETFeederNode> node = et_feeder->getNextIssuableNode();
    while (node != nullptr) {
        if (hw_resource->is_available(node)) {
            issue(node);
        } else {
            push_back_queue.push(node);
        }
        node = et_feeder->getNextIssuableNode();
    }

    while (!push_back_queue.empty()) {
        shared_ptr<Chakra::ETFeederNode> node = push_back_queue.front();
        et_feeder->pushBackIssuableNode(node->id());
        push_back_queue.pop();
    }
    sys->memory_movement_executor->dispatch();
}

void Workload::issue(shared_ptr<Chakra::ETFeederNode> node) {
    auto logger = LoggerFactory::get_logger("workload");
    if (sys->memory_movement_executor->is_movement_node(node)) {
        issue_memory_movement(node);
        return;
    }
    if (sys->replay_only) {
        hw_resource->occupy(node);
        issue_replay(node);
    } else {
        if ((node->type() == ChakraNodeType::MEM_LOAD_NODE) ||
            (node->type() == ChakraNodeType::MEM_STORE_NODE) ||
            (node->type() == ChakraNodeType::PIM_COMP_NODE)) {
            if (sys->trace_enabled) {
                logger->debug("issue,sys->id={}, tick={}, node->id={}, "
                              "node->name={}, node->type={}",
                              sys->id, Sys::boostedTick(), node->id(),
                              node->name(),
                              static_cast<uint64_t>(node->type()));
            }
            issue_mem(node);
        } else if (node->is_cpu_op() ||
                   (!node->is_cpu_op() &&
                    node->type() == ChakraNodeType::COMP_NODE)) {
            if ((node->runtime() == 0) && (node->num_ops() == 0)) {
                skip_invalid(node);
            } else {
                if (sys->trace_enabled) {
                    logger->debug("issue,sys->id={}, tick={}, node->id={}, "
                                  "node->name={}, node->type={}",
                                  sys->id, Sys::boostedTick(), node->id(),
                                  node->name(),
                                  static_cast<uint64_t>(node->type()));
                }
                issue_comp(node);
            }
        } else if (!node->is_cpu_op() &&
                   (node->type() == ChakraNodeType::COMM_COLL_NODE ||
                    (node->type() == ChakraNodeType::COMM_SEND_NODE) ||
                    (node->type() == ChakraNodeType::COMM_RECV_NODE))) {
            if (sys->trace_enabled) {
                if (sys->trace_enabled) {
                    logger->debug("issue,sys->id={}, tick={}, node->id={}, "
                                  "node->name={}, node->type={}",
                                  sys->id, Sys::boostedTick(), node->id(),
                                  node->name(),
                                  static_cast<uint64_t>(node->type()));
                }
            }
            issue_comm(node);
        } else if (node->type() == ChakraNodeType::INVALID_NODE) {
            skip_invalid(node);
        }
    }
}

void Workload::issue_memory_movement(
    shared_ptr<Chakra::ETFeederNode> node) {
    try {
        const bool foreground =
            sys->memory_movement_executor->submit(node, this);
        if (!foreground) {
            et_feeder->freeChildrenNodes(node->id());
            et_feeder->removeNode(node->id());
        }
    } catch (const std::exception& error) {
        LoggerFactory::get_logger("workload")->critical(
            "Memory movement dispatch failed for node {}: {}",
            node->id(), error.what());
        exit(EXIT_FAILURE);
    }
}

void Workload::complete_memory_movement(uint64_t node_id) {
    auto* data = new WorkloadLayerHandlerData;
    data->node_id = node_id;
    call(EventType::General, data);
}

optional<uint64_t> Workload::movement_exposed_to_dependent_ns(
    uint64_t node_id,
    uint64_t ready_ns,
    uint64_t finish_ns) {
    const auto movement = et_feeder->lookupNode(node_id);
    bool has_compute_consumer = false;
    uint64_t maximum_exposure_ns = 0;
    for (const auto& child : movement->getChildren()) {
        if (child->type() != ChakraNodeType::COMP_NODE || child->is_cpu_op()) {
            continue;
        }
        has_compute_consumer = true;
        uint64_t baseline_ns = ready_ns;
        const auto previous = latest_parent_completion_ns_.find(child->id());
        if (previous != latest_parent_completion_ns_.end()) {
            baseline_ns = max(baseline_ns, previous->second);
        }
        if (finish_ns < baseline_ns) {
            throw logic_error("movement exposure baseline exceeds finish time");
        }
        maximum_exposure_ns =
            max(maximum_exposure_ns, finish_ns - baseline_ns);
    }
    return has_compute_consumer ? optional<uint64_t>(maximum_exposure_ns)
                                : nullopt;
}

void Workload::record_parent_completion(
    const shared_ptr<Chakra::ETFeederNode>& node) {
    const uint64_t completion_ns = static_cast<uint64_t>(Sys::boostedTick());
    for (const auto& child : node->getChildren()) {
        auto& latest = latest_parent_completion_ns_[child->id()];
        latest = max(latest, completion_ns);
    }
}

void Workload::reset_iteration_tracking() {
    latest_parent_completion_ns_.clear();
}

void Workload::issue_replay(shared_ptr<Chakra::ETFeederNode> node) {
    WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
    wlhd->node_id = node->id();
    uint64_t runtime = 1ul;
    if (node->runtime() != 0ul) {
        // chakra runtimes are in microseconds and we should convert it into
        // nanoseconds
        // -> changed chakra to return nanoseconds
        runtime = node->runtime(); // * 1000;
    }
    if (node->is_cpu_op()) {
        hw_resource->tics_cpu_ops += runtime;
    } else {
        hw_resource->tics_gpu_ops += runtime;
    }
    sys->register_event(this, EventType::General, wlhd, runtime);
}

/*
    Now the node only stores one tensor_size for MEM_LOAD/STORE
    Can get tensor size using
    - node->tensor_size()
    Loading/storing of the input/output is hardcoded in chakra
    But, still can get input types from the node name
    - node->name().find("INPUT") != string::npos
    - node->name().find("OUTPUT") != string::npos
    - node->name().find("WEIGHT") != string::npos

    For the tensor location use tensor_loc()
    - node->tensor_loc()
    INVALID_MEMORY = 0, LOCAL_MEMORY = 1, REMOTE_MEMORY = 2, CXL_MEMORY = 3 STORAGE_MEMORY = 4
*/
void Workload::issue_mem(shared_ptr<Chakra::ETFeederNode> node) {
    hw_resource->occupy(node);

    WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
    wlhd->sys_id = sys->id;
    wlhd->workload = this;
    wlhd->node_id = node->id();
    wlhd->device_id = node->tensor_device();
    if (node->type() == ChakraNodeType::PIM_COMP_NODE) { // pim implementation
        wlhd->pim_enabled = true;
        wlhd->pim_channel_id = node->tensor_channel();
        wlhd->pim_runtime = node->runtime();
    }
    MemoryOperation operation;
    try {
        operation = memory_operation_for_node_type(node->type());
    } catch (const invalid_argument& error) {
        LoggerFactory::get_logger("workload")->critical(
            "Unsupported memory node type {} for workload node {}: {}",
            static_cast<uint64_t>(node->type()), node->id(), error.what());
        delete wlhd;
        exit(EXIT_FAILURE);
    }
    try {
        sys->memory_api(node->tensor_loc(), node->tensor_device())
            ->issue({node->tensor_size(), operation}, wlhd);
    } catch (const std::exception& error) {
        LoggerFactory::get_logger("workload")->critical(
            "Memory dispatch failed for workload node {}, tier_id {}, "
            "device_id {}, operation {}: {}",
            node->id(), node->tensor_loc(), node->tensor_device(),
            operation == MemoryOperation::Read ? "read" : "write",
            error.what());
        delete wlhd;
        exit(EXIT_FAILURE);
    }
}

void Workload::issue_comp(shared_ptr<Chakra::ETFeederNode> node) {
    hw_resource->occupy(node);
    if (!node->is_cpu_op()) {
        sys->memory_movement_executor->record_compute_start(node->id());
    }

    if (sys->roofline_enabled) {
        WorkloadLayerHandlerData* wlhd = new WorkloadLayerHandlerData;
        wlhd->node_id = node->id();

        double operational_intensity = static_cast<double>(node->num_ops()) /
                                       static_cast<double>(node->tensor_size());
        double perf = sys->roofline->get_perf(operational_intensity);
        double elapsed_time =
            static_cast<double>(node->num_ops()) / perf;  // sec
        uint64_t runtime =
            static_cast<uint64_t>(elapsed_time * 1e9);  // sec -> ns
        if (node->is_cpu_op()) {
            hw_resource->tics_cpu_ops += runtime;
        } else {
            hw_resource->tics_gpu_ops += runtime;
        }
        sys->register_event(this, EventType::General, wlhd, runtime);
    } else {
        // advance this node forward the recorded "replayed" time specificed in
        // the ET.
        issue_replay(node);
    }
}

void Workload::issue_comm(shared_ptr<Chakra::ETFeederNode> node) {
    hw_resource->occupy(node);

    vector<bool> involved_dim;

    if (node->has_other_attr("involved_dim")) {
        const ChakraProtoMsg::AttributeProto& attr =
            node->get_other_attr("involved_dim");

        // Ensure the attribute is of type bool_list before accessing
        if (attr.has_bool_list()) {
            const ChakraProtoMsg::BoolList& bool_list = attr.bool_list();

            // Traverse bool_list and add values to involved_dim
            for (int i = 0; i < bool_list.values_size(); ++i) {
                involved_dim.push_back(bool_list.values(i));
            }
        } else {
            cerr << "Expected bool_list in involved_dim but found another type."
                 << endl;
            exit(EXIT_FAILURE);
        }
    } else {
        // involved_dim does not exist in ETFeeder.
        // Assume involved_dim = [1,1,1,1,1] which we could simulate 5-Dimension.
	// Could use Process Group to build involved_dim later.
	// Once process group is implemented, you should get
        // that with node->pg_name()

	for(int i = 0; i < 4; i++)
            involved_dim.push_back(true);
    }

    if (!node->is_cpu_op() &&
        (node->type() == ChakraNodeType::COMM_COLL_NODE)) {
        if (node->comm_type() == ChakraCollectiveCommType::ALL_REDUCE) {
            DataSet* fp =
                sys->generate_all_reduce(node->comm_size(), involved_dim,
                                         comm_group, node->comm_priority());
            collective_comm_node_id_map[fp->my_id] = node->id();
            collective_comm_wrapper_map[fp->my_id] = fp;
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);

        } else if (node->comm_type() == ChakraCollectiveCommType::ALL_TO_ALL) {
            DataSet* fp =
                sys->generate_all_to_all(node->comm_size(), involved_dim,
                                         comm_group, node->comm_priority());
            collective_comm_node_id_map[fp->my_id] = node->id();
            collective_comm_wrapper_map[fp->my_id] = fp;
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);

        } else if (node->comm_type() == ChakraCollectiveCommType::ALL_GATHER) {
            DataSet* fp =
                sys->generate_all_gather(node->comm_size(), involved_dim,
                                         comm_group, node->comm_priority());
            collective_comm_node_id_map[fp->my_id] = node->id();
            collective_comm_wrapper_map[fp->my_id] = fp;
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);

        } else if (node->comm_type() ==
                   ChakraCollectiveCommType::REDUCE_SCATTER) {
            DataSet* fp =
                sys->generate_reduce_scatter(node->comm_size(), involved_dim,
                                             comm_group, node->comm_priority());
            collective_comm_node_id_map[fp->my_id] = node->id();
            collective_comm_wrapper_map[fp->my_id] = fp;
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);

        } else if (node->comm_type() == ChakraCollectiveCommType::BROADCAST) {
            // broadcast colelctive has not been implemented in ASTRA-SIM yet.
            // So, we just use its real system mesurements
            uint64_t runtime = 1ul;
            if (node->runtime() != 0ul) {
                // chakra runtimes are in microseconds and we should convert it
                // into nanoseconds
                // -> changed chakra to return nanoseconds
                runtime = node->runtime(); // * 1000;
            }
            DataSet* fp = new DataSet(1);
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
            collective_comm_node_id_map[fp->my_id] = node->id();
            collective_comm_wrapper_map[fp->my_id] = fp;
            sys->register_event(fp, EventType::General, nullptr,
                                // chakra runtimes are in microseconds and we
                                // should convert it into nanoseconds
                                runtime);
            fp->set_notifier(this, EventType::CollectiveCommunicationFinished);
        }
    } else if (node->type() == ChakraNodeType::COMM_SEND_NODE) {
        sim_request snd_req;
        snd_req.srcRank = node->comm_src();
        snd_req.dstRank = node->comm_dst();
        snd_req.reqType = UINT8;
        SendPacketEventHandlerData* sehd = new SendPacketEventHandlerData;
        sehd->callable = this;
        sehd->wlhd = new WorkloadLayerHandlerData;
        sehd->wlhd->node_id = node->id();
        sehd->event = EventType::PacketSent;
        sys->front_end_sim_send(0, Sys::dummy_data, node->comm_size(), UINT8,
                                node->comm_dst(), node->comm_tag(), &snd_req,
                                Sys::FrontEndSendRecvType::NATIVE,
                                &Sys::handleEvent, sehd);
    } else if (node->type() == ChakraNodeType::COMM_RECV_NODE) {
        sim_request rcv_req;
        RecvPacketEventHandlerData* rcehd = new RecvPacketEventHandlerData;
        rcehd->wlhd = new WorkloadLayerHandlerData;
        rcehd->wlhd->node_id = node->id();
        rcehd->workload = this;
        rcehd->event = EventType::PacketReceived;
        sys->front_end_sim_recv(0, Sys::dummy_data, node->comm_size(), UINT8,
                                node->comm_src(), node->comm_tag(), &rcv_req,
                                Sys::FrontEndSendRecvType::NATIVE,
                                &Sys::handleEvent, rcehd);
    } else {
        LoggerFactory::get_logger("workload")
            ->critical("Unknown communication node type");
        exit(EXIT_FAILURE);
    }
}

void Workload::skip_invalid(shared_ptr<Chakra::ETFeederNode> node) {
    record_parent_completion(node);
    et_feeder->freeChildrenNodes(node->id());
    et_feeder->removeNode(node->id());
}

void Workload::call(EventType event, CallData* data) {
    if (is_finished) {
        return;
    }

    if (event == EventType::CollectiveCommunicationFinished) {
        IntData* int_data = (IntData*)data;
        hw_resource->tics_gpu_comms += int_data->execution_time;
        uint64_t node_id = collective_comm_node_id_map[int_data->data];
        shared_ptr<Chakra::ETFeederNode> node = et_feeder->lookupNode(node_id);

        if (sys->trace_enabled) {
            LoggerFactory::get_logger("workload")
                ->debug("callback,sys->id={}, tick={}, node->id={}, "
                        "node->name={}, node->type={}",
                        sys->id, Sys::boostedTick(), node->id(), node->name(),
                        static_cast<uint64_t>(node->type()));
        }

        hw_resource->release(node);

        record_parent_completion(node);
        et_feeder->freeChildrenNodes(node_id);

        issue_dep_free_nodes();

        // The Dataset class provides statistics that should be used later to dump
        // more statistics in the workload layer
        delete collective_comm_wrapper_map[int_data->data];
        collective_comm_wrapper_map.erase(int_data->data);
        et_feeder->removeNode(node_id);

    } else {
        if (data == nullptr) {
            issue_dep_free_nodes();
        } else {
            WorkloadLayerHandlerData* wlhd = (WorkloadLayerHandlerData*)data;
            shared_ptr<Chakra::ETFeederNode> node =
                et_feeder->lookupNode(wlhd->node_id);

            if (node->type() == ChakraNodeType::COMP_NODE &&
                !node->is_cpu_op()) {
                sys->memory_movement_executor->record_compute_finish(node->id());
            }
            record_parent_completion(node);

            if (sys->trace_enabled) {
                LoggerFactory::get_logger("workload")
                    ->debug("callback,sys->id={}, tick={}, node->id={}, "
                            "node->name={}, node->type={}",
                            sys->id, Sys::boostedTick(), node->id(),
                            node->name(), static_cast<uint64_t>(node->type()));
            }

            hw_resource->release(node);

            et_feeder->freeChildrenNodes(node->id());

            issue_dep_free_nodes();

            et_feeder->removeNode(wlhd->node_id);
            delete wlhd;
        }
    }

    if (!et_feeder->hasNodesToIssue() &&
        (hw_resource->num_in_flight_cpu_ops == 0) &&
        (hw_resource->num_in_flight_gpu_comp_ops == 0) &&
        (hw_resource->num_in_flight_gpu_comm_ops == 0) ) {
        // report();
        if (!pending_workloads.empty()) {
            string next_workload = pending_workloads.front();
            pending_workloads.pop();
            // there exists new workload, change the ETFeeder
            if (this->et_feeder != nullptr)
                delete this->et_feeder;
            this->et_feeder = load_et_feeder(next_workload);
            reset_iteration_tracking();
            iteration++;
            is_finished = false;
            // fire next iteration
            fire();
        } else {
            // wait for the new workload command
            // should be fired by parent controller
            sys->comm_NI->sim_notify_finished();
            is_finished = true;
        }
    }
}

void Workload::add_workload(const std::string& new_filename,
                            const std::vector<Sys*>& systems) {

    // add new workload filename to pending workloads in managed systems
    for (auto* managed_sys : systems) {
        if (managed_sys == nullptr || managed_sys->workload == nullptr) {
            LoggerFactory::get_logger("workload")
                ->critical("Null system or workload while adding workload {}", new_filename);
        }
        string workload_filename = new_filename + "." + to_string(managed_sys->id) + ".et";
        // Check if workload filename exists
        if (access(workload_filename.c_str(), R_OK) < 0) {
            string error_msg;
            if (errno == ENOENT) {
                error_msg = "workload file: " + workload_filename + " does not exist";
            } else if (errno == EACCES) {
                error_msg = "workload file: " + workload_filename + " exists but is not readable";
            } else {
                error_msg = "Unknown workload file: " + workload_filename + " access error";
            }
            LoggerFactory::get_logger("workload")->critical(error_msg);
            return;
        }
        if (managed_sys->workload->is_finished && managed_sys->workload->pending_workloads.empty()) {
            // if the workload is finished, we can directly change the ETFeeder
            if (managed_sys->workload->et_feeder != nullptr)
                delete managed_sys->workload->et_feeder;
            managed_sys->workload->et_feeder =
                managed_sys->workload->load_et_feeder(workload_filename);
            managed_sys->workload->reset_iteration_tracking();
            managed_sys->workload->iteration++;
            managed_sys->workload->is_finished = false;
            // fire next iteration
            managed_sys->workload->fire();
            continue;
        }
        else{
            managed_sys->workload->pending_workloads.push(workload_filename);
        }
    }
    string workload_filename = new_filename + "." + to_string(sys->id) + ".et";
    // cout << workload_filename << endl;

    // Check if workload filename exists
    if (access(workload_filename.c_str(), R_OK) < 0) {
        string error_msg;
        if (errno == ENOENT) {
            error_msg = "workload file: " + workload_filename + " does not exist";
        } else if (errno == EACCES) {
            error_msg = "workload file: " + workload_filename + " exists but is not readable";
        } else {
            error_msg = "Unknown workload file: " + workload_filename + " access error";
        }
        LoggerFactory::get_logger("workload")->critical(error_msg);
        return;
    }

    // there exists new workload, change the ETFeeder
    if (this->et_feeder != nullptr)
      delete this->et_feeder;
    this->et_feeder = load_et_feeder(workload_filename);
    reset_iteration_tracking();
    iteration++;
    is_finished = false;
    // fire next iteration
    fire();
}

void Workload::sleep_workload(const std::vector<Sys*>& systems) {

    // add new workload filename to pending workloads in managed systems
    for (auto* managed_sys : systems) {
        if (managed_sys == nullptr || managed_sys->workload == nullptr) {
            LoggerFactory::get_logger("workload")
                ->critical("Null system or workload while sleeping workload");
        }
        // make managed systems sleep
        managed_sys->workload->is_sleep = true;
    }
    // sleep myself
    is_sleep = true;
}

void Workload::fire() {
    call(EventType::General, NULL);
}

void Workload::report() {
    Tick curr_tick = Sys::boostedTick();
    LoggerFactory::get_logger("workload")
        ->info("sys[{}] iteration {} finished, {} cycles, exposed communication {} cycles.",
               sys->id, iteration, curr_tick, curr_tick - hw_resource->tics_gpu_ops);
}
