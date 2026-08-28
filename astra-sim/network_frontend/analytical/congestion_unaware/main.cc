/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/

#include "astra-sim/common/Logging.hh"
#include "common/CmdLineParser.hh"
#include "common/GracefulExit.hh"
#include "congestion_unaware/CongestionUnawareNetworkApi.hh"
#include <astra-network-analytical/common/EventQueue.h>
#include <astra-network-analytical/common/NetworkParser.h>
#include <astra-network-analytical/congestion_unaware/Helper.h>
#include <memory_backend/analytical/AnalyticalMemory.hh>
#include "astra-sim/system/MemoryTierConfig.hh"
#include <algorithm>
#include <iostream>

using namespace AstraSim;
using namespace Analytical;
using namespace AstraSimAnalytical;
using namespace AstraSimAnalyticalCongestionUnaware;
using namespace NetworkAnalytical;
using namespace NetworkAnalyticalCongestionUnaware;
using namespace std;
int main(int argc, char* argv[]) {
    // Parse command line arguments
    auto cmd_line_parser = CmdLineParser(argv[0]);
    cmd_line_parser.parse(argc, argv);

    // Get command line arguments
    const auto workload_configuration =
        cmd_line_parser.get<std::string>("workload-configuration");
    const auto comm_group_configuration =
        cmd_line_parser.get<std::string>("comm-group-configuration");
    const auto system_configuration =
        cmd_line_parser.get<std::string>("system-configuration");
    const auto memory_configuration =
        cmd_line_parser.get<std::string>("memory-configuration");
    const auto network_configuration =
        cmd_line_parser.get<std::string>("network-configuration");
    const auto logging_configuration =
        cmd_line_parser.get<std::string>("logging-configuration");
    const auto num_queues_per_dim =
        cmd_line_parser.get<int>("num-queues-per-dim");
    const auto comm_scale = cmd_line_parser.get<double>("comm-scale");
    const auto injection_scale = cmd_line_parser.get<double>("injection-scale");
    const auto rendezvous_protocol =
        cmd_line_parser.get<bool>("rendezvous-protocol");
    auto start_npu_ids =
        cmd_line_parser.get<std::vector<int>>("start-npu-ids");
    auto end_npu_ids =
        cmd_line_parser.get<std::vector<int>>("end-npu-ids");

    // clear vector if default value is used
    if (start_npu_ids.size() == 1 && start_npu_ids[0] == -1) {
      start_npu_ids.clear();
    }
    if (end_npu_ids.size() == 1 && end_npu_ids[0] == -1) {
      end_npu_ids.clear();
    }

    AstraSim::LoggerFactory::init(logging_configuration);

    // Instantiate event queue
    const auto event_queue = std::make_shared<EventQueue>();

    // Generate topology
    const auto network_parser = NetworkParser(network_configuration);
    const auto topology = construct_topology(network_parser);

    // Get topology information
    const auto npus_count = topology->get_npus_count();
    const auto npus_count_per_dim = topology->get_npus_count_per_dim();
    const auto dims_count = topology->get_dims_count();

    // Set up Network API
    CongestionUnawareNetworkApi::set_event_queue(event_queue);
    CongestionUnawareNetworkApi::set_topology(topology);

    // Create ASTRA-sim related resources
    auto network_apis =
        std::vector<std::unique_ptr<CongestionUnawareNetworkApi>>();
    
    std::vector<std::unique_ptr<AnalyticalMemory>> memory_levels;
    const auto memory_config = load_memory_tier_config(memory_configuration);
    auto memory_tiers = std::vector<MemoryTierBinding>();
    for (const auto& tier : memory_config.tiers) {
      const auto path = write_temporary_memory_backend_config(
          tier.backend_config);
      memory_levels.push_back(std::make_unique<AnalyticalMemory>(path));
      std::remove(path.c_str());
      memory_tiers.push_back(
          {tier.tier_id, tier.tier_name, tier.num_devices,
           memory_levels.back().get()});
    }

    auto systems = std::vector<Sys*>();

    auto queues_per_dim = std::vector<int>();
    for (auto i = 0; i < dims_count; i++) {
        queues_per_dim.push_back(num_queues_per_dim);
    }

    for (int i = 0; i < npus_count; i++) {
        // create network and system
        auto network_api = std::make_unique<CongestionUnawareNetworkApi>(i);
        auto* const system =
            new Sys(i, workload_configuration, comm_group_configuration,
                    system_configuration, memory_tiers,
                    memory_config.manifest_digest, network_api.get(),
                    npus_count_per_dim, queues_per_dim, injection_scale,
                    comm_scale, rendezvous_protocol);

        // push back network and system
        network_apis.push_back(std::move(network_api));
        systems.push_back(system);
    }

    // Map instance NPU IDs for proper workload management
    // Precompute the systems handled by each controller NPU
    std::vector<std::vector<Sys*>> managed_systems(start_npu_ids.size());

    for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
      int npu_id = start_npu_ids[idx];

      // Determine the upper bound for this controller:
      // - If there's a next controller, stop before it
      // - Otherwise, go until npus_count
      int upper_bound_id;
      if (idx + 1 < start_npu_ids.size()) {
        upper_bound_id = start_npu_ids[idx + 1];
      } else {
        upper_bound_id = npus_count;  // last controller handles until the end
      }

      // Collect systems in the range (npu_id+1 .. upper_bound_id-1)
      for (int sid = npu_id + 1; sid < upper_bound_id; ++sid) {
        if (sid < 0 || sid >= npus_count) {
            AstraSim::LoggerFactory::get_logger("workload")
                ->critical("Skipping invalid system id {} while building managed_systems", sid);
        }
        if (std::find(end_npu_ids.begin(), end_npu_ids.end(), sid) != end_npu_ids.end()) {
          continue;
        }
        managed_systems[idx].push_back(systems[sid]);
      }
    }

    // Initiate simulation
    for (int i = 0; i < npus_count; i++) {
        systems[i]->workload->fire();
        // For debugging
        // systems[i]->workload->et_feeder->printGraph();
    }

    // run simulation
    // while (!event_queue->finished()) {
    //     event_queue->proceed();
    // }

    GracefulExit graceful_exit(systems.size());
    const auto finished_system_count = [&systems]() {
      return std::count_if(
          systems.begin(), systems.end(), [](const Sys* system) {
            return system->workload->is_finished;
          });
    };

    bool exit = false;
    while (!exit) {
      if (graceful_exit.should_exit(finished_system_count())) {
        exit = true;
        break;
      }

      if(!event_queue->finished()){
        event_queue->proceed();
      }
      else {
        event_queue->add_current_time();
      }

      if (graceful_exit.requested()) {
        continue;
      }

      for (std::size_t idx = 0; idx < end_npu_ids.size(); ++idx) {
        int npu_id = end_npu_ids[idx];
        cout << "Checking End NPU " << npu_id << " ..." << endl;
        // Only proceed if the workload has finished its iteration
        if (!systems[npu_id]->workload->is_sleep && systems[npu_id]->workload->is_finished) {
          systems[npu_id]->workload->report();
          AstraSim::LoggerFactory::get_logger("workload")->info("Waiting");

          std::string new_filename;
          std::getline(std::cin, new_filename);

          if (new_filename == "pass") {  
            // Skip to the next npu
            continue;
          } 
          else if (new_filename == "exit") {  
            // Stop accepting work and let every managed system drain before
            // entering the terminal self-check.
            graceful_exit.request();
            break;
          } 
          else if (new_filename == "done") {
            // This instance is done. Go to sleep until exit
            systems[npu_id]->workload->is_sleep = true;
          }
          else {  
            // Add new workload to this system
            systems[npu_id]->workload
                ->add_workload(new_filename, {});
          }
        }
      }
      
      if (graceful_exit.requested()) {
        continue;
      }

      for (std::size_t idx = 0; idx < start_npu_ids.size(); ++idx) {
        int npu_id = start_npu_ids[idx];
        // Only proceed if the workload has finished its iteration
        cout << "Checking Managed Systems for Controller NPU " << npu_id << " ..." << endl;
        if (!systems[npu_id]->workload->is_sleep && systems[npu_id]->workload->is_finished) {
          systems[npu_id]->workload->report();
          AstraSim::LoggerFactory::get_logger("workload")->info("Waiting");

          std::string new_filename;
          std::getline(std::cin, new_filename);

          if (new_filename == "pass") {  
            // Skip to the next npu
            continue;
          } 
          else if (new_filename == "exit") {  
            // Stop accepting work and let every managed system drain before
            // entering the terminal self-check.
            graceful_exit.request();
            break;
          } 
          else if (new_filename == "done") {
            // This instance is done. Go to sleep until exit
            systems[npu_id]->workload->is_sleep = true;
          }
          else {  
            // Add new workload to the systems handled by this npu
            systems[npu_id]->workload
                ->add_workload(new_filename, managed_systems[idx]);
          }
        }
      }
    }

    // check non exited system
    cout << "Checking Non-Exited Systems ..." << endl;
    bool done = true;
    for (int npu_id = 0; npu_id < npus_count; npu_id++) {

      if (systems[npu_id]->workload->is_finished == false){
        cout << "sys[" << npu_id << "] " << endl;
        systems[npu_id]->workload->et_feeder->printGraph();
        done = false;
      }
    }
    if (done){
      cout << "---------------------------" << endl;
      cout << "All Request Has Been Exited" << endl;
      cout << "---------------------------" << endl;
    }
    else{
      cout << "---------------------------" << endl;
      cout << "ERROR: Some Requests Remain" << endl;
      cout << "---------------------------" << endl;
    }

    // terminate simulation
    AstraSim::LoggerFactory::shutdown();
    return 0;
}
