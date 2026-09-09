/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/

#include <iostream>
#include <stdexcept>
#include "astra-sim/system/memory/PdPathWork.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"

int main(int argc, char** argv) {
    try {
        if (argc != 5) throw std::invalid_argument("expected manifest/services/interfaces/work paths");
        const auto memory = AstraSim::load_memory_tier_config(argv[1]);
        const auto raw = AstraSim::ServiceBindingJson::read(argv[2]);
        const auto physical = AstraSim::load_physical_service_config(argv[2], memory, raw.at("ranks").size());
        const auto interfaces = AstraSim::load_pd_path_service_config(argv[3], physical);
        const auto work = AstraSim::load_pd_path_work(argv[4], physical, interfaces);
        std::cout << "PASS pd-path-work slices=" << work.slices.size() << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
