/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#ifndef __PD_NETWORK_PROJECTION_HH__
#define __PD_NETWORK_PROJECTION_HH__

#include <astra-network-analytical/common/NetworkParser.h>
#include "extern/helper/json/json.hpp"

inline nlohmann::json pd_network_projection(const NetworkAnalytical::NetworkParser& parser) {
    nlohmann::json names = nlohmann::json::array();
    using Kind = NetworkAnalytical::TopologyBuildingBlock;
    for (const auto topology : parser.get_topologies_per_dim()) {
        names.push_back(topology == Kind::FullyConnected ? "FullyConnected" :
                        topology == Kind::Ring ? "Ring" : topology == Kind::Switch ? "Switch" : "Undefined");
    }
    return {{"topology", names}, {"npus_count", parser.get_npus_counts_per_dim()},
            {"bandwidth", parser.get_bandwidths_per_dim()}, {"latency", parser.get_latencies_per_dim()}};
}
#endif
