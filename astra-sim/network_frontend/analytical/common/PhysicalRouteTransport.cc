/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "common/PhysicalRouteTransport.hh"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <limits>
#include <set>
#include <stdexcept>
#include <utility>

#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"

namespace AstraSimAnalytical {
using namespace AstraSim;
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;
namespace {
constexpr const char* kProfile = "message_store_forward_v1";
constexpr const char* kPropagationProfile = "message_store_forward_propagation_v1";

void add(Json& object, const char* key, uint64_t value) {
    const auto before = object.value(key, uint64_t{0});
    if (value > UINT64_MAX - before) throw std::overflow_error("physical route counter overflow");
    object[key] = before + value;
}

uint64_t sum(uint64_t left, uint64_t right) {
    if (right > UINT64_MAX - left) throw std::overflow_error("physical path timing overflow");
    return left + right;
}

auto read_routes(const Json& body, const TransportServiceConfig& service, uint32_t count) {
    std::map<std::pair<int, int>, std::vector<std::string>> routes;
    for (const auto& row : Wire::array(body.at("routes"))) {
        Wire::fields(row, {"source_rank", "target_rank", "service_link_ids"});
        const auto src = Wire::uint32(row["source_rank"]), dst = Wire::uint32(row["target_rank"]);
        if (src >= count || dst >= count || routes.count({src, dst})) {
            throw std::invalid_argument("unknown or duplicate physical rank pair");
        }
        std::vector<std::string> links;
        std::set<std::string> used;
        for (const auto& raw : Wire::array(row["service_link_ids"])) {
            const auto id = Wire::name(raw);
            const auto link = service.links.find(id);
            if (link == service.links.end() || !used.insert(link->second.resource).second) {
                throw std::invalid_argument("unknown link or repeated physical service");
            }
            links.push_back(id);
        }
        routes.emplace(std::make_pair(src, dst), std::move(links));
    }
    return routes;
}
}  // namespace

struct PhysicalRouteTransport::Path : Callable {
    PhysicalRouteTransport& owner;
    uint64_t id;
    TransportPathRequest input;
    TransportPathTiming timing;
    size_t hop = 0;
    std::unique_ptr<TransportRequest> request;
    Path(PhysicalRouteTransport& owner, uint64_t id, TransportPathRequest input, uint64_t ready)
        : owner(owner), id(id), input(std::move(input)), timing{ready, ready, ready, 0, 0} {}
    void call(EventType, CallData*) override {
        const auto receipt = request->receipt();
        if (receipt.ready_ns != timing.finish_ns || receipt.start_ns < receipt.ready_ns ||
            receipt.release_ns < receipt.start_ns || receipt.finish_ns < receipt.release_ns) {
            throw std::logic_error("noncontiguous physical path service timing");
        }
        if (hop == 1) timing.first_start_ns = receipt.start_ns;
        timing.finish_ns = receipt.finish_ns;
        timing.total_wait_ns = sum(timing.total_wait_ns, receipt.start_ns - receipt.ready_ns);
        timing.total_service_ns = sum(timing.total_service_ns, receipt.finish_ns - receipt.start_ns);
        if (input.observe_hop) input.observe_hop(receipt);
        // Both H3 callbacks must return before this request is released.
        owner.events_->schedule_event(owner.events_->get_current_time(), continue_path, this);
    }
};

PhysicalRouteTransport::PhysicalRouteTransport(const std::string& path, const std::string& summary,
    PhysicalServiceFactory& services, const std::vector<Sys*>& systems,
    std::shared_ptr<NetworkAnalytical::EventQueue> events)
    : services_(services), systems_(systems), events_(std::move(events)), summary_path_(summary) {
    if (!events_ || summary.empty() || std::filesystem::exists(summary)) {
        throw std::invalid_argument("physical routes require fresh summary and original events");
    }
    for (const auto* sys : systems) {
        if (!sys || sys->inter_dimension_scheduling == InterDimensionScheduling::OfflineGreedy ||
            sys->inter_dimension_scheduling == InterDimensionScheduling::OfflineGreedyFlex) {
            throw std::invalid_argument("physical routes do not support OfflineGreedy/Flex");
        }
    }
    auto body = Wire::read(path);
    Wire::fields(body, {"schema_version", "timing_profile", "physical_network_sha256",
        "transport_projection_digest", "service_binding_digest", "service_activation_id",
        "rank_count", "routes", "routes_digest"});
    const auto& service = services.transport_configuration();
    const auto* profile = service.policy == TransportLatencyPolicy::Propagation
        ? kPropagationProfile : kProfile;
    const auto digest = Wire::name(body["routes_digest"]);
    body.erase("routes_digest");
    if (body["schema_version"] != "physical-message-routes-v1" || body["timing_profile"] != profile ||
        body["physical_network_sha256"] != service.physical_network_sha256 ||
        body["transport_projection_digest"] != service.projection_digest ||
        body["service_binding_digest"] != services.identity().binding_digest ||
        body["service_activation_id"] != services.identity().activation_id ||
        Wire::uint32(body["rank_count"]) != systems.size() || Wire::digest(body) != digest) {
        throw std::invalid_argument("physical route identity/profile mismatch");
    }
    routes_ = read_routes(body, service, systems.size());
    summary_ = {{"schema_version", "physical-route-summary-v1"}, {"timing_profile", profile},
        {"uncalibrated", true}, {"physical_network_sha256", service.physical_network_sha256},
        {"routes_digest", digest}, {"submitted_messages", 0u}, {"completed_messages", 0u},
        {"completed_payload_bytes", 0u}, {"resources", Json::object()}};
}

PhysicalRouteTransport::~PhysicalRouteTransport() = default;

void PhysicalRouteTransport::validate(int src, int dst, uint64_t bytes) const {
    if (failure_) std::rethrow_exception(failure_);
    const auto found = routes_.find({src, dst});
    if (!bytes || found == routes_.end()) throw std::invalid_argument("missing physical route or zero payload");
    validate_path(src, bytes, found->second);
}

void PhysicalRouteTransport::submit(Key key, void (*arrival)(void*)) {
    const auto src = std::get<1>(key), dst = std::get<2>(key);
    validate(src, dst, std::get<3>(key));
    if (!arrival) throw std::invalid_argument("invalid route arrival");
    const auto bytes = std::get<3>(key);
    submit_path({static_cast<uint32_t>(src), bytes, routes_.at({src, dst}),
        [this, key = std::move(key), arrival, bytes](const TransportPathTiming&) mutable {
            arrival(&key);  // Original matching/late-recv semantics.
            add(summary_, "completed_messages", 1);
            add(summary_, "completed_payload_bytes", bytes);
            --pending_messages_;
        }, [this](const TransportReceipt& receipt) { record(receipt); }});
    add(summary_, "submitted_messages", 1);
    ++pending_messages_;
}

void PhysicalRouteTransport::validate_path(
    uint32_t rank, uint64_t bytes, const std::vector<std::string>& links) const {
    if (failure_) std::rethrow_exception(failure_);
    if (rank >= systems_.size() || !bytes) throw std::invalid_argument("invalid path rank or zero payload");
    const auto& config = services_.transport_configuration();
    std::set<std::string> used;
    uint64_t total = 0;
    for (const auto& id : links) {
        const auto link = config.links.find(id);
        if (link == config.links.end() || !used.insert(link->second.resource).second) {
            throw std::invalid_argument("unknown path link or repeated physical service");
        }
        const auto& resource = config.resources.at(link->second.resource);
        total = sum(total, BandwidthResource(resource.bandwidth).service_time_ns(
            bytes, MemoryOperation::Read, resource.latency_ns, MemoryTimeRounding::Ceil));
    }
}

void PhysicalRouteTransport::submit_path(TransportPathRequest input) {
    validate_path(input.rank, input.bytes, input.service_link_ids);
    if (!input.complete || next_id_ == UINT64_MAX) {
        throw std::invalid_argument("invalid path completion or ID exhaustion");
    }
    const auto id = next_id_++;
    auto path = std::make_unique<Path>(*this, id, std::move(input), Sys::boostedTick());
    auto* context = path.get();
    paths_.emplace(id, std::move(path));
    events_->schedule_event(events_->get_current_time(), continue_path, context);
}

void PhysicalRouteTransport::advance(Path& path) {
    path.request.reset();
    if (path.hop == path.input.service_link_ids.size()) {
        const auto& timing = path.timing;
        if (sum(timing.total_wait_ns, timing.total_service_ns) !=
                timing.finish_ns - timing.first_ready_ns || timing.finish_ns != Sys::boostedTick()) {
            throw std::logic_error("physical path completion timing mismatch");
        }
        path.input.complete(timing);
        paths_.erase(path.id);
        return;
    }
    const auto rank = path.input.rank;
    auto& api = services_.transport(rank, path.input.service_link_ids[path.hop++]);
    api.set_sys(systems_.at(rank));
    path.request = std::make_unique<TransportRequest>(rank,
        "path-" + std::to_string(path.id), path.input.bytes, path);
    api.issue(*path.request);
}

void PhysicalRouteTransport::continue_path(void* data) {
    auto* path = static_cast<Path*>(data);
    auto& owner = path->owner;
    try { owner.advance(*path); } catch (...) {
        if (!owner.failure_) owner.failure_ = std::current_exception();
    }
}

void PhysicalRouteTransport::record(const TransportReceipt& receipt) {
    auto& row = summary_["resources"][receipt.resource_id];
    if (row.is_null()) row = Json::object();
    add(row, "completed_requests", 1);
    add(row, "completed_payload_bytes", receipt.payload_bytes);
    add(row, "queue_wait_ns", receipt.start_ns - receipt.ready_ns);
    add(row, "service_ns", receipt.finish_ns - receipt.start_ns);
    if (services_.transport_configuration().policy == TransportLatencyPolicy::Propagation) {
        add(row, "busy_ns", receipt.release_ns - receipt.start_ns);
        add(row, "propagation_ns", receipt.finish_ns - receipt.release_ns);
    }
    row["first_ready_ns"] = std::min(row.value("first_ready_ns", UINT64_MAX), receipt.ready_ns);
    row["last_finish_ns"] = std::max(row.value("last_finish_ns", uint64_t{0}), receipt.finish_ns);
}

bool PhysicalRouteTransport::drained() const { return !failure_ && paths_.empty(); }

void PhysicalRouteTransport::rethrow_failure() {
    try {
        services_.rethrow_failure();
        if (failure_) std::rethrow_exception(failure_);
    } catch (...) {
        if (!failure_) failure_ = std::current_exception();
        write_summary();
        throw;
    }
}

void PhysicalRouteTransport::write_summary() {
    if (summary_written_) return;
    summary_["status"] = failure_ ? "failed" : drained() ? "completed" : "incomplete";
    summary_["pending_messages"] = pending_messages_;
    std::ofstream stream(summary_path_);
    stream << summary_.dump(2) << '\n';
    stream.close();
    if (!stream) throw std::runtime_error("cannot save physical route summary");
    summary_written_ = true;
}
}  // namespace AstraSimAnalytical
