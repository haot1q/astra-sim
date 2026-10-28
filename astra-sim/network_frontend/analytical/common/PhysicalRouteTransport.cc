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

void add(Json& object, const char* key, uint64_t value) {
    const auto before = object.value(key, uint64_t{0});
    if (value > UINT64_MAX - before) throw std::overflow_error("physical route counter overflow");
    object[key] = before + value;
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

struct PhysicalRouteTransport::Message : Callable {
    PhysicalRouteTransport& owner;
    uint64_t id;
    Key key;
    void (*arrival)(void*);
    const std::vector<std::string>& path;
    size_t hop = 0;
    std::unique_ptr<TransportRequest> request;
    Message(PhysicalRouteTransport& owner, uint64_t id, Key key, void (*arrival)(void*),
            const std::vector<std::string>& path)
        : owner(owner), id(id), key(std::move(key)), arrival(arrival), path(path) {}
    void call(EventType, CallData*) override {
        owner.record(request->receipt());
        // Both H3 callbacks must return before this request is released.
        owner.events_->schedule_event(owner.events_->get_current_time(), continue_message, this);
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
    const auto digest = Wire::name(body["routes_digest"]);
    body.erase("routes_digest");
    if (body["schema_version"] != "physical-message-routes-v1" || body["timing_profile"] != kProfile ||
        body["physical_network_sha256"] != service.physical_network_sha256 ||
        body["transport_projection_digest"] != service.projection_digest ||
        body["service_binding_digest"] != services.identity().binding_digest ||
        body["service_activation_id"] != services.identity().activation_id ||
        Wire::uint32(body["rank_count"]) != systems.size() || Wire::digest(body) != digest) {
        throw std::invalid_argument("physical route identity/profile mismatch");
    }
    routes_ = read_routes(body, service, systems.size());
    summary_ = {{"schema_version", "physical-route-summary-v1"}, {"timing_profile", kProfile},
        {"uncalibrated", true}, {"physical_network_sha256", service.physical_network_sha256},
        {"routes_digest", digest}, {"submitted_messages", 0u}, {"completed_messages", 0u},
        {"completed_payload_bytes", 0u}, {"resources", Json::object()}};
}

PhysicalRouteTransport::~PhysicalRouteTransport() = default;

void PhysicalRouteTransport::validate(int src, int dst, uint64_t bytes) const {
    if (failure_) std::rethrow_exception(failure_);
    const auto found = routes_.find({src, dst});
    if (!bytes || found == routes_.end()) throw std::invalid_argument("missing physical route or zero payload");
    const auto& config = services_.transport_configuration();
    for (const auto& link : found->second) {
        const auto& resource = config.resources.at(config.links.at(link).resource);
        BandwidthResource(resource.bandwidth).service_time_ns(bytes, MemoryOperation::Read,
            resource.latency_ns, MemoryTimeRounding::Ceil);
    }
}

void PhysicalRouteTransport::submit(Key key, void (*arrival)(void*)) {
    const auto src = std::get<1>(key), dst = std::get<2>(key);
    validate(src, dst, std::get<3>(key));
    if (!arrival || next_id_ == UINT64_MAX) throw std::invalid_argument("invalid route arrival or ID exhaustion");
    const auto id = next_id_++;
    auto message = std::make_unique<Message>(*this, id, key, arrival, routes_.at({src, dst}));
    auto* context = message.get();
    messages_.emplace(id, std::move(message));
    add(summary_, "submitted_messages", 1);
    events_->schedule_event(events_->get_current_time(), continue_message, context);
}

void PhysicalRouteTransport::advance(Message& message) {
    message.request.reset();
    if (message.hop == message.path.size()) {
        message.arrival(&message.key);  // Original matching/late-recv semantics.
        add(summary_, "completed_messages", 1);
        add(summary_, "completed_payload_bytes", std::get<3>(message.key));
        messages_.erase(message.id);
        return;
    }
    const auto src = std::get<1>(message.key);
    auto& api = services_.transport(src, message.path[message.hop++]);
    api.set_sys(systems_.at(src));
    message.request = std::make_unique<TransportRequest>(src,
        "message-" + std::to_string(message.id), std::get<3>(message.key), message);
    api.issue(*message.request);
}

void PhysicalRouteTransport::continue_message(void* data) {
    auto* message = static_cast<Message*>(data);
    auto& owner = message->owner;
    try { owner.advance(*message); } catch (...) {
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
    row["first_ready_ns"] = std::min(row.value("first_ready_ns", UINT64_MAX), receipt.ready_ns);
    row["last_finish_ns"] = std::max(row.value("last_finish_ns", uint64_t{0}), receipt.finish_ns);
}

bool PhysicalRouteTransport::drained() const { return messages_.empty(); }

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
    summary_["pending_messages"] = messages_.size();
    std::ofstream stream(summary_path_);
    stream << summary_.dump(2) << '\n';
    stream.close();
    if (!stream) throw std::runtime_error("cannot save physical route summary");
    summary_written_ = true;
}
}  // namespace AstraSimAnalytical
