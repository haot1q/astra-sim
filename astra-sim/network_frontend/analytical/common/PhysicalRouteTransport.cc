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
constexpr const char* kPipelineProfile = "message_block_pipeline_v1";

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
        if (owner.pipeline_) add(owner.summary_["pipeline_counters"], "completed_block_hops", 1);
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
    const bool pipeline = body.value("timing_profile", "") == kPipelineProfile;
    Json limits;
    if (pipeline) {
        limits = body.at("pipeline");
        Wire::fields(limits, {"block_bytes", "max_blocks_per_message", "max_block_hops_per_message",
                             "max_inflight_blocks", "max_inflight_block_hops"});
        for (const auto& value : limits.items()) {
            if (!value.value().is_number_unsigned() || value.value().get<uint64_t>() == 0) {
                throw std::invalid_argument("pipeline limits require positive uint64 integers");
            }
        }
        pipeline_ = PipelineLimits{limits["block_bytes"], limits["max_blocks_per_message"],
            limits["max_block_hops_per_message"], limits["max_inflight_blocks"], limits["max_inflight_block_hops"]};
    }
    auto fields = body;
    if (pipeline) fields.erase("pipeline");
    Wire::fields(fields, {"schema_version", "timing_profile", "physical_network_sha256",
        "transport_projection_digest", "service_binding_digest", "service_activation_id",
        "rank_count", "routes", "routes_digest"});
    const auto& service = services.transport_configuration();
    const auto* profile = pipeline ? kPipelineProfile : service.policy == TransportLatencyPolicy::Propagation
        ? kPropagationProfile : kProfile;
    if (pipeline && service.policy != TransportLatencyPolicy::Propagation) {
        throw std::invalid_argument("pipeline requires service-then-propagate policy");
    }
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
    if (pipeline_) {
        summary_["pipeline"] = limits;
        summary_["pipeline_counters"] = {{"submitted_blocks", 0u}, {"completed_blocks", 0u},
            {"completed_block_hops", 0u}, {"continuation_callbacks", 0u}, {"completed_message_elapsed_ns", 0u},
            {"peak_inflight_blocks", 0u}, {"peak_reserved_block_hops", 0u}};
    }
}

PhysicalRouteTransport::~PhysicalRouteTransport() = default;

void PhysicalRouteTransport::validate(int src, int dst, uint64_t bytes) const {
    if (failure_) std::rethrow_exception(failure_);
    const auto found = routes_.find({src, dst});
    if (!bytes || found == routes_.end()) throw std::invalid_argument("missing physical route or zero payload");
    if (pipeline_) {
        const auto blocks = block_count(bytes, found->second.size());
        check_budget(blocks, found->second.size());
        validate_path(src, std::min(bytes, pipeline_->block_bytes), found->second);
        const auto tail = bytes % pipeline_->block_bytes;
        if (tail && tail != std::min(bytes, pipeline_->block_bytes)) validate_path(src, tail, found->second);
    } else {
        validate_path(src, bytes, found->second);
    }
}

void PhysicalRouteTransport::submit(Key key, void (*arrival)(void*)) {
    const auto src = std::get<1>(key), dst = std::get<2>(key);
    validate(src, dst, std::get<3>(key));
    if (!arrival) throw std::invalid_argument("invalid route arrival");
    if (pipeline_) { submit_blocks(std::move(key), arrival); return; }
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

uint64_t PhysicalRouteTransport::block_count(uint64_t bytes, size_t hops) const {
    return hops ? (bytes - 1) / pipeline_->block_bytes + 1 : 1;
}

void PhysicalRouteTransport::check_budget(uint64_t blocks, uint64_t hops) const {
    if (blocks > UINT64_MAX - next_id_ || (hops && blocks > UINT64_MAX / hops)) {
        throw std::overflow_error("pipeline ID or block-hop overflow");
    }
    const auto work = blocks * hops;
    if (blocks > pipeline_->message_blocks || work > pipeline_->message_hops ||
        paths_.size() > pipeline_->inflight_blocks || blocks > pipeline_->inflight_blocks - paths_.size() ||
        reserved_hops_ > pipeline_->inflight_hops || work > pipeline_->inflight_hops - reserved_hops_) {
        throw std::invalid_argument("pipeline block budget exceeded");
    }
}

void PhysicalRouteTransport::publish(std::vector<std::unique_ptr<Path>> staged) {
    // Validation precedes publication; host allocation failure remains terminal-abort.
    // submitted_blocks tracks next_id_, so the checked ID bound also bounds this counter.
    // All first-hop events share this tick.
    for (auto& path : staged) {
        const auto id = path->id;
        auto* context = path.get();
        paths_.emplace(id, std::move(path));
        if (pipeline_) reserved_hops_ += context->input.service_link_ids.size();
    }
    if (pipeline_) {
        auto& row = summary_["pipeline_counters"];
        add(row, "submitted_blocks", staged.size());
        row["peak_inflight_blocks"] = std::max(row["peak_inflight_blocks"].get<uint64_t>(), uint64_t(paths_.size()));
        row["peak_reserved_block_hops"] = std::max(row["peak_reserved_block_hops"].get<uint64_t>(), reserved_hops_);
    }
    const auto first = next_id_;
    next_id_ += staged.size();
    for (uint64_t id = first; id < next_id_; ++id) {
        events_->schedule_event(events_->get_current_time(), continue_path, paths_.at(id).get());
    }
}

void PhysicalRouteTransport::submit_blocks(Key key, void (*arrival)(void*)) {
    struct Message {
        Key key;
        uint64_t remaining, ready;
    };
    const auto src = std::get<1>(key), dst = std::get<2>(key);
    const auto bytes = std::get<3>(key);
    const auto ready = Sys::boostedTick();
    const auto& links = routes_.at({src, dst});
    const auto count = block_count(bytes, links.size());
    auto message = std::make_shared<Message>(Message{std::move(key), count, ready});
    std::vector<std::unique_ptr<Path>> staged;
    staged.reserve(count);
    uint64_t remaining_bytes = bytes;
    for (uint64_t i = 0; i < count; ++i) {
        const auto size = links.empty() ? bytes : std::min(remaining_bytes, pipeline_->block_bytes);
        TransportPathRequest input{static_cast<uint32_t>(src), size, links,
            [this, message, arrival, bytes](const TransportPathTiming& timing) {
                if (--message->remaining) return;
                arrival(&message->key);
                add(summary_, "completed_messages", 1);
                add(summary_, "completed_payload_bytes", bytes);
                add(summary_["pipeline_counters"], "completed_message_elapsed_ns", timing.finish_ns - message->ready);
                --pending_messages_;
            }, [this](const TransportReceipt& receipt) { record(receipt); }};
        staged.push_back(std::make_unique<Path>(*this, next_id_ + i, std::move(input), ready));
        remaining_bytes -= size;
    }
    publish(std::move(staged));
    add(summary_, "submitted_messages", 1);
    ++pending_messages_;
}

void PhysicalRouteTransport::submit_path(TransportPathRequest input) {
    validate_path(input.rank, input.bytes, input.service_link_ids);
    if (!input.complete || next_id_ == UINT64_MAX) {
        throw std::invalid_argument("invalid path completion or ID exhaustion");
    }
    if (pipeline_) check_budget(1, input.service_link_ids.size());
    std::vector<std::unique_ptr<Path>> staged;
    staged.push_back(std::make_unique<Path>(*this, next_id_, std::move(input), Sys::boostedTick()));
    publish(std::move(staged));
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
        if (pipeline_) {
            add(summary_["pipeline_counters"], "completed_blocks", 1);
            reserved_hops_ -= path.input.service_link_ids.size();
        }
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
    try {
        if (owner.pipeline_) {
            if (owner.failure_) return;
            add(owner.summary_["pipeline_counters"], "continuation_callbacks", 1);
        }
        owner.advance(*path);
    } catch (...) {
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

void PhysicalRouteTransport::abort(std::exception_ptr failure) {
    if (!failure_) failure_ = std::move(failure);
    write_summary();
}

void PhysicalRouteTransport::write_summary() {
    if (summary_written_) return;
    summary_["status"] = failure_ ? "failed" : drained() ? "completed" : "incomplete";
    summary_["pending_messages"] = pending_messages_;
    if (pipeline_) {
        summary_["pipeline_counters"]["inflight_blocks"] = paths_.size();
        summary_["pipeline_counters"]["reserved_block_hops"] = reserved_hops_;
    }
    std::ofstream stream(summary_path_);
    stream << summary_.dump(2) << '\n';
    stream.close();
    if (!stream) throw std::runtime_error("cannot save physical route summary");
    summary_written_ = true;
}
}  // namespace AstraSimAnalytical
