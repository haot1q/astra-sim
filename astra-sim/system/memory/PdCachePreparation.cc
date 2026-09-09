/******************************************************************************
This source code is licensed under the MIT license found in the root LICENSE.
*******************************************************************************/
#include "PdCachePreparation.hh"

#include <filesystem>
#include <algorithm>
#include <memory>
#include "MemoryMovementExecutor.hh"
#include "PipelinePreparationIdentity.hh"
#include "ServiceBindingJson.hh"
#include "UcieTransport.hh"
#include "astra-sim/system/Sys.hh"

namespace AstraSim {
namespace {
namespace Wire = ServiceBindingJson;
using Json = nlohmann::json;
void require(bool condition, const char* message) {
    if (!condition) throw std::invalid_argument(message);
}
uint64_t number(const Json& value) {
    require(value.is_number_unsigned() || (value.is_number_integer() && value.get<int64_t>() >= 0),
            "Cache preparation expects uint64");
    return value.get<uint64_t>();
}
std::string text(const Json& value) {
    require(value.is_string() && !value.get<std::string>().empty(), "Cache preparation expects text");
    return value.get<std::string>();
}
Json identity(const Json& document) {
    Json result;
    for (const auto* key : {"work_digest", "run_id", "attempt_id", "transfer_id", "instance_id",
                            "plan_id", "manifest_digest", "service_binding_digest", "service_activation_id"}) {
        result[key] = text(document.at(key));
    }
    return result;
}
}

PdCachePreparation::PdCachePreparation(const std::vector<Sys*>& systems,
        PhysicalServiceFactory& services, const MemoryTierConfigSet& memory)
    : systems_(systems), services_(services), memory_(memory) {}

PdCachePreparation::Work PdCachePreparation::read_work(const std::string& path) const {
    require(std::filesystem::file_size(path) <= 64 * 1024 * 1024, "Cache work exceeds protocol size limit");
    auto document = Wire::read(path);
    const bool staged = document.at("schema_version") == "pd-cache-preparation-v2";
    auto envelope = document;
    if (staged) {
        envelope.erase("pipeline_stage");
        envelope.erase("endpoint");
    }
    Wire::fields(envelope, {"schema_version", "work_digest", "run_id", "attempt_id", "transfer_id",
        "instance_id", "backend_instance_id", "plan_id", "page_id", "expected_residency_version",
        "home_domain_id", "manifest_digest", "service_binding_digest", "service_activation_id",
        "tier_name", "tier_id", "device_id", "ucie_link_id", "writeback_latency_ns", "ranks", "events"});
    const auto digest = text(document.at("work_digest"));
    auto body = document;
    body.erase("work_digest");
    require(digest == Wire::digest(body, true), "Cache work digest mismatch");
    require(staged || document.at("schema_version") == "pd-cache-preparation-v1", "unsupported Cache work schema");
    require(document.at("manifest_digest") == memory_.manifest_digest &&
        document.at("service_binding_digest") == services_.identity().binding_digest &&
        document.at("service_activation_id") == services_.identity().activation_id,
        "Cache work run service identity mismatch");
    Work result;
    result.document = document;
    result.identity = identity(document);
    const auto instance = Wire::uint32(document.at("backend_instance_id"));
    require(document.at("ranks").is_array(), "Cache ranks must be an array");
    for (const auto& raw : document.at("ranks")) {
        const auto rank = Wire::uint32(raw);
        const auto found = services_.configuration().ranks.find(rank);
        require(found != services_.configuration().ranks.end() &&
                found->second.at("instance_id") == instance && rank < systems_.size(),
                "Cache rank differs from actual backend instance");
        require(result.ranks.empty() || result.ranks.back() < rank,
                "Cache ranks must be sorted and unique");
        result.ranks.push_back(rank);
    }
    require(!result.ranks.empty(), "Cache preparation requires actual ranks");
    if (staged) {
        const auto stage = validate_pipeline_preparation_identity(document, services_);
        require(result.ranks == stage.ranks, "Cache work does not cover exact stage TP ranks");
    }
    validate_location(document, result);
    const auto& cache = memory_.native_payload.at("cache_hierarchy");
    require(number(document.at("writeback_latency_ns")) == number(cache.at("writeback_latency_ns")),
            "Cache fixed latency differs from actual policy");
    std::set<std::string> ids;
    for (const auto& item : document.at("events")) {
        Wire::fields(item, {"schema_version", "manifest_digest", "event_id", "plan_id", "page_id",
            "expected_residency_version", "home_domain_id", "bytes", "kind", "priority_class"});
        require(item.at("schema_version") == "cache-writeback-event-v1" && item.at("kind") == "store" &&
                item.at("priority_class") == "page_invalidate", "invalid Cache writeback event");
        for (const auto* key : {"manifest_digest", "plan_id", "page_id", "expected_residency_version", "home_domain_id"}) {
            require(item.at(key) == document.at(key), "Cache event differs from original plan");
        }
        require(ids.insert(text(item.at("event_id"))).second, "duplicate Cache writeback event");
        require(number(item.at("bytes")) == number(cache.at("line_bytes")), "Cache writeback must match actual line bytes");
        for (const auto rank : result.ranks) {
            if (!result.link.empty()) ucie_transaction_hops(MemoryOperation::Write,
                number(item.at("bytes")), systems_.at(rank)->ucie_link(result.link).header_bytes);
        }
    }
    require(document.at("events").is_array(), "Cache events must be an array");
    text(document.at("page_id"));
    number(document.at("expected_residency_version"));
    return result;
}

void PdCachePreparation::validate_location(const Json& document, Work& result) const {
    const auto& native = memory_.native_payload;
    require(native.contains("cache_hierarchy") && native.contains("home_domain_topology"),
            "Cache preparation requires actual Cache/Home configuration");
    const auto tier = text(document.at("tier_name"));
    result.tier = Wire::uint32(document.at("tier_id"));
    result.device = Wire::uint32(document.at("device_id"));
    const auto domain_id = Wire::uint32(document.at("home_domain_id"));
    bool found = false;
    for (const auto& domain : native.at("home_domain_topology").at("domains")) {
        if (Wire::uint32(domain.at("home_domain_id")) == domain_id) {
            found = domain.at("hot") == tier + ":" + std::to_string(result.device);
        }
    }
    require(found && native.at("cache_hierarchy").at("lower_tier_name") == tier,
            "Cache writeback must target its actual lower Home HBM");
    std::vector<std::string> links;
    for (const auto& link : memory_.ucie_links) {
        if (std::find(link.endpoints.begin(), link.endpoints.end(), tier) != link.endpoints.end()) links.push_back(link.id);
    }
    require(links.size() <= 1, "Cache lower tier UCIe path is ambiguous");
    result.link = links.empty() ? "" : links.front();
    require((result.link.empty() && document.at("ucie_link_id").is_null()) ||
            document.at("ucie_link_id") == result.link, "Cache UCIe access cannot be omitted or substituted");
    for (const auto rank : result.ranks) {
        bool matched = false;
        for (const auto& binding : services_.at(rank).memory) {
            if (binding.tier_name == tier && binding.tier_id == result.tier && result.device < binding.num_devices) matched = true;
        }
        require(matched, "Cache tier name/id/device mismatch");
        systems_.at(rank)->memory_api(result.tier, result.device);
        if (!result.link.empty()) require(result.device < systems_.at(rank)->ucie_link(result.link).stack_count,
                                          "Cache UCIe device out of range");
    }
}

void PdCachePreparation::submit(const std::string& path) {
    rethrow_failure();
    auto value = read_work(path);
    const auto digest = text(value.document.at("work_digest"));
    const auto key = Json::array({value.identity.at("run_id"), value.identity.at("attempt_id"),
        value.identity.at("transfer_id"), value.identity.at("instance_id"), value.identity.at("plan_id")}).dump();
    require(used_.insert(key).second && !work_.count(digest), "duplicate Cache preparation");
    value.ready = Sys::boostedTick();
    value.pending = 1;
    auto& work = work_.emplace(digest, std::move(value)).first->second;
    auto* callback = new Callback;
    callback->digest = digest;
    callback->start = true;
    callback->sys_id = work.ranks.front();
    const auto delay = work.document.at("events").empty() ? 0 : number(work.document.at("writeback_latency_ns"));
    systems_.at(callback->sys_id)->register_event(this, EventType::General, callback, delay);
}

void PdCachePreparation::start(Work& work, const std::string& digest) {
    if (work.cancelled) return;
    work.issued = Sys::boostedTick();
    // This synchronous enqueue has no cancellation boundary inside an all-rank intent.
    for (std::size_t item = 0; item < work.document.at("events").size(); ++item) {
        for (const auto rank : work.ranks) {
            auto* callback = new Callback;
            callback->digest = digest;
            callback->item = item;
            callback->start = false;
            callback->sys_id = rank;
            callback->device_id = work.device;
            callback->completion_target = this;
            ++work.pending;
            const auto bytes = number(work.document.at("events").at(item).at("bytes"));
            if (work.link.empty()) systems_.at(rank)->memory_api(work.tier, work.device)
                ->issue({bytes, MemoryOperation::Write}, callback);
            else issue_ucie_mem(systems_.at(rank), {work.tier, work.device, bytes,
                                work.link, MemoryOperation::Write}, callback);
        }
    }
}

void PdCachePreparation::complete(Work& work, const Callback& callback) {
    require(callback.memory_finish_ns == static_cast<uint64_t>(Sys::boostedTick()) &&
            callback.memory_start_ns >= work.issued, "Cache completion differs from actual memory callback");
    auto logical = work.document.at("events").at(callback.item);
    logical.erase("kind");
    logical.erase("priority_class");
    logical["schema_version"] = "cache-writeback-receipt-v1";
    logical["status"] = "completed";
    auto envelope = work.identity;
    envelope.update({{"schema_version", "pd-cache-writeback-receipt-v1"}, {"rank", callback.sys_id},
        {"issued_ns", work.issued}, {"finish_ns", static_cast<uint64_t>(Sys::boostedTick())},
        {"writeback", logical}});
    envelope["memory_resource_ref"] = services_.configuration().bindings.at(
        {callback.sys_id, "memory", text(work.document.at("tier_name")), work.device});
    envelope["ucie_resource_ref"] = work.link.empty() ? Json(nullptr) : Json(
        services_.configuration().bindings.at({callback.sys_id, "ucie", work.link, work.device}));
    emit_memory_protocol_line("PD_CACHE_WRITEBACK_COMPLETE " + envelope.dump());
    ++work.completed;
}

void PdCachePreparation::call(EventType event, CallData* raw) {
    std::unique_ptr<Callback> callback(static_cast<Callback*>(raw));
    try {
        rethrow_failure();
        services_.rethrow_failure();
        require(callback && event == EventType::General, "invalid Cache callback");
        auto& work = work_.at(callback->digest);
        require(work.pending > 0, "duplicate Cache callback");
        --work.pending;
        if (callback->start) start(work, callback->digest);
        else complete(work, *callback);
        finish(callback->digest);
    } catch (...) {
        failure_ = std::current_exception();
        throw;
    }
}

void PdCachePreparation::finish(const std::string& digest) {
    auto& work = work_.at(digest);
    if (work.pending) return;
    auto result = work.identity;
    result.update({{"schema_version", "pd-cache-preparation-receipt-v1"}, {"ready_ns", work.ready},
        {"finish_ns", static_cast<uint64_t>(Sys::boostedTick())}, {"completed_rank_events", work.completed},
        {"status", work.cancelled ? "cancelled" : "success"}});
    emit_memory_protocol_line("PD_CACHE_PREPARATION_COMPLETE " + result.dump());
    ++completed_count_;
    work_.erase(digest);
}

void PdCachePreparation::cancel(const std::string& digest) {
    rethrow_failure();
    auto& work = work_.at(digest);
    require(!work.cancelled, "duplicate Cache preparation cancellation");
    work.cancelled = true;
}

void PdCachePreparation::rethrow_failure() const {
    if (failure_) std::rethrow_exception(failure_);
}
}  // namespace AstraSim
