#include "common/JointSimulationCoordinator.hh"
#include "astra-sim/workload/JointAccessIndex.hh"
#include "astra-sim/workload/JointAccessIo.hh"
#include "astra-sim/workload/Workload.hh"
#include "astra-sim/system/Sys.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <openssl/evp.h>
#include <sstream>
#include <stdexcept>

namespace AstraSimAnalytical {
namespace {
using Json = JointRuntimeClient::Json;
using AstraSim::ServiceBindingJson::fields;
uint64_t number(const Json& value) { return JointRuntimeClient::decimal(value); }

Json original_ref(const std::string& path) {
    namespace fs = std::filesystem;
    const auto identity = AstraSim::JointAccessIo::file_identity(path);
    if (fs::canonical(path).string() != path || !fs::is_regular_file(path) || fs::file_size(path) > (256U << 20))
        throw std::invalid_argument("control original path or size differs");
    std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)> hash(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!hash || EVP_DigestInit_ex(hash.get(), EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("control hash initialization failed");
    std::ifstream stream(path, std::ios::binary);
    char data[65536]; uint64_t count = 0;
    while (stream.read(data, sizeof(data)) || stream.gcount()) {
        count += stream.gcount();
        if (count > (256U << 20) || EVP_DigestUpdate(hash.get(), data, stream.gcount()) != 1)
            throw std::runtime_error("control original hash/read failed");
    }
    unsigned char digest[EVP_MAX_MD_SIZE]; unsigned size = 0;
    if (!stream.eof() || identity != AstraSim::JointAccessIo::file_identity(path) ||
        EVP_DigestFinal_ex(hash.get(), digest, &size) != 1 || size != 32)
        throw std::runtime_error("control original changed");
    std::ostringstream hex; hex << std::hex << std::setfill('0');
    for (unsigned i = 0; i < size; ++i) hex << std::setw(2) << static_cast<unsigned>(digest[i]);
    return {{"path", path}, {"size_bytes", std::to_string(count)}, {"sha256", hex.str()}};
}
}

void JointSimulationCoordinator::require_finished_cohort(uint64_t iteration) const {
    if (loaded_ranks_.size() != workloads_.size() || !pending_.empty() || !sent_.empty() ||
        !receiving_.empty() || !rejected_.empty())
        throw std::invalid_argument("cohort retains ranks or memory promises");
    for (const auto* workload : workloads_)
        if (!loaded_ranks_.count(workload->sys->id) || !workload->is_finished ||
            !workload->pending_workloads.empty() || workload->iteration != iteration)
            throw std::invalid_argument("cohort has not finished in the original Workloads");
}

JointSimulationCoordinator::Json JointSimulationCoordinator::inspect_control(
        const std::string& prefix, uint32_t iteration) const {
    Json graphs = Json::array(); std::string kind;
    // All original ranks are validated together. Release these temporary charges
    // before Workload constructs and installs its own original feeders.
    std::vector<std::unique_ptr<AstraSim::JointAccessIndex>> indexes;
    for (const auto* workload : workloads_) {
        const auto rank = static_cast<uint32_t>(workload->sys->id);
        const auto path = prefix + "." + std::to_string(rank) + ".et";
        auto index = AstraSim::JointAccessIndex::load_and_validate(path,
            workload->sys->joint_source_context, rank, iteration, &workload->sys->memory_tiers);
        const auto actual = index->kind();
        if ((actual != "dep_forward" && actual != "dep_finish" && actual != "idle") ||
            (!kind.empty() && actual != kind) || index->binding_count())
            throw std::invalid_argument("successive control must be an original uniform non-memory cohort");
        kind = actual;
        graphs.push_back({{"rank", std::to_string(rank)}, {"et", original_ref(path)},
            {"companion", original_ref(path + ".joint-access.json")}});
        indexes.push_back(std::move(index));
    }
    return {{"kind", kind}, {"execution_iteration", std::to_string(iteration)},
            {"workload_prefix", prefix}, {"graphs", graphs}};
}

void JointSimulationCoordinator::check_workload(const std::string& prefix,
        const std::vector<AstraSim::Workload*>& targets) {
    if (awaiting_ || activation_.is_null()) return;
    if (handoff_) throw std::invalid_argument("workload arrived before successor READY");
    Json cohort = control_.is_null() ? activation_ : control_;
    auto loaded = loaded_ranks_;
    auto retired = retired_business_;
    const bool changed = prefix != cohort.at("workload_prefix");
    if (changed) {
        if (!config_.contains("successor")) throw std::invalid_argument("activated workload prefix changed");
        const auto previous = number(control_.is_null() ? view_identity_.at("execution_iteration") :
                                      control_.at("execution_iteration"));
        require_finished_cohort(previous);
        if (previous >= UINT32_MAX) throw std::overflow_error("control iteration overflow");
        cohort = inspect_control(prefix, previous + 1);
        if (control_.is_null()) retired = static_cast<uint32_t>(previous);
        loaded.clear();
    }
    for (const auto* target : targets) {
        if (std::find(workloads_.begin(), workloads_.end(), target) == workloads_.end())
            throw std::invalid_argument("workload target is not an original rank owner");
        const auto rank = static_cast<uint32_t>(target->sys->id);
        if (!loaded.insert(rank).second) throw std::invalid_argument("repeated cohort rank admission");
        const auto found = std::find_if(cohort.at("graphs").begin(), cohort.at("graphs").end(),
            [&](const auto& graph) { return number(graph.at("rank")) == rank; });
        if (found == cohort.at("graphs").end()) throw std::invalid_argument("cohort graph rank missing");
        for (const auto key : {"et", "companion"}) JointRuntimeClient::original(found->at(key), 256U << 20);
    }
    // Invalid late ranks/files do not partially consume earlier rank admission.
    if (changed) { control_ = std::move(cohort); retired_business_ = retired; }
    loaded_ranks_ = std::move(loaded);
}

void JointSimulationCoordinator::validate_business_graphs(const Json& manifest, uint32_t iteration) const {
    if (manifest.at("graphs").size() != workloads_.size())
        throw std::invalid_argument("successor graph rank count differs");
    std::vector<std::unique_ptr<AstraSim::JointAccessIndex>> indexes;
    for (size_t i = 0; i < workloads_.size(); ++i) {
        const auto* workload = workloads_[i]; const auto& graph = manifest.at("graphs").at(i);
        fields(graph, {"rank", "et", "companion"});
        const auto path = manifest.at("workload_prefix").get<std::string>() + "." +
            std::to_string(workload->sys->id) + ".et";
        if (number(graph.at("rank")) != static_cast<uint64_t>(workload->sys->id) ||
            graph.at("et").at("path") != path || graph.at("companion").at("path") != path + ".joint-access.json")
            throw std::invalid_argument("successor actual graph path/rank differs");
        for (const auto key : {"et", "companion"}) JointRuntimeClient::original(graph.at(key), 256U << 20);
        auto index = AstraSim::JointAccessIndex::load_and_validate(path, workload->sys->joint_source_context,
            workload->sys->id, iteration, &workload->sys->memory_tiers);
        if (index->kind() != "business") throw std::invalid_argument("successor is not actual business");
        indexes.push_back(std::move(index));
    }
}

void JointSimulationCoordinator::check_successor_facts(const Json& value, const Json& ref, uint64_t cut) const {
    fields(value, {"schema", "successor", "source", "workload_prefix", "graphs"});
    const auto& facts = value.at("successor"); const auto& prior = facts.at("predecessor");
    fields(facts, {"schema", "run_id", "source_provenance_digest", "source_selection_sha256", "ranks",
        "session_id", "view_sequence", "execution_iteration", "model_sequence", "model_rounds_before",
        "prepared_at_ns", "predecessor", "control", "preparations", "source_inventory_sha256"});
    fields(prior, {"session_id", "manifest_sha256", "view_sequence", "execution_iteration",
                   "acks", "resources_retired", "observed_at_ns"});
    const auto sequence = number(view_identity_.at("view_sequence"));
    const auto& control = facts.at("control"); fields(control, {"kind", "iteration", "acks"});
    if (sequence == UINT64_MAX || value.at("schema") != "simulator-joint-successor-v1" ||
        facts.at("schema") != "simulator-successor-facts-v1" || facts.at("session_id") != config_.at("session_id") ||
        number(facts.at("view_sequence")) != sequence + 1 || number(facts.at("model_sequence")) != sequence + 1 ||
        number(facts.at("model_rounds_before")) != sequence + 1 || fs(number(facts.at("prepared_at_ns"))) != cut ||
        control.at("kind") != "forward" || control.at("iteration") != control_.at("execution_iteration") ||
        number(facts.at("execution_iteration")) != number(control.at("iteration")) + 1 ||
        number(facts.at("execution_iteration")) > UINT32_MAX ||
        !prior.at("resources_retired").is_boolean() || !prior.at("resources_retired").get<bool>())
        throw std::invalid_argument("successor identity/prepare/retirement differs");
    for (const auto key : {"session_id", "manifest_sha256", "view_sequence", "execution_iteration"})
        if (prior.at(key) != view_identity_.at(key)) throw std::invalid_argument("successor predecessor differs");
    for (const auto key : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (facts.at(key) != context_.at(key)) throw std::invalid_argument("successor original context differs");
    if (number(prior.at("execution_iteration")) != *retired_business_ ||
        prior.at("acks").size() != workloads_.size() || control.at("acks").size() != workloads_.size())
        throw std::invalid_argument("successor actual retired cohort differs");
    const auto observed = fs(number(prior.at("observed_at_ns")));
    if (observed > cut) throw std::invalid_argument("successor retirement occurs after prepare");
    for (size_t i = 0; i < workloads_.size(); ++i) {
        const auto& model = prior.at("acks").at(i); const auto& forward = control.at("acks").at(i);
        fields(model, {"rank", "iteration", "finish_ns"}); fields(forward, {"rank", "iteration", "finish_ns"});
        if (number(model.at("rank")) != static_cast<uint64_t>(workloads_[i]->sys->id) ||
            forward.at("rank") != model.at("rank") || model.at("iteration") != prior.at("execution_iteration") ||
            forward.at("iteration") != control.at("iteration") || fs(number(model.at("finish_ns"))) > observed ||
            fs(number(forward.at("finish_ns"))) < observed || fs(number(forward.at("finish_ns"))) > cut)
            throw std::invalid_argument("successor original model/control ACK differs");
    }
    const auto root = std::filesystem::path(config_.at("successor").at("manifest_root").get<std::string>());
    if (!root.is_absolute() || std::filesystem::canonical(root) != root ||
        ref.at("path") != (root / ("view-" + std::to_string(sequence + 1) + ".json")).string())
        throw std::invalid_argument("successor immutable manifest path differs");
}

JointSimulationCoordinator::Json JointSimulationCoordinator::successor(const Json& ref) {
    rethrow_failure();
    if (!config_.contains("successor") || awaiting_ || handoff_ || stopped_ || draining_ ||
        view_identity_.is_null() || control_.is_null() || control_.at("kind") != "dep_forward" || !retired_business_)
        throw std::invalid_argument("successor requires a retired business and actual forward cohort");
    require_finished_cohort(number(control_.at("execution_iteration")));
    const auto cut = fs(queue_->get_current_time());
    if (cut != frontier_fs_) throw std::invalid_argument("successor is not at the original queue frontier");
    const auto value = AstraSim::ServiceBindingJson::parse(JointRuntimeClient::original(ref, 1U << 20, true));
    check_successor_facts(value, ref, cut);
    const auto& facts = value.at("successor");
    validate_business_graphs(value, number(facts.at("execution_iteration")));
    const Json identity{{"session_id", config_.at("session_id")}, {"manifest_sha256", ref.at("sha256")},
        {"view_sequence", facts.at("view_sequence")}, {"execution_iteration", facts.at("execution_iteration")}};
    const auto admitted = status_.at("admitted_nodes"), completed = status_.at("completed_nodes");
    if (number(admitted) != number(completed)) throw std::invalid_argument("successor has unfinished cumulative nodes");
    if (progress_observer_) progress_observer_->observe(config_.at("run_id"), view_identity_, status_, true);
    handoff_ = true;
    const auto initial = call("successor", {{"manifest", ref}, {"cut_fs", std::to_string(cut)}});
    for (const auto key : {"run_id", "source_provenance_digest", "source_selection_sha256", "ranks"})
        if (initial.at("source_context").at(key) != context_.at(key))
            throw std::invalid_argument("successor consumer source context differs");
    while (!status_.at("view_ready").get<bool>()) advance();
    if (!status_.at("ready").get<bool>() || status_.at("successor_capability") != "simulator-successive-views-v1" ||
        status_.at("view_identity") != identity || status_.at("admitted_nodes") != admitted ||
        status_.at("completed_nodes") != completed || number(status_.at("business_not_before_fs")) < cut)
        throw std::invalid_argument("successor READY identity/time/cumulative counts differ");
    Json ready;
    enqueue_receive(number(status_.at("business_not_before_fs")), [&](Service::ReceiveEdge edge) {
        ready = {{"schema", "simulator-joint-successor-ready-v1"}, {"run_id", config_.at("run_id")},
            {"session_id", config_.at("session_id")}, {"manifest_sha256", ref.at("sha256")},
            {"execution_iteration", facts.at("execution_iteration")}, {"view_sequence", facts.at("view_sequence")},
            {"cut_fs", std::to_string(cut)}, {"business_not_before_fs", status_.at("business_not_before_fs")},
            {"anchor_ns", std::to_string(edge.visible_ns)}, {"admitted_nodes", admitted}, {"completed_nodes", completed}};
    });
    while (ready.is_null()) advance();
    activation_ = value; view_identity_ = identity; control_ = Json();
    loaded_ranks_.clear(); retired_business_.reset(); handoff_ = false; idle_events_ = 0;
    begin_view_timing();
    return ready;
}
}
