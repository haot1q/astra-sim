/******************************************************************************
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*******************************************************************************/
#include "astra-sim/workload/JointAccessIo.hh"
#include "astra-sim/system/memory/ServiceBindingJson.hh"

#include <charconv>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <set>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace AstraSim::JointAccessIo {
namespace {
int open_without_symlink_traversal(const std::string& path) {
    const std::filesystem::path input(path);
    std::vector<std::string> parts;
    for (const auto& part : input.relative_path()) parts.push_back(part.string());
    if (parts.empty()) throw std::invalid_argument("joint access: empty file path");
    int directory = open(input.is_absolute() ? "/" : ".", O_RDONLY | O_CLOEXEC | O_DIRECTORY);
    if (directory < 0) throw std::invalid_argument("joint access: cannot open path root");
    for (size_t i = 0; i < parts.size(); ++i) {
        const auto flags = O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK |
            (i + 1 == parts.size() ? 0 : O_DIRECTORY);
        const int next = openat(directory, parts[i].c_str(), flags);
        close(directory);
        if (next < 0) throw std::invalid_argument("joint access: inaccessible or symlinked path");
        directory = next;
    }
    return directory;
}

class CheckedFile {
  public:
    CheckedFile(const std::string& path, uint64_t cap) : path_(path) {
        fd_ = open_without_symlink_traversal(path);
        if (fd_ < 0) throw std::invalid_argument("joint access: cannot open regular file");
        if (fstat(fd_, &initial_) || !S_ISREG(initial_.st_mode) ||
            initial_.st_size < 0 || static_cast<uint64_t>(initial_.st_size) > cap) {
            close(fd_);
            throw std::invalid_argument("joint access: file type or byte budget");
        }
        remaining_ = static_cast<uint64_t>(initial_.st_size);
    }
    ~CheckedFile() { close(fd_); }
    uint64_t remaining() const { return remaining_; }
    std::string take(uint64_t count) {
        if (count > remaining_ || count > std::string().max_size()) {
            throw std::invalid_argument("joint access: truncated file");
        }
        std::string result(static_cast<size_t>(count), '\0');
        size_t offset = 0;
        while (offset < result.size()) {
            auto got = ::read(fd_, result.data() + offset, result.size() - offset);
            if (got <= 0) throw std::invalid_argument("joint access: incomplete file read");
            offset += static_cast<size_t>(got);
        }
        remaining_ -= count;
        return result;
    }
    void reject_gzip() const {
        unsigned char prefix[2]{};
        if (pread(fd_, prefix, sizeof(prefix), 0) == 2 && prefix[0] == 0x1f && prefix[1] == 0x8b) {
            throw std::invalid_argument("joint access: gzip ET unsupported in association mode");
        }
    }
    void verify() const {
        struct stat after{}, named{};
        if (fstat(fd_, &after) || lstat(path_.c_str(), &named) ||
            !S_ISREG(named.st_mode) || after.st_dev != initial_.st_dev ||
            after.st_ino != initial_.st_ino || named.st_dev != initial_.st_dev ||
            named.st_ino != initial_.st_ino || after.st_size != initial_.st_size ||
            after.st_mtim.tv_sec != initial_.st_mtim.tv_sec ||
            after.st_mtim.tv_nsec != initial_.st_mtim.tv_nsec ||
            after.st_ctim.tv_sec != initial_.st_ctim.tv_sec ||
            after.st_ctim.tv_nsec != initial_.st_ctim.tv_nsec) {
            throw std::invalid_argument("joint access: file changed while reading");
        }
    }
  private:
    std::string path_;
    int fd_ = -1;
    struct stat initial_{};
    uint64_t remaining_ = 0;
};

void read_frame(CheckedFile& file, google::protobuf::Message& message, uint64_t cap) {
    uint32_t length = 0;
    for (unsigned shift = 0; shift < 35; shift += 7) {
        auto byte = static_cast<unsigned char>(file.take(1)[0]);
        if (shift == 28 && (byte & 0xf0)) {
            throw std::invalid_argument("joint access: invalid varint32 frame length");
        }
        length |= static_cast<uint32_t>(byte & 127) << shift;
        if ((byte & 128) == 0) {
            if ((shift && byte == 0) || !length || length > cap) {
                throw std::invalid_argument("joint access: noncanonical or oversized frame");
            }
            auto frame = file.take(length);
            if (!message.ParseFromArray(frame.data(), static_cast<int>(frame.size()))) {
                throw std::invalid_argument("joint access: invalid protobuf frame");
            }
            return;
        }
    }
    throw std::invalid_argument("joint access: incomplete frame length");
}
}  // namespace

uint64_t u64(const Json& value, bool positive) {
    auto digits = text(value);
    if ((digits.size() > 1 && digits[0] == '0') || digits[0] < '0' || digits[0] > '9') {
        throw std::invalid_argument("joint access: expected canonical uint64 string");
    }
    uint64_t result = 0;
    auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), result);
    if (parsed.ec != std::errc() || parsed.ptr != digits.data() + digits.size() ||
        (positive && !result)) {
        throw std::invalid_argument("joint access: unsigned value out of range");
    }
    return result;
}

int64_t integer(const Json& value) {
    auto digits = text(value);
    auto magnitude = digits[0] == '-' ? digits.substr(1) : digits;
    if (magnitude.empty() || (magnitude.size() > 1 && magnitude[0] == '0') ||
        magnitude[0] < '0' || magnitude[0] > '9' || digits == "-0") {
        throw std::invalid_argument("joint access: expected canonical int64 string");
    }
    int64_t result = 0;
    auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), result);
    if (parsed.ec != std::errc() || parsed.ptr != digits.data() + digits.size()) {
        throw std::invalid_argument("joint access: signed value out of range");
    }
    return result;
}

std::string text(const Json& value) {
    if (!value.is_string() || value.get_ref<const std::string&>().empty() ||
        value.get_ref<const std::string&>().find('\0') != std::string::npos) {
        throw std::invalid_argument("joint access: expected nonempty string");
    }
    return value.get<std::string>();
}

std::string hash(const Json& value) {
    auto result = text(value);
    if (result.size() != 64 || result.find_first_not_of("0123456789abcdef") != std::string::npos) {
        throw std::invalid_argument("joint access: expected bare SHA-256 hex");
    }
    return result;
}

uint64_t add(uint64_t first, uint64_t second) {
    if (second > UINT64_MAX - first) throw std::invalid_argument("joint access: uint64 addition overflow");
    return first + second;
}

uint64_t multiply(uint64_t first, uint64_t second) {
    if (first && second > UINT64_MAX / first) throw std::invalid_argument("joint access: uint64 product overflow");
    return first * second;
}

std::array<uint64_t, 7> file_identity(const std::string& path) {
    struct stat value{};
    if (lstat(path.c_str(), &value) || !S_ISREG(value.st_mode)) {
        throw std::invalid_argument("joint access: expected regular ET file");
    }
    return {static_cast<uint64_t>(value.st_dev), static_cast<uint64_t>(value.st_ino),
        static_cast<uint64_t>(value.st_size), static_cast<uint64_t>(value.st_mtim.tv_sec),
        static_cast<uint64_t>(value.st_mtim.tv_nsec), static_cast<uint64_t>(value.st_ctim.tv_sec),
        static_cast<uint64_t>(value.st_ctim.tv_nsec)};
}

Json read_json(const std::string& path, const JointAccessLimits& limits,
               uint64_t cap, std::string& raw) {
    CheckedFile file(path, cap);
    raw = file.take(file.remaining());
    file.verify();
    std::vector<std::set<std::string>> keys;
    auto callback = [&](int depth, Json::parse_event_t event, Json& value) {
        if (depth < 0 || static_cast<uint64_t>(depth) > limits.json_depth) {
            throw std::invalid_argument("joint access: JSON depth budget");
        }
        if (value.is_string()) {
            const auto& string = value.get_ref<const std::string&>();
            if (string.size() > limits.string_bytes || string.find('\0') != std::string::npos) {
                throw std::invalid_argument("joint access: invalid or oversized string");
            }
        }
        if (event == Json::parse_event_t::object_start) keys.emplace_back();
        if (event == Json::parse_event_t::key &&
            !keys.back().insert(value.get<std::string>()).second) {
            throw std::invalid_argument("joint access: duplicate JSON key");
        }
        if (event == Json::parse_event_t::object_end) keys.pop_back();
        if (event == Json::parse_event_t::value && value.is_number()) {
            throw std::invalid_argument("joint access: numeric fields require canonical strings");
        }
        return true;
    };
    return Json::parse(raw, callback);
}

Attributes attributes(const google::protobuf::RepeatedPtrField<ChakraProtoMsg::AttributeProto>& attrs) {
    Attributes result;
    for (const auto& attr : attrs) {
        if (!result.emplace(attr.name(), &attr).second) {
            throw std::invalid_argument("joint access: duplicate protobuf attribute");
        }
    }
    return result;
}

void scan_et(const std::string& path, const JointAccessLimits& limits,
             const std::function<void(const ChakraProtoMsg::GlobalMetadata&)>& metadata,
             const std::function<void(const ChakraProtoMsg::Node&)>& node) {
    CheckedFile file(path, limits.et_bytes);
    file.reject_gzip();
    ChakraProtoMsg::GlobalMetadata header;
    read_frame(file, header, limits.frame_bytes);
    metadata(header);
    uint64_t count = 0;
    std::set<uint64_t> ids;
    while (file.remaining()) {
        if (++count > limits.et_nodes) throw std::invalid_argument("joint access: ET node budget");
        ChakraProtoMsg::Node current;
        read_frame(file, current, limits.frame_bytes);
        if (!ids.insert(current.id()).second) throw std::invalid_argument("joint access: duplicate ET node");
        node(current);
    }
    if (!count) throw std::invalid_argument("joint access: empty ET");
    file.verify();
}
}  // namespace AstraSim::JointAccessIo

namespace AstraSim {
std::shared_ptr<JointSourceContext> JointSourceContext::load(const std::string& path) {
    using namespace JointAccessIo;
    auto result = std::shared_ptr<JointSourceContext>(new JointSourceContext());
    std::string raw;
    auto& identity = result->identity_;
    identity = read_json(path, result->limits_, result->limits_.context_bytes, raw);
    ServiceBindingJson::fields(identity, {"schema", "mode", "run_id", "source_provenance_digest",
        "source_profile", "source_selection_sha256", "ranks", "limits"});
    if (identity.at("schema") != "joint-source-context-v1" || identity.at("mode") != "association_only") {
        throw std::invalid_argument("joint access: unsupported context version/mode");
    }
    text(identity.at("run_id"));
    if (identity.at("source_profile") != "native-source-prefix-v1") {
        throw std::invalid_argument("joint access: unsupported Source profile");
    }
    hash(identity.at("source_provenance_digest")); hash(identity.at("source_selection_sha256"));
    const auto& limits = identity.at("limits");
    ServiceBindingJson::fields(limits, {"context_bytes", "association_bytes", "json_depth", "string_bytes",
        "table_records", "pieces", "et_bytes", "frame_bytes", "et_nodes", "pending_graphs_per_rank",
        "live_index_charge_bytes", "live_pieces"});
    const std::pair<const char*, uint64_t JointAccessLimits::*> fields[] = {
        {"context_bytes", &JointAccessLimits::context_bytes}, {"association_bytes", &JointAccessLimits::association_bytes},
        {"json_depth", &JointAccessLimits::json_depth}, {"string_bytes", &JointAccessLimits::string_bytes},
        {"table_records", &JointAccessLimits::table_records}, {"pieces", &JointAccessLimits::pieces},
        {"et_bytes", &JointAccessLimits::et_bytes}, {"frame_bytes", &JointAccessLimits::frame_bytes},
        {"et_nodes", &JointAccessLimits::et_nodes}, {"pending_graphs_per_rank", &JointAccessLimits::pending_graphs_per_rank},
        {"live_index_charge_bytes", &JointAccessLimits::live_index_charge_bytes}, {"live_pieces", &JointAccessLimits::live_pieces}};
    for (auto [key, member] : fields) {
        auto value = u64(limits.at(key), true);
        if (value > result->limits_.*member) throw std::invalid_argument("joint access: limit exceeds admitted cap");
        result->limits_.*member = value;
    }
    if (raw.size() > result->limits_.context_bytes) throw std::invalid_argument("joint access: context byte budget");
    // Enforce caller-lowered string/depth limits on the context itself, too.
    std::string confirmed;
    read_json(path, result->limits_, result->limits_.context_bytes, confirmed);
    if (confirmed != raw) throw std::invalid_argument("joint access: context changed during admission");
    const auto& ranks = ServiceBindingJson::array(identity.at("ranks"));
    if (ranks.empty() || ranks.size() > result->limits_.table_records) {
        throw std::invalid_argument("joint access: rank budget");
    }
    uint64_t previous = 0;
    for (const auto& rank : ranks) {
        auto value = u64(rank);
        if (value > UINT32_MAX || (!result->ranks_.empty() && value <= previous)) {
            throw std::invalid_argument("joint access: ranks must be sorted unique uint32");
        }
        result->ranks_.insert(static_cast<uint32_t>(value)); previous = value;
    }
    return result;
}

void JointSourceContext::reserve(uint64_t bytes, uint64_t pieces) {
    if (bytes > limits_.live_index_charge_bytes - live_bytes_ || pieces > limits_.live_pieces - live_pieces_) {
        throw std::invalid_argument("joint access: aggregate live index budget");
    }
    live_bytes_ += bytes; live_pieces_ += pieces;
}

void JointSourceContext::release(uint64_t bytes, uint64_t pieces) noexcept {
    live_bytes_ -= bytes; live_pieces_ -= pieces;
}
}  // namespace AstraSim
