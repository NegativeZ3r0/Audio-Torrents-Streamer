// ============================================================================
// flac_torrent_streamer.cpp
// ============================================================================
// A zero-disk-I/O C++20 CLI application that takes a BitTorrent magnet link,
// fetches the torrent metadata strictly in RAM, filters for .flac audio files,
// prompts the user to select a track, and streams the selected FLAC file
// directly from memory to the system audio output device as pieces arrive.
//
// Dependencies:
//   1. Rasterbar libtorrent (>= 2.0) — Custom RAM-only disk_interface backend
//   2. miniaudio (v0.11+ single-header library) — Built-in FLAC decoder & audio
//
// Build Instructions (Linux / macOS):
//   curl -sLO https://raw.githubusercontent.com/mackron/miniaudio/master/miniaudio.h
//   g++ -std=c++20 -O2 flac_torrent_streamer.cpp -o flac_torrent_streamer \
//       $(pkg-config --cflags --libs libtorrent-rasterbar) -lpthread -ldl -lm
//
// Usage:
//   ./flac_torrent_streamer "magnet:?xt=urn:btih:..."
// ============================================================================

#define MINIAUDIO_IMPLEMENTATION
#include "miniaudio.h"

#include <libtorrent/session.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/magnet_uri.hpp>
#include <libtorrent/disk_interface.hpp>
#include <libtorrent/disk_buffer_holder.hpp>
#include <libtorrent/disk_observer.hpp>
#include <libtorrent/hasher.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/peer_request.hpp>
#include <libtorrent/error_code.hpp>
#include <libtorrent/flags.hpp>

#include <boost/asio/post.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace lt = libtorrent;

namespace {

std::atomic<bool> g_interrupted{false};

void signal_handler(int) {
    g_interrupted.store(true);
}

// Helper for case-insensitive suffix check
bool ends_with_flac(std::string const& path) {
    if (path.size() < 5) return false;
    std::string ext = path.substr(path.size() - 5);
    std::transform(ext.begin(), ext.end(), ext.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return ext == ".flac";
}

// Format byte counts into human-readable MiB/KiB
std::string format_bytes(std::int64_t bytes) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (bytes >= 1024 * 1024) {
        oss << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MiB";
    } else if (bytes >= 1024) {
        oss << (static_cast<double>(bytes) / 1024.0) << " KiB";
    } else {
        oss << bytes << " B";
    }
    return oss.str();
}

} // namespace

// ============================================================================
// 1. In-Memory Storage Backend (Zero Disk I/O for libtorrent 2.x)
// ============================================================================
// Implements lt::disk_interface and lt::buffer_allocator_interface so that
// neither torrent metadata, nor piece blocks, nor partfiles ever touch the
// filesystem. All blocks are stored in RAM inside MemoryTorrentStorage.
// ============================================================================

class MemoryTorrentStorage {
public:
    explicit MemoryTorrentStorage(lt::file_storage const& fs)
        : m_files(fs) {}

    lt::span<char const> readv(lt::peer_request const& r, lt::storage_error& ec) const {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto const it = m_piece_data.find(r.piece);
        if (it == m_piece_data.end()) {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::eof;
            return {};
        }
        if (static_cast<int>(it->second.size()) <= r.start) {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::eof;
            return {};
        }
        int const avail = std::min(r.length, static_cast<int>(it->second.size()) - r.start);
        return {it->second.data() + r.start, avail};
    }

    void writev(lt::span<char const> const b, lt::piece_index_t const piece, int const offset) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto& data = m_piece_data[piece];
        if (data.empty()) {
            int const size = piece_size(piece);
            data.resize(static_cast<std::size_t>(size), 0);
        }
        if (offset >= 0 && offset + static_cast<int>(b.size()) <= static_cast<int>(data.size())) {
            std::memcpy(data.data() + offset, b.data(), static_cast<std::size_t>(b.size()));
        }
    }

    lt::sha1_hash hash(lt::piece_index_t const piece,
                       lt::span<lt::sha256_hash> const block_hashes,
                       lt::storage_error& ec) const {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto const it = m_piece_data.find(piece);
        if (it == m_piece_data.end()) {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::eof;
            return {};
        }
        if (!block_hashes.empty()) {
            int const piece_sz2 = m_files.piece_size2(piece);
            int const blocks_in_piece2 = m_files.blocks_in_piece2(piece);
            char const* buf = it->second.data();
            std::int64_t offset = 0;
            for (int k = 0; k < blocks_in_piece2; ++k) {
                lt::hasher256 h2;
                std::ptrdiff_t const len2 = std::min(
                    lt::default_block_size, static_cast<int>(piece_sz2 - offset));
                h2.update({buf, len2});
                buf += len2;
                offset += len2;
                block_hashes[k] = h2.final();
            }
        }
        return lt::hasher(it->second).final();
    }

    lt::sha256_hash hash2(lt::piece_index_t const piece, int const offset, lt::storage_error& ec) const {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto const it = m_piece_data.find(piece);
        if (it == m_piece_data.end()) {
            ec.operation = lt::operation_t::file_read;
            ec.ec = boost::asio::error::eof;
            return {};
        }
        int const piece_sz = m_files.piece_size2(piece);
        std::ptrdiff_t const len = std::min(lt::default_block_size, piece_sz - offset);
        lt::span<char const> b = {it->second.data() + offset, len};
        return lt::hasher256(b).final();
    }

    void clear_piece(lt::piece_index_t const piece) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_piece_data.erase(piece);
    }

private:
    int piece_size(lt::piece_index_t piece) const {
        int const num_pieces = static_cast<int>(
            (m_files.total_size() + m_files.piece_length() - 1) / m_files.piece_length());
        return static_cast<int>(piece) < num_pieces - 1
                   ? m_files.piece_length()
                   : static_cast<int>(m_files.total_size() -
                                      static_cast<std::int64_t>(num_pieces - 1) * m_files.piece_length());
    }

    lt::file_storage m_files;
    mutable std::mutex m_mutex;
    std::map<lt::piece_index_t, std::vector<char>> m_piece_data;
};

class MemoryDiskIO final : public lt::disk_interface, public lt::buffer_allocator_interface {
public:
    explicit MemoryDiskIO(lt::io_context& ioc) : m_ioc(ioc) {}

    void settings_updated() override {}

    lt::storage_holder new_torrent(lt::storage_params const& params,
                                   std::shared_ptr<void> const&) override {
        lt::storage_index_t const idx = m_free_slots.empty()
                                            ? m_torrents.end_index()
                                            : pop_free_slot();
        auto storage = std::make_shared<MemoryTorrentStorage>(params.files);
        if (idx == m_torrents.end_index()) {
            m_torrents.emplace_back(std::move(storage));
        } else {
            m_torrents[idx] = std::move(storage);
        }
        return lt::storage_holder(idx, *this);
    }

    void remove_torrent(lt::storage_index_t const idx) override {
        m_torrents[idx].reset();
        m_free_slots.push_back(idx);
    }

    void abort(bool) override {}

    void async_read(lt::storage_index_t storage,
                    lt::peer_request const& r,
                    std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
                    lt::disk_job_flags_t) override {
        lt::storage_error error;
        auto& st = m_torrents[storage];
        lt::span<char const> b = st->readv(r, error);

        // Copy into a heap buffer managed by our buffer_allocator_interface so
        // pointers remain safe even if piece storage is mutated concurrently.
        char* owned_buf = nullptr;
        if (!error && !b.empty()) {
            owned_buf = new char[static_cast<std::size_t>(b.size())];
            std::memcpy(owned_buf, b.data(), static_cast<std::size_t>(b.size()));
        }

        boost::asio::post(m_ioc, [this, handler = std::move(handler), error, owned_buf]() mutable {
            handler(lt::disk_buffer_holder(*this, owned_buf), error);
        });
    }

    bool async_write(lt::storage_index_t storage,
                     lt::peer_request const& r,
                     char const* buf,
                     std::shared_ptr<lt::disk_observer>,
                     std::function<void(lt::storage_error const&)> handler,
                     lt::disk_job_flags_t) override {
        lt::span<char const> const b = {buf, r.length};
        m_torrents[storage]->writev(b, r.piece, r.start);
        boost::asio::post(m_ioc, [handler = std::move(handler)]() {
            handler(lt::storage_error());
        });
        return false;
    }

    void async_hash(lt::storage_index_t storage,
                    lt::piece_index_t const piece,
                    lt::span<lt::sha256_hash> block_hashes,
                    lt::disk_job_flags_t,
                    std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler) override {
        lt::storage_error error;
        lt::sha1_hash const h = m_torrents[storage]->hash(piece, block_hashes, error);
        boost::asio::post(m_ioc, [handler = std::move(handler), piece, h, error]() {
            handler(piece, h, error);
        });
    }

    void async_hash2(lt::storage_index_t storage,
                     lt::piece_index_t const piece,
                     int const offset,
                     lt::disk_job_flags_t,
                     std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler) override {
        lt::storage_error error;
        lt::sha256_hash const h = m_torrents[storage]->hash2(piece, offset, error);
        boost::asio::post(m_ioc, [handler = std::move(handler), piece, h, error]() {
            handler(piece, h, error);
        });
    }

    void async_move_storage(lt::storage_index_t,
                            std::string p,
                            lt::move_flags_t,
                            std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> handler) override {
        boost::asio::post(m_ioc, [handler = std::move(handler), p]() {
            handler(lt::disk_status::fatal_disk_error, p,
                    lt::storage_error(lt::error_code(boost::system::errc::operation_not_supported, lt::system_category())));
        });
    }

    void async_release_files(lt::storage_index_t, std::function<void()> handler) override {
        if (handler) boost::asio::post(m_ioc, std::move(handler));
    }

    void async_delete_files(lt::storage_index_t storage,
                            lt::remove_flags_t,
                            std::function<void(lt::storage_error const&)> handler) override {
        if (m_torrents[storage]) {
            m_torrents[storage].reset();
        }
        boost::asio::post(m_ioc, [handler = std::move(handler)]() {
            handler(lt::storage_error());
        });
    }

    void async_check_files(lt::storage_index_t,
                           lt::add_torrent_params const*,
                           lt::aux::vector<std::string, lt::file_index_t>,
                           std::function<void(lt::status_t, lt::storage_error const&)> handler) override {
        boost::asio::post(m_ioc, [handler = std::move(handler)]() {
            handler(lt::status_t{}, lt::storage_error());
        });
    }

    void async_rename_file(lt::storage_index_t,
                           lt::file_index_t const idx,
                           std::string const name,
                           std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> handler) override {
        boost::asio::post(m_ioc, [handler = std::move(handler), name, idx]() {
            handler(name, idx, lt::storage_error());
        });
    }

    void async_stop_torrent(lt::storage_index_t, std::function<void()> handler) override {
        if (handler) boost::asio::post(m_ioc, std::move(handler));
    }

    void async_set_file_priority(lt::storage_index_t,
                                 lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
                                 std::function<void(lt::storage_error const&,
                                                    lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> handler) override {
        boost::asio::post(m_ioc, [handler = std::move(handler), prio = std::move(prio)]() mutable {
            handler(lt::storage_error(), std::move(prio));
        });
    }

    void async_clear_piece(lt::storage_index_t storage,
                           lt::piece_index_t index,
                           std::function<void(lt::piece_index_t)> handler) override {
        m_torrents[storage]->clear_piece(index);
        boost::asio::post(m_ioc, [handler = std::move(handler), index]() {
            handler(index);
        });
    }

    // buffer_allocator_interface implementation
    void free_disk_buffer(char* b) override {
        delete[] b;
    }

    void free_multiple_buffers(lt::span<char*> bufs) override {
        for (char* b : bufs) {
            delete[] b;
        }
    }

    void update_stats_counters(lt::counters&) const override {}

    std::vector<lt::open_file_state> get_status(lt::storage_index_t) const override {
        return {};
    }

    void submit_jobs() override {}

private:
    lt::storage_index_t pop_free_slot() {
        lt::storage_index_t const ret = m_free_slots.back();
        m_free_slots.pop_back();
        return ret;
    }

    lt::aux::vector<std::shared_ptr<MemoryTorrentStorage>, lt::storage_index_t> m_torrents;
    std::vector<lt::storage_index_t> m_free_slots;
    lt::io_context& m_ioc;
};

std::unique_ptr<lt::disk_interface> memory_disk_io_constructor(
    lt::io_context& ioc, lt::settings_interface const&, lt::counters&) {
    return std::make_unique<MemoryDiskIO>(ioc);
}

// ============================================================================
// 2. In-Memory Torrent File Stream & Sliding-Window Prefetcher
// ============================================================================
// Maps byte offsets within the chosen FLAC file to BitTorrent piece indices +
// piece offsets, schedules piece deadlines ahead of the playback cursor, and
// blocks miniaudio's decoder read callback cleanly until the required piece
// is verified and stored in RAM.
// ============================================================================

class TorrentFlacStream {
public:
    TorrentFlacStream(lt::torrent_handle handle,
                      std::shared_ptr<const lt::torrent_info> ti,
                      lt::file_index_t file_index)
        : m_handle(std::move(handle)),
          m_ti(std::move(ti)),
          m_file_index(file_index) {
        lt::file_storage const& fs = m_ti->layout();
        m_file_offset = fs.file_offset(m_file_index);
        m_file_size = fs.file_size(m_file_index);
        m_piece_length = fs.piece_length();

        auto const first_req = fs.map_file(m_file_index, 0, 1);
        auto const last_req = fs.map_file(
            m_file_index, std::max<std::int64_t>(0, m_file_size - 1), 1);

        m_first_piece = static_cast<int>(first_req.piece);
        m_last_piece = static_cast<int>(last_req.piece);
    }

    std::int64_t file_size() const { return m_file_size; }
    int first_piece() const { return m_first_piece; }
    int last_piece() const { return m_last_piece; }

    // Called from the libtorrent alert loop when a read_piece_alert arrives
    void on_piece_ready(int piece_index, char const* buffer, int size) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_pieces.find(piece_index) == m_pieces.end()) {
                m_pieces.emplace(piece_index, std::vector<char>(buffer, buffer + size));
            }
            m_requested_pieces.erase(piece_index);
        }
        m_cv.notify_all();
    }

    bool has_piece(int piece_index) const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_pieces.find(piece_index) != m_pieces.end();
    }

    std::size_t cached_piece_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_pieces.size();
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
        }
        m_cv.notify_all();
    }

    // Request a piece with high priority & immediate alert delivery
    void request_piece_urgent(int piece_index, int deadline_ms = 0) {
        if (piece_index < m_first_piece || piece_index > m_last_piece) return;

        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pieces.find(piece_index) != m_pieces.end()) return;
        if (m_requested_pieces.find(piece_index) != m_requested_pieces.end()) return;

        m_requested_pieces.insert(piece_index);
        lt::piece_index_t const p(piece_index);
        m_handle.piece_priority(p, lt::top_priority);
        m_handle.set_piece_deadline(p, deadline_ms, lt::torrent_handle::alert_when_available);
    }

    // Prefetch a sliding window of pieces ahead of the current piece
    void prefetch_window(int current_piece, int window_size = 8) {
        for (int i = 0; i < window_size; ++i) {
            int const p = current_piece + i;
            if (p > m_last_piece) break;
            request_piece_urgent(p, i * 150);
        }
    }

    // Read bytes from the virtual FLAC file in RAM (called by miniaudio decoder)
    ma_result read(void* dst_buf, std::size_t bytes_to_read, std::size_t* bytes_read_out) {
        if (bytes_read_out) *bytes_read_out = 0;
        if (m_cursor >= m_file_size) {
            return MA_AT_END;
        }

        std::int64_t const remaining_in_file = m_file_size - m_cursor;
        std::size_t const to_copy_total = static_cast<std::size_t>(
            std::min<std::int64_t>(static_cast<std::int64_t>(bytes_to_read), remaining_in_file));

        char* out_ptr = static_cast<char*>(dst_buf);
        std::size_t total_copied = 0;

        while (total_copied < to_copy_total) {
            std::int64_t const abs_torrent_offset = m_file_offset + m_cursor;
            int const piece_idx = static_cast<int>(abs_torrent_offset / m_piece_length);
            int const offset_in_piece = static_cast<int>(abs_torrent_offset % m_piece_length);

            // Schedule urgent download for this piece and prefetch lookahead window
            prefetch_window(piece_idx, 8);

            // Also ensure the last piece of the file is requested early because
            // FLAC decoders sometimes seek to the end during initialization/probing.
            request_piece_urgent(m_last_piece, 200);

            std::unique_lock<std::mutex> lock(m_mutex);
            m_cv.wait(lock, [this, piece_idx]() {
                return m_closed || g_interrupted.load() ||
                       m_pieces.find(piece_idx) != m_pieces.end();
            });

            if (m_closed || g_interrupted.load()) {
                return MA_CANCELLED;
            }

            auto const& piece_buf = m_pieces.at(piece_idx);
            if (offset_in_piece >= static_cast<int>(piece_buf.size())) {
                return MA_IO_ERROR;
            }

            std::size_t const avail_in_piece =
                piece_buf.size() - static_cast<std::size_t>(offset_in_piece);
            std::size_t const chunk =
                std::min(to_copy_total - total_copied, avail_in_piece);

            std::memcpy(out_ptr + total_copied, piece_buf.data() + offset_in_piece, chunk);
            total_copied += chunk;
            m_cursor += static_cast<std::int64_t>(chunk);
        }

        if (bytes_read_out) *bytes_read_out = total_copied;
        return (total_copied > 0) ? MA_SUCCESS : MA_AT_END;
    }

    // Seek within the virtual FLAC file (called by miniaudio decoder)
    ma_result seek(ma_int64 byte_offset, ma_seek_origin origin) {
        std::int64_t target = 0;
        switch (origin) {
            case ma_seek_origin_start:
                target = byte_offset;
                break;
            case ma_seek_origin_current:
                target = m_cursor + byte_offset;
                break;
            case ma_seek_origin_end:
                target = m_file_size + byte_offset;
                break;
            default:
                return MA_INVALID_ARGS;
        }

        if (target < 0 || target > m_file_size) {
            return MA_BAD_SEEK;
        }

        m_cursor = target;
        if (m_cursor < m_file_size) {
            int const target_piece =
                static_cast<int>((m_file_offset + m_cursor) / m_piece_length);
            prefetch_window(target_piece, 6);
        }
        return MA_SUCCESS;
    }

    // Tell current byte cursor within the FLAC file
    ma_result tell(ma_int64* cursor_out) const {
        if (!cursor_out) return MA_INVALID_ARGS;
        *cursor_out = static_cast<ma_int64>(m_cursor);
        return MA_SUCCESS;
    }

    std::int64_t cursor() const { return m_cursor; }

private:
    lt::torrent_handle m_handle;
    std::shared_ptr<const lt::torrent_info> m_ti;
    lt::file_index_t m_file_index;

    std::int64_t m_file_offset{0};
    std::int64_t m_file_size{0};
    int m_piece_length{0};
    int m_first_piece{0};
    int m_last_piece{0};

    std::atomic<std::int64_t> m_cursor{0};

    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    std::unordered_map<int, std::vector<char>> m_pieces;
    std::set<int> m_requested_pieces;
    bool m_closed{false};
};

// ============================================================================
// 3. Miniaudio Decoder & Playback Callbacks
// ============================================================================

static ma_result on_decoder_read(ma_decoder* pDecoder,
                                 void* pBufferOut,
                                 std::size_t bytesToRead,
                                 std::size_t* pBytesRead) {
    auto* stream = static_cast<TorrentFlacStream*>(pDecoder->pUserData);
    if (!stream) return MA_INVALID_ARGS;
    return stream->read(pBufferOut, bytesToRead, pBytesRead);
}

static ma_result on_decoder_seek(ma_decoder* pDecoder,
                                 ma_int64 byteOffset,
                                 ma_seek_origin origin) {
    auto* stream = static_cast<TorrentFlacStream*>(pDecoder->pUserData);
    if (!stream) return MA_INVALID_ARGS;
    return stream->seek(byteOffset, origin);
}

static ma_result on_decoder_tell(ma_decoder* pDecoder, ma_int64* pCursor) {
    auto* stream = static_cast<TorrentFlacStream*>(pDecoder->pUserData);
    if (!stream) return MA_INVALID_ARGS;
    return stream->tell(pCursor);
}

struct AudioContext {
    ma_decoder decoder{};
    std::atomic<bool> playback_finished{false};
    std::atomic<ma_uint64> frames_played{0};
};

static void audio_data_callback(ma_device* pDevice,
                                void* pOutput,
                                const void* /*pInput*/,
                                ma_uint32 frameCount) {
    auto* ctx = static_cast<AudioContext*>(pDevice->pUserData);
    if (!ctx || ctx->playback_finished.load()) {
        return;
    }

    ma_uint64 framesRead = 0;
    ma_result const res =
        ma_decoder_read_pcm_frames(&ctx->decoder, pOutput, frameCount, &framesRead);

    ctx->frames_played.fetch_add(framesRead);

    if (framesRead < frameCount || res == MA_AT_END) {
        ctx->playback_finished.store(true);
    }
}

// ============================================================================
// 4. Main CLI Application
// ============================================================================

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);

    std::string magnet_uri;
    if (argc >= 2) {
        magnet_uri = argv[1];
    } else {
        std::cout << "Enter BitTorrent magnet link: ";
        if (!std::getline(std::cin, magnet_uri) || magnet_uri.empty()) {
            std::cerr << "Error: No magnet link provided.\n"
                      << "Usage: " << argv[0] << " \"magnet:?xt=urn:btih:...\"\n";
            return 1;
        }
    }

    try {
        // 1. Configure libtorrent session with our custom RAM-only disk_io_constructor
        lt::session_params ses_params;
        ses_params.disk_io_constructor = memory_disk_io_constructor;
        ses_params.settings.set_int(
            lt::settings_pack::alert_mask,
            lt::alert_category::error |
            lt::alert_category::status |
            lt::alert_category::piece_progress);
        ses_params.settings.set_bool(lt::settings_pack::enable_dht, true);
        ses_params.settings.set_bool(lt::settings_pack::enable_lsd, true);

        lt::session ses(std::move(ses_params));

        // 2. Parse magnet URI and add torrent to session
        lt::error_code ec;
        lt::add_torrent_params atp = lt::parse_magnet_uri(magnet_uri, ec);
        if (ec) {
            std::cerr << "Failed to parse magnet link: " << ec.message() << "\n";
            return 1;
        }

        // Virtual save path required by libtorrent API even though MemoryDiskIO never touches disk
        atp.save_path = "/ram_virtual_root";

        // Pre-fill file priorities with 0 (do_not_download) so libtorrent does not
        // download any pieces until the user selects a specific FLAC track.
        atp.file_priorities.assign(1000, lt::dont_download);

        std::cout << "[*] Connecting to peers & fetching torrent metadata in memory...\n";
        lt::torrent_handle handle = ses.add_torrent(std::move(atp));

        // 3. Wait for metadata_received_alert (or if torrent already has metadata)
        bool has_metadata = handle.status().has_metadata;
        while (!has_metadata && !g_interrupted.load()) {
            std::vector<lt::alert*> alerts;
            ses.pop_alerts(&alerts);
            for (lt::alert* a : alerts) {
                if (lt::alert_cast<lt::metadata_received_alert>(a)) {
                    has_metadata = true;
                } else if (auto* err = lt::alert_cast<lt::torrent_error_alert>(a)) {
                    std::cerr << "\n[!] Torrent error: " << err->message() << "\n";
                    return 1;
                }
            }
            if (!has_metadata) {
                lt::torrent_status st = handle.status();
                std::cout << "\r[~] Fetching metadata... Peers: " << st.num_peers << std::flush;
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }
        }

        if (g_interrupted.load()) {
            std::cout << "\n[!] Interrupted by user.\n";
            return 0;
        }

        std::shared_ptr<const lt::torrent_info> ti = handle.torrent_file();
        if (!ti) {
            std::cerr << "\n[!] Failed to retrieve torrent_info.\n";
            return 1;
        }

        std::cout << "\n[+] Metadata received for: " << ti->name() << "\n";

        // 4. Filter for .flac files and silence all files initially
        lt::file_storage const& fs = ti->layout();
        std::vector<lt::download_priority_t> priorities(
            static_cast<std::size_t>(fs.num_files()), lt::dont_download);
        handle.prioritize_files(priorities);

        struct FlacEntry {
            lt::file_index_t file_index;
            std::string path;
            std::int64_t size;
        };
        std::vector<FlacEntry> flac_files;

        for (auto const i : fs.file_range()) {
            std::string const path = fs.file_path(i);
            if (ends_with_flac(path)) {
                flac_files.push_back({i, path, fs.file_size(i)});
            }
        }

        if (flac_files.empty()) {
            std::cout << "[!] No .flac audio files found in this torrent.\n";
            return 0;
        }

        // 5. Display FLAC tracks and ask user which one to stream
        std::cout << "\nAvailable FLAC tracks (" << flac_files.size() << "):\n";
        for (std::size_t idx = 0; idx < flac_files.size(); ++idx) {
            std::cout << "  [" << (idx + 1) << "] " << flac_files[idx].path
                      << " (" << format_bytes(flac_files[idx].size) << ")\n";
        }

        std::size_t selection = 0;
        while (true) {
            std::cout << "\nSelect a FLAC track to play [1-" << flac_files.size() << "]: ";
            std::string line;
            if (!std::getline(std::cin, line) || g_interrupted.load()) {
                return 0;
            }
            try {
                selection = std::stoull(line);
                if (selection >= 1 && selection <= flac_files.size()) {
                    break;
                }
            } catch (...) {}
            std::cout << "Invalid choice. Please enter a number between 1 and "
                      << flac_files.size() << ".\n";
        }

        FlacEntry const& chosen = flac_files[selection - 1];
        std::cout << "\n[*] Selected: " << chosen.path << " (" << format_bytes(chosen.size) << ")\n";

        // 6. Initialize the in-memory stream and prioritize only the chosen FLAC file
        TorrentFlacStream stream(handle, ti, chosen.file_index);

        priorities[static_cast<std::size_t>(static_cast<int>(chosen.file_index))] =
            lt::default_priority;
        handle.prioritize_files(priorities);

        // Start a background alert pump thread to feed completed pieces into TorrentFlacStream
        std::atomic<bool> pump_running{true};
        std::thread alert_pump([&]() {
            while (pump_running.load() && !g_interrupted.load()) {
                std::vector<lt::alert*> alerts;
                ses.pop_alerts(&alerts);
                for (lt::alert* a : alerts) {
                    if (auto* rp = lt::alert_cast<lt::read_piece_alert>(a)) {
                        if (!rp->error && rp->buffer) {
                            stream.on_piece_ready(
                                static_cast<int>(rp->piece), rp->buffer.get(), rp->size);
                        } else if (!rp->error) {
                            // Re-request if buffer was empty
                            handle.read_piece(rp->piece);
                        }
                    } else if (auto* pf = lt::alert_cast<lt::piece_finished_alert>(a)) {
                        int const p = static_cast<int>(pf->piece_index);
                        if (p >= stream.first_piece() && p <= stream.last_piece()) {
                            handle.read_piece(pf->piece_index);
                        }
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(25));
            }
        });

        // 7. Pre-buffer the initial pieces of the chosen FLAC file before starting miniaudio
        int const total_track_pieces = stream.last_piece() - stream.first_piece() + 1;
        int const prebuffer_target = std::min(3, total_track_pieces);
        stream.prefetch_window(stream.first_piece(), std::min(8, total_track_pieces));

        std::cout << "[*] Buffering initial FLAC header & audio frames in RAM...\n";
        while (!g_interrupted.load()) {
            int ready_initial = 0;
            for (int i = 0; i < prebuffer_target; ++i) {
                if (stream.has_piece(stream.first_piece() + i)) {
                    ++ready_initial;
                }
            }
            if (ready_initial >= prebuffer_target) {
                break;
            }
            lt::torrent_status st = handle.status();
            std::cout << "\r[~] Pre-buffering: " << ready_initial << "/" << prebuffer_target
                      << " initial pieces | Speed: " << (st.download_payload_rate / 1024)
                      << " KiB/s | Peers: " << st.num_peers << "   " << std::flush;
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
        std::cout << "\n";

        if (g_interrupted.load()) {
            pump_running.store(false);
            stream.close();
            if (alert_pump.joinable()) alert_pump.join();
            return 0;
        }

        // 8. Initialize miniaudio FLAC decoder backed by our RAM stream callbacks
        AudioContext audio_ctx;
        ma_decoder_config dec_cfg = ma_decoder_config_init_default();
        dec_cfg.encodingFormat = ma_encoding_format_flac;

        ma_result res = ma_decoder_init(
            on_decoder_read, on_decoder_seek, &stream, &dec_cfg, &audio_ctx.decoder);
        if (res != MA_SUCCESS) {
            std::cerr << "[!] Failed to initialize FLAC decoder from stream (error " << res << ").\n";
            pump_running.store(false);
            stream.close();
            if (alert_pump.joinable()) alert_pump.join();
            return 1;
        }
        audio_ctx.decoder.onTell = on_decoder_tell;

        // 9. Configure and start the system audio playback device
        ma_device_config dev_cfg = ma_device_config_init(ma_device_type_playback);
        dev_cfg.playback.format = audio_ctx.decoder.outputFormat;
        dev_cfg.playback.channels = audio_ctx.decoder.outputChannels;
        dev_cfg.sampleRate = audio_ctx.decoder.outputSampleRate;
        dev_cfg.dataCallback = audio_data_callback;
        dev_cfg.pUserData = &audio_ctx;

        ma_device device;
        if (ma_device_init(nullptr, &dev_cfg, &device) != MA_SUCCESS) {
            std::cerr << "[!] Failed to open audio playback device.\n";
            ma_decoder_uninit(&audio_ctx.decoder);
            pump_running.store(false);
            stream.close();
            if (alert_pump.joinable()) alert_pump.join();
            return 1;
        }

        if (ma_device_start(&device) != MA_SUCCESS) {
            std::cerr << "[!] Failed to start audio playback device.\n";
            ma_device_uninit(&device);
            ma_decoder_uninit(&audio_ctx.decoder);
            pump_running.store(false);
            stream.close();
            if (alert_pump.joinable()) alert_pump.join();
            return 1;
        }

        std::cout << "[+] Streaming FLAC audio ("
                  << audio_ctx.decoder.outputSampleRate << " Hz, "
                  << audio_ctx.decoder.outputChannels << " ch) — Press Ctrl+C to stop.\n";

        // 10. Monitor playback and display real-time streaming telemetry
        while (!audio_ctx.playback_finished.load() && !g_interrupted.load()) {
            lt::torrent_status st = handle.status();
            double const seconds_played =
                (audio_ctx.decoder.outputSampleRate > 0)
                    ? static_cast<double>(audio_ctx.frames_played.load()) /
                          static_cast<double>(audio_ctx.decoder.outputSampleRate)
                    : 0.0;

            int const mins = static_cast<int>(seconds_played) / 60;
            int const secs = static_cast<int>(seconds_played) % 60;

            std::cout << "\r[>] Playing "
                      << std::setw(2) << std::setfill('0') << mins << ":"
                      << std::setw(2) << std::setfill('0') << secs
                      << " | Stream Pos: " << format_bytes(stream.cursor())
                      << " / " << format_bytes(stream.file_size())
                      << " | RAM Pieces: " << stream.cached_piece_count() << "/" << total_track_pieces
                      << " | DL: " << (st.download_payload_rate / 1024) << " KiB/s   "
                      << std::flush;

            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }

        std::cout << "\n[+] Playback finished. Cleaning up in-memory buffers...\n";

        // Clean shutdown
        pump_running.store(false);
        stream.close();
        ma_device_uninit(&device);
        ma_decoder_uninit(&audio_ctx.decoder);
        if (alert_pump.joinable()) {
            alert_pump.join();
        }

    } catch (std::exception const& e) {
        std::cerr << "\n[!] Exception: " << e.what() << "\n";
        return 1;
    }

    return 0;
}