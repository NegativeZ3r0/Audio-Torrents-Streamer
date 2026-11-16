# Audio-Torrents-Streamer

A zero-disk-I/O C++20 CLI application that takes a BitTorrent magnet link,
fetches the torrent metadata strictly in RAM, filters for .flac audio files,
prompts the user to select a track, and streams the selected FLAC file
directly from memory to the system audio output device as pieces arrive.

### Dependencies:
- [libtorrent-rasterbar](https://www.libtorrent.org/), version used: v2.1.2
- [boost](), version used: v1.92.0
- [miniaudio](https://miniaud.io/), version used: v0.11.25

### Build Instructions (Linux / macOS):
  ```bash
    g++ -std=c++20 -O2 -DTORRENT_USE_OPENSSL flac_torrent_streamer.cpp -o flac_torrent_streamer \
        $(pkg-config --cflags --libs libtorrent-rasterbar) -lssl -lcrypto -lpthread -ldl -lm
  ```

### Usage:
  ```bash
  ./flac_torrent_streamer "magnet:?xt=urn:btih:..."
  ```

### Demo:
https://github.com/user-attachments/assets/b8543108-6306-4d1f-8208-9b0b6bb2010b


