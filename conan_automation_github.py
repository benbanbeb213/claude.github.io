"""
conan_automation_github.py — Detective Conan Ultimate Studio Automation

Upload routing:
  Soft Sub (SS) → StreamP2P  via TUS protocol  (original .mkv, no re-encode)
  Hard Sub (HS) → DoodStream  via HTTP upload   (ffmpeg-burned .mp4)

Downloader improvements:
  • 6 Nyaa search strategies tried in order before giving up
  • DHT + PEX + LPD enabled for better peer discovery
  • Recursive .mkv detection across subdirectories
  • Per-file timeout + graceful partial-file recovery

All prior features preserved:
  • Episode range parsing  (1000 / 1000-1005 / 1000,1005 / mixed)
  • Batch magnet links
  • Auto movie/episode detection from filename
  • Single git commit+push at end of run
  • Per-file error isolation — 1 failure never kills the batch
  • Upload retries with backoff
"""

import os
import re
import sys
import glob
import time
import base64
import subprocess
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from tusclient import client as tus_client

from conan_utils import xor_encrypt
from update import patch_hs, patch_ss, patch_movie_hs, patch_movie_ss, read_html, write_html


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# DoodStream — Hard Subs only
DOODSTREAM_API_KEY  = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID  = os.environ.get("HARD_SUB_FOLDER_ID", "")

# StreamP2P — Soft Subs only
STREAMP2P_API_KEY   = os.environ.get("STREAMP2P_API_KEY", "2a82d855a5801d4f32c498f8")

# Episode tracking
BASE_EPISODE        = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE           = os.environ.get("BASE_DATE", "2026-03-14")

# Input controls
EPISODE_OVERRIDE    = os.environ.get("EPISODE_OVERRIDE",   "").strip()
MAGNET_LINKS        = os.environ.get("MAGNET_LINKS",        "").strip()
CUSTOM_SEARCH       = os.environ.get("CUSTOM_SEARCH",       "").strip()
NYAA_UPLOADER_URL   = os.environ.get("NYAA_UPLOADER_URL",   "").strip()
MOVIE_MODE          = os.environ.get("MOVIE_MODE", "0").strip() == "1"
SELECT_FILES        = os.environ.get("SELECT_FILES", "").strip()  # e.g. "32" / "32-35" / "32,40"

# DoodStream title templates  — {ep} or {num}
HS_TITLE_TPL        = os.environ.get("HS_TITLE_TPL",       "Detective Conan - {ep} HS")
SS_TITLE_TPL        = os.environ.get("SS_TITLE_TPL",       "Detective Conan - {ep} SS")
MOVIE_HS_TITLE_TPL  = os.environ.get("MOVIE_HS_TITLE_TPL", "Detective Conan Movie - {num} HS")
MOVIE_SS_TITLE_TPL  = os.environ.get("MOVIE_SS_TITLE_TPL", "Detective Conan Movie - {num} SS")

HTML_FILE           = os.environ.get("HTML_FILE", "index.html")

UPLOAD_RETRIES      = 3
RETRY_DELAY         = 15        # seconds between retries
TUS_CHUNK_SIZE      = 52_428_800  # 50 MB chunks for StreamP2P TUS

_dood_server_url    = None      # cached DoodStream upload server URL


# ══════════════════════════════════════════════════════════════════════════════
# EPISODE / MOVIE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def parse_file_info(filename: str) -> tuple:
    """
    Auto-detect episode vs movie and extract the number from the filename.
    Returns (number, is_movie).

    Movie keywords:  Movie, Film, OVA  anywhere in the filename
    Episode pattern: Detective Conan - 1194 (3–4 digit number after dash)
    MOVIE_MODE=1:    forces all files to be treated as movies
    """
    base = os.path.basename(filename)

    if MOVIE_MODE:
        m = re.search(r"\bMovie\s*[-\u2013]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        return (int(m.group(1)) if m else None), True

    if re.search(r"\b(Movie|Film|OVA)\b", base, re.IGNORECASE):
        m = re.search(r"\b(?:Movie|Film|OVA)\s*[-\u2013]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        return (int(m.group(1)) if m else None), True

    m = re.search(r"Detective Conan\s*[-\u2013]\s*(\d{3,4})\b", base, re.IGNORECASE)
    if m:
        return int(m.group(1)), False

    m = re.search(r"\b(\d{3,4})\b", base)
    if m:
        return int(m.group(1)), False

    return None, False


def get_auto_episode() -> int:
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    return BASE_EPISODE + max(0, (datetime.now() - base_dt).days // 7)


def parse_episode_override(raw: str) -> list:
    """
    Parse EPISODE_OVERRIDE into a sorted, deduplicated list of episode numbers.

      "1000"              → [1000]
      "1000-1005"         → [1000, 1001, 1002, 1003, 1004, 1005]
      "1000,1005"         → [1000, 1005]
      "1000,1003-1005"    → [1000, 1003, 1004, 1005]
      ""                  → [auto-calculated this week's episode]
    """
    raw = raw.strip()
    if not raw:
        return [get_auto_episode()]

    episodes = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            halves = part.split("-", 1)
            try:
                start, end = int(halves[0].strip()), int(halves[1].strip())
                if start > end:
                    start, end = end, start
                episodes.extend(range(start, end + 1))
            except ValueError:
                print(f"  WARNING: bad range '{part}' — skipped", file=sys.stderr)
        else:
            try:
                episodes.append(int(part))
            except ValueError:
                print(f"  WARNING: bad episode '{part}' — skipped", file=sys.stderr)

    if not episodes:
        print("  WARNING: no valid episodes parsed — using auto", file=sys.stderr)
        return [get_auto_episode()]

    seen, unique = set(), []
    for ep in episodes:
        if ep not in seen:
            seen.add(ep)
            unique.append(ep)
    return unique


# ══════════════════════════════════════════════════════════════════════════════
# NYAA SEARCH  (6 strategies, most specific → broadest)
# ══════════════════════════════════════════════════════════════════════════════

def _nyaa_magnets(url: str) -> list:
    """Fetch a Nyaa page and return all magnet links found."""
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"    Nyaa fetch error: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    magnets = []
    for row in soup.select("tr.success, tr.default"):
        for a in row.find_all("a", href=True):
            if a["href"].startswith("magnet:"):
                magnets.append((row, a["href"]))
    return magnets


def _best_magnet(rows_magnets: list, prefer_1080: bool = True) -> str | None:
    """From a list of (row, magnet) pairs, prefer 1080p rows, return first magnet."""
    if not rows_magnets:
        return None
    if prefer_1080:
        for row, mag in rows_magnets:
            text = row.get_text()
            if "1080" in text:
                return mag
    # fallback: just return the very first magnet found
    return rows_magnets[0][1]


def search_nyaa(episode: int) -> str | None:
    """
    Try 6 search strategies in order.  Returns the first magnet found.

    Strategy priority:
      1. Custom search query (if provided)
      2. Custom uploader profile + episode number
      3. SubsPlease user page  (most reliable for recent episodes)
      4. Erai-raws user page
      5. Global Nyaa search — anime-English category
      6. Global Nyaa search — all categories (broadest fallback)
    """
    ep3  = str(episode).zfill(3)    # "001" – "999"
    ep4  = str(episode)             # "1000"+"

    base_uploader = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else ""

    strategies = []

    # 1 — custom search query
    if CUSTOM_SEARCH:
        q = requests.utils.quote(CUSTOM_SEARCH)
        strategies.append(("Custom search", f"https://nyaa.si/?f=0&c=1_2&q={q}"))

    # 2 — custom uploader profile
    if base_uploader:
        for q in [f"Detective+Conan+-+{ep4}", f"Detective+Conan+-+{ep3}"]:
            strategies.append((f"Custom uploader ({q})", f"{base_uploader}?f=0&c=0_0&q={q}"))

    # 3 — SubsPlease (default, best for recent)
    for q in [f"Detective+Conan+-+{ep4}+1080p",
              f"Detective+Conan+-+{ep3}+1080p",
              f"Detective+Conan+-+{ep4}",
              f"Detective+Conan+-+{ep3}"]:
        strategies.append((f"SubsPlease ({q})",
                            f"https://nyaa.si/user/subsplease?f=0&c=0_0&q={q}"))

    # 4 — Erai-raws fallback
    for q in [f"Detective+Conan+-+{ep4}+1080p",
              f"Detective+Conan+-+{ep4}"]:
        strategies.append((f"Erai-raws ({q})",
                            f"https://nyaa.si/user/Erai-raws?f=0&c=0_0&q={q}"))

    # 5 — Global anime-English category
    for q in [f"Detective+Conan+-+{ep4}+1080p",
              f"Detective+Conan+-+{ep4}"]:
        strategies.append((f"Global anime-English ({q})",
                            f"https://nyaa.si/?f=0&c=1_2&q={q}"))

    # 6 — Absolute broadest fallback
    strategies.append(("Global all-categories",
                        f"https://nyaa.si/?f=0&c=0_0&q=Detective+Conan+{ep4}"))

    for name, url in strategies:
        print(f"  [{name}] {url}")
        pairs = _nyaa_magnets(url)
        mag   = _best_magnet(pairs, prefer_1080=True)
        if mag:
            print(f"  Found via: {name}")
            return mag

    print(f"  Episode {episode} not found on Nyaa after all strategies.", file=sys.stderr)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DOWNLOADER  (aria2c with full BitTorrent options)
# ══════════════════════════════════════════════════════════════════════════════

def parse_select_files(raw: str) -> str:
    """
    Parse SELECT_FILES into an aria2c --select-file= string.

    Formats accepted (same style as episode override):
      "32"         -> "32"        single file
      "32-35"      -> "32-35"     range of files
      "32,40"      -> "32,40"     specific files
      "32,40-42"   -> "32,40-42"  mixed
      ""           -> ""          blank = download all files

    aria2c accepts this string directly, e.g. --select-file=32,40-42
    """
    raw = raw.strip()
    if not raw:
        return ""

    parts = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            halves = part.split("-", 1)
            try:
                start = int(halves[0].strip())
                end   = int(halves[1].strip())
                if start > end:
                    start, end = end, start
                parts.append(f"{start}-{end}")
            except ValueError:
                print(f"  WARNING: bad file range '{part}' — skipped", file=sys.stderr)
        else:
            try:
                parts.append(str(int(part)))
            except ValueError:
                print(f"  WARNING: bad file index '{part}' — skipped", file=sys.stderr)

    return ",".join(parts)


def download_magnet(magnet: str, select_files: str = "") -> list:
    """
    Download a magnet with aria2c.
    Returns list of all new .mkv files found afterwards (recursive).

    Key flags explained:
      --seed-time=0             stop seeding immediately after download
      --bt-enable-lpd           Local Peer Discovery  (finds peers on LAN)
      --enable-dht              Distributed Hash Table (finds peers without tracker)
      --enable-peer-exchange    PEX — peers share peer lists with each other
      --bt-request-peer-speed-limit  caps how aggressively we ask for peers
      --max-connection-per-server=8  more connections per source = faster
      --min-split-size=5M       split files aggressively into parallel streams
      --file-allocation=none    skip pre-allocation, start downloading immediately
      --bt-stop-timeout=600     give up if stalled for 10 minutes
      --disk-cache=64M          write buffering to reduce I/O overhead
    """
    before = set(glob.glob("**/*.mkv", recursive=True))
    print(f"  Downloading: {magnet[:100]}...")

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--bt-enable-lpd=true",
        "--enable-dht=true",
        "--enable-dht6=true",
        "--enable-peer-exchange=true",
        "--bt-request-peer-speed-limit=10M",
        "--max-connection-per-server=8",
        "--split=8",
        "--min-split-size=5M",
        "--file-allocation=none",
        "--bt-stop-timeout=600",
        "--disk-cache=64M",
        "--summary-interval=60",    # print progress every 60 s
        "--console-log-level=notice",
    ]
    if select_files:
        cmd.append(f"--select-file={select_files}")
        print(f"  File selection: {select_files}")
    cmd.append(magnet)

    try:
        subprocess.run(cmd, check=True, timeout=7200)
    except subprocess.TimeoutExpired:
        print("  aria2c: 2-hour timeout reached — checking for completed files",
              file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c exit code {e.returncode} — checking for completed files",
              file=sys.stderr)

    after  = set(glob.glob("**/*.mkv", recursive=True))
    new    = sorted(after - before, key=os.path.getmtime)

    # Filter out tiny/corrupt files (< 50 MB is almost certainly incomplete)
    valid  = [f for f in new if os.path.getsize(f) > 50 * 1024 * 1024]
    skipped = set(new) - set(valid)
    if skipped:
        print(f"  Skipped {len(skipped)} file(s) under 50 MB (likely incomplete):",
              file=sys.stderr)
        for f in skipped:
            print(f"    {f}  ({os.path.getsize(f) // 1024} KB)", file=sys.stderr)

    print(f"  Valid .mkv files: {valid or 'none'}")
    return valid


# ══════════════════════════════════════════════════════════════════════════════
# STREAMP2P — Soft Sub uploader  (TUS protocol, original .mkv)
# ══════════════════════════════════════════════════════════════════════════════

def _streamp2p_video_url(data: dict) -> str:
    """
    Extract the watchable video URL from the StreamP2P initial API response.
    Tries several common field names; falls back to constructing from accessToken.
    """
    # Direct fields the API might return
    for field in ("embedUrl", "embed_url", "watchUrl", "watch_url",
                  "videoUrl", "video_url", "url", "playUrl", "play_url"):
        if data.get(field):
            return data[field]

    # Some APIs embed the video ID in the access token (JWT middle section)
    access_token = data.get("accessToken", "")
    if access_token:
        # Try JWT payload decode (base64 middle part)
        parts = access_token.split(".")
        if len(parts) == 3:
            try:
                payload = parts[1] + "=="   # add padding
                decoded = base64.urlsafe_b64decode(payload).decode("utf-8", errors="ignore")
                import json
                payload_data = json.loads(decoded)
                for id_field in ("videoId", "video_id", "id", "vid"):
                    if payload_data.get(id_field):
                        return f"https://streamp2p.com/v/{payload_data[id_field]}"
            except Exception:
                pass

    # Try to extract ID from TUS URL itself
    tus_url = data.get("tusUrl", "")
    if tus_url:
        # TUS URLs often end in /uploads/{id} or /video/{id}
        m = re.search(r"/(?:uploads?|videos?|files?)/([a-zA-Z0-9_\-]+)/?$", tus_url)
        if m:
            return f"https://streamp2p.com/v/{m.group(1)}"

    # Return empty string — caller will log a warning
    return ""


def upload_to_streamp2p(file_path: str, title: str) -> str | None:
    """
    Upload file to StreamP2P via TUS protocol.

    Steps:
      1. GET /api/v1/video/upload  →  { tusUrl, accessToken, ... }
      2. TUS upload the .mkv in 50 MB chunks
      3. Extract and return the video embed/watch URL

    Retries the full sequence up to UPLOAD_RETRIES times on failure.
    The original .mkv is uploaded directly — no remux, no re-encode.
    """
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  [StreamP2P] Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            # ── Step 1: get TUS endpoint + access token ───────────────────
            print(f"  [StreamP2P] Getting upload token (attempt {attempt})...")
            init_resp = requests.get(
                "https://streamp2p.com/api/v1/video/upload",
                headers={
                    "api-token": STREAMP2P_API_KEY,
                    "Accept":    "application/json",
                },
                timeout=30,
            )

            print(f"  [StreamP2P] Init status: {init_resp.status_code}")

            if init_resp.status_code != 200:
                print(f"  [StreamP2P] Init failed: {init_resp.text[:300]}", file=sys.stderr)
                raise RuntimeError(f"Init HTTP {init_resp.status_code}")

            data         = init_resp.json()
            print(f"  [StreamP2P] Init response keys: {list(data.keys())}")

            tus_url      = data.get("tusUrl") or data.get("tus_url") or data.get("uploadUrl")
            access_token = data.get("accessToken") or data.get("access_token") or ""

            if not tus_url:
                print(f"  [StreamP2P] No TUS URL in response: {data}", file=sys.stderr)
                raise RuntimeError("No TUS URL")

            print(f"  [StreamP2P] TUS URL: {tus_url}")

            # ── Step 2: TUS upload ────────────────────────────────────────
            filename = os.path.basename(file_path)
            print(f"  [StreamP2P] Starting TUS upload: {filename}")

            tc       = tus_client.TusClient(tus_url)
            uploader = tc.uploader(
                file_path=file_path,
                chunk_size=TUS_CHUNK_SIZE,
                metadata={
                    "accessToken": access_token,
                    "filename":    filename,
                    "filetype":    "video/x-matroska",
                },
            )
            uploader.upload()
            print(f"  [StreamP2P] TUS upload complete")

            # ── Step 3: extract video URL ─────────────────────────────────
            video_url = _streamp2p_video_url(data)
            if video_url:
                print(f"  [StreamP2P] Video URL: {video_url}")
                return video_url
            else:
                # Upload succeeded but we couldn't determine the URL.
                # Log the full response so the user can check the dashboard.
                print(f"  [StreamP2P] Upload OK but could not determine video URL.",
                      file=sys.stderr)
                print(f"  [StreamP2P] Full response: {data}", file=sys.stderr)
                print(f"  [StreamP2P] Check your StreamP2P dashboard for the video.",
                      file=sys.stderr)
                # Return a placeholder so HTML patching is skipped (None = skip)
                return None

        except Exception as e:
            print(f"  [StreamP2P] Attempt {attempt} failed: {e}", file=sys.stderr)
            if attempt < UPLOAD_RETRIES:
                print(f"  [StreamP2P] Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

    print(f"  [StreamP2P] All {UPLOAD_RETRIES} attempts failed for '{title}'",
          file=sys.stderr)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DOODSTREAM — Hard Sub uploader
# ══════════════════════════════════════════════════════════════════════════════

def _get_dood_server() -> str | None:
    global _dood_server_url
    _dood_server_url = None   # always refresh
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            _dood_server_url = resp["result"]
            return _dood_server_url
    except Exception as e:
        print(f"  [DoodStream] Server lookup error: {e}", file=sys.stderr)
    return None


def _rename_dood(file_code: str, title: str) -> None:
    try:
        resp = requests.get(
            "https://doodapi.co/api/file/rename",
            params={"key": DOODSTREAM_API_KEY, "file_code": file_code, "title": title},
            timeout=15,
        ).json()
        if resp.get("status") == 200:
            print(f"  [DoodStream] Title set: '{title}'")
        else:
            print(f"  [DoodStream] Rename response: {resp}", file=sys.stderr)
    except Exception as e:
        print(f"  [DoodStream] Rename error: {e}", file=sys.stderr)


def upload_to_doodstream(file_path: str, title: str, folder_id: str = "") -> str | None:
    """
    Upload an .mp4 to DoodStream, then set its title via the rename API.
    Returns the embed/download URL or None.
    """
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  [DoodStream] Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        server = _get_dood_server()
        if not server:
            print(f"  [DoodStream] No server (attempt {attempt})", file=sys.stderr)
            time.sleep(RETRY_DELAY)
            continue

        try:
            with open(file_path, "rb") as fh:
                data = {"api_key": DOODSTREAM_API_KEY}
                if folder_id:
                    data["fld_id"] = folder_id
                resp = requests.post(
                    server,
                    files={"file": (os.path.basename(file_path), fh, "video/mp4")},
                    data=data,
                    timeout=7200,
                ).json()

            if resp.get("status") == 200:
                result    = resp["result"][0]
                file_code = result.get("file_code") or result.get("filecode") or ""
                url       = result.get("download_url") or result.get("embed_url") or ""
                if file_code:
                    _rename_dood(file_code, title)
                print(f"  [DoodStream] Uploaded: {url}")
                return url
            else:
                print(f"  [DoodStream] Bad response (attempt {attempt}): {resp}",
                      file=sys.stderr)

        except Exception as e:
            print(f"  [DoodStream] Exception (attempt {attempt}): {e}", file=sys.stderr)

        if attempt < UPLOAD_RETRIES:
            print(f"  [DoodStream] Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    print(f"  [DoodStream] All {UPLOAD_RETRIES} attempts failed for '{title}'",
          file=sys.stderr)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# FFMPEG — Hard Sub encoder
# ══════════════════════════════════════════════════════════════════════════════

def _esc(path: str) -> str:
    """Escape a file path for use inside ffmpeg subtitles= filter."""
    p = path.replace("\\", "\\\\").replace("'", "\\'")
    return p.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")


def _find_english_subtitle_index(input_file: str) -> int:
    """
    Use ffprobe to scan all subtitle streams and return the 0-based index
    of the first English one.

    Returns the index (e.g. 0, 1, 2) so ffmpeg can use:
      subtitles=file.mkv:si=<index>

    Falls back to 0 (first track) if:
      - ffprobe is not available
      - no subtitle stream is tagged as English
      - ffprobe errors out for any reason
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-select_streams", "s",   # subtitle streams only
                input_file,
            ],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print("  [ffprobe] Non-zero exit — defaulting to subtitle index 0",
                  file=sys.stderr)
            return 0

        import json
        data    = json.loads(result.stdout)
        streams = data.get("streams", [])

        print(f"  [ffprobe] Found {len(streams)} subtitle stream(s):")
        for i, s in enumerate(streams):
            lang  = s.get("tags", {}).get("language", "und")
            title = s.get("tags", {}).get("title", "")
            codec = s.get("codec_name", "?")
            print(f"    [{i}] lang={lang}  codec={codec}  title={title}")

        # First pass: exact "eng" tag
        for i, s in enumerate(streams):
            lang = s.get("tags", {}).get("language", "").lower()
            if lang == "eng":
                print(f"  [ffprobe] Chose subtitle index {i} (language=eng)")
                return i

        # Second pass: title contains "english" (some files tag it this way)
        for i, s in enumerate(streams):
            title = s.get("tags", {}).get("title", "").lower()
            if "english" in title or "eng" in title:
                print(f"  [ffprobe] Chose subtitle index {i} (title contains english)")
                return i

        print("  [ffprobe] No English subtitle found — defaulting to index 0",
              file=sys.stderr)
        return 0

    except Exception as e:
        print(f"  [ffprobe] Error: {e} — defaulting to subtitle index 0",
              file=sys.stderr)
        return 0


def hardsub(input_file: str, label: str) -> str | None:
    """
    Burn subtitles into video using ffmpeg.
    Automatically picks the English subtitle track via ffprobe.
    Falls back to track 0 if English isn't found.
    Output: conan_{label}_hs.mp4
    """
    output    = f"conan_{label}_hs.mp4"
    sub_index = _find_english_subtitle_index(input_file)
    print(f"  [ffmpeg] Hard-subbing with subtitle index {sub_index} -> {output}")

    esc = _esc(input_file)

    # si= selects subtitle stream by index (0-based within subtitle streams)
    for vf in [
        f"subtitles='{esc}':si={sub_index}",
        f"subtitles={esc}:si={sub_index}",
        # Final fallback: no si= (uses default track, in case si= causes issues)
        f"subtitles='{esc}'",
        f"subtitles={esc}",
    ]:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if result.returncode == 0 and os.path.exists(output):
            size_mb = os.path.getsize(output) // (1024 * 1024)
            print(f"  [ffmpeg] Hard-sub done ({size_mb} MB): {output}")
            return output
        print(f"  [ffmpeg] Attempt failed (rc={result.returncode}):", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr[-500:]}", file=sys.stderr)

    print(f"  [ffmpeg] Hard-sub FAILED for {label}", file=sys.stderr)
    return None
# ══════════════════════════════════════════════════════════════════════════════
# PER-FILE PROCESSING
# ══════════════════════════════════════════════════════════════════════════════

def process_file(mkv_file: str):
    """
    Process one .mkv file end-to-end.

      SS  →  upload original .mkv to StreamP2P (no re-encode)
      HS  →  burn subs with ffmpeg → upload .mp4 to DoodStream

    Returns (num, is_movie, hs_url, ss_url).
    Never raises — all exceptions are caught and logged.
    """
    num, is_movie = parse_file_info(mkv_file)

    if num is None:
        num      = get_auto_episode()
        is_movie = MOVIE_MODE
        print(f"  Could not parse number — using calculated: EP {num}")
    else:
        kind = "Movie" if is_movie else "Episode"
        print(f"  Auto-detected: {kind} {num}  ({os.path.basename(mkv_file)})")

    label   = f"m{num}" if is_movie else str(num)
    ss_url  = None
    hs_url  = None
    hs_file = None

    # ── Soft Sub: upload original .mkv directly to StreamP2P ─────────────
    try:
        ss_title = MOVIE_SS_TITLE_TPL.format(num=num) if is_movie else SS_TITLE_TPL.format(ep=num)
        ss_url   = upload_to_streamp2p(mkv_file, ss_title)
    except Exception as e:
        print(f"  SS exception: {e}", file=sys.stderr)

    # ── Hard Sub: burn subs → .mp4 → DoodStream ──────────────────────────
    try:
        hs_file = hardsub(mkv_file, label)
        if hs_file:
            hs_title = MOVIE_HS_TITLE_TPL.format(num=num) if is_movie else HS_TITLE_TPL.format(ep=num)
            hs_url   = upload_to_doodstream(hs_file, hs_title, HARD_SUB_FOLDER_ID)
        else:
            print("  HS skipped — hardsub failed", file=sys.stderr)
    except Exception as e:
        print(f"  HS exception: {e}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try:
                os.remove(hs_file)
            except OSError:
                pass

    # Source .mkv can be removed now — SS was uploaded directly from it
    try:
        os.remove(mkv_file)
    except OSError:
        pass

    return num, is_movie, hs_url, ss_url


# ══════════════════════════════════════════════════════════════════════════════
# HTML PATCHING + GIT
# ══════════════════════════════════════════════════════════════════════════════

def patch_html_batch(results: list) -> bool:
    if not any(hs or ss for _, _m, hs, ss in results):
        print("\nNo URLs to patch — index.html unchanged.")
        return False

    html = read_html()
    for num, is_movie, hs_url, ss_url in results:
        if is_movie:
            if hs_url: html = patch_movie_hs(html, num, hs_url)
            if ss_url: html = patch_movie_ss(html, num, ss_url)
        else:
            if hs_url: html = patch_hs(html, num, hs_url)
            if ss_url: html = patch_ss(html, num, ss_url)
    write_html(html)
    return True


def git_commit_push(results: list) -> None:
    ep_parts  = [str(n) for n, m, hs, ss in results if not m and (hs or ss)]
    mov_parts = [f"M{n}" for n, m, hs, ss in results if m     and (hs or ss)]
    label     = ", ".join(sorted(ep_parts) + mov_parts) or "unknown"

    try:
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"],             check=True)
        subprocess.run(["git", "add", HTML_FILE],                                     check=True)
        subprocess.run(["git", "commit", "-m", f"chore: add links for {label}"],      check=True)
        subprocess.run(["git", "pull", "--rebase"],                                   check=False)
        subprocess.run(["git", "push"],                                                check=True)
        print(f"\n  Git pushed: {label}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_magnet_list(raw: str) -> list:
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    all_mkv = []

    # ── Source: batch magnet links ─────────────────────────────────────────
    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s) | Movie mode: {MOVIE_MODE}")
        for i, magnet in enumerate(magnets, 1):
            print(f"\n[{i}/{len(magnets)}] Downloading...")
            new_files = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not new_files:
                print("  No valid .mkv files — skipping this magnet", file=sys.stderr)
            else:
                all_mkv.extend(new_files)

    # ── Source: Nyaa search by episode number(s) ───────────────────────────
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        if not EPISODE_OVERRIDE.strip():
            print(f"Auto mode — episode {episodes[0]} (calculated) | Movie mode: {MOVIE_MODE}")
        else:
            print(f"Episode mode — {len(episodes)} ep(s): {episodes} | Movie mode: {MOVIE_MODE}")

        not_found = []
        for ep in episodes:
            print(f"\n── Searching episode {ep} ──")
            magnet = search_nyaa(ep)
            if not magnet:
                not_found.append(ep)
                continue
            new_files = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not new_files:
                print(f"  No valid .mkv files for episode {ep}", file=sys.stderr)
            else:
                all_mkv.extend(new_files)

        if not_found:
            print(f"\n  Not found on Nyaa: {not_found}", file=sys.stderr)

    if not all_mkv:
        print("Nothing to process.")
        sys.exit(0)

    # ── Process every file ─────────────────────────────────────────────────
    print(f"\nProcessing {len(all_mkv)} file(s)...")
    results = []
    for i, mkv in enumerate(all_mkv, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(all_mkv)}] {os.path.basename(mkv)}")
        print(f"{'='*60}")
        try:
            results.append(process_file(mkv))
        except Exception as e:
            print(f"  FATAL ERROR: {e}", file=sys.stderr)

    # ── Patch HTML + git push (once for the whole batch) ───────────────────
    if results:
        changed = patch_html_batch(results)
        if changed:
            git_commit_push(results)

    # ── Run summary ────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("RUN SUMMARY")
    print("="*60)
    for num, is_movie, hs_url, ss_url in results:
        kind = "Movie" if is_movie else "EP"
        ss   = "OK  " if ss_url else "FAIL"
        hs   = "OK  " if hs_url else "FAIL"
        print(f"  {kind} {num:>4}  |  SS (StreamP2P): {ss}  |  HS (DoodStream): {hs}")

    failed = [n for n, _m, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  Fully failed (no uploads at all): {failed}")
        sys.exit(1)
    else:
        print(f"\n  All {len(results)} file(s) processed.")


if __name__ == "__main__":
    main()
