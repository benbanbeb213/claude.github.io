"""
conan_automation_dood_only.py - Detective Conan Single DoodStream Automation

Upload routing:
  Soft Sub (SS) -> DoodStream  (remuxed .mp4 with faststart, no re-encode)
  Hard Sub (HS) -> DoodStream  (ffmpeg-burned .mp4, English subs auto-selected)

Features:
  - Episode range parsing  (1000 / 1000-1005 / 1000,1005 / mixed)
  - Batch magnet links + select specific files from a torrent
  - Subtitle magnet: separate magnet containing subtitle files only
  - Auto movie/episode detection from filename
  - English subtitle auto-selection via ffprobe
  - External subtitle files (.srt/.ass) matched by episode number
  - 6 Nyaa search strategies before giving up
  - DHT + PEX + LPD for better peer discovery
  - Single git commit+push at end of run
  - Per-file error isolation - 1 failure never kills the batch
  - Upload retries x3 with server URL refresh each attempt
"""

import os
import re
import sys
import glob
import json
import time
import subprocess
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from conan_utils import xor_encrypt
from update import patch_hs, patch_ss, patch_movie_hs, patch_movie_ss, read_html, write_html


# ==============================================================================
# CONFIG
# ==============================================================================

DOODSTREAM_API_KEY  = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID  = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID  = os.environ.get("SOFT_SUB_FOLDER_ID", "")

BASE_EPISODE        = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE           = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE    = os.environ.get("EPISODE_OVERRIDE",   "").strip()
MAGNET_LINKS        = os.environ.get("MAGNET_LINKS",        "").strip()
SUBTITLE_MAGNET     = os.environ.get("SUBTITLE_MAGNET",     "").strip()
CUSTOM_SEARCH       = os.environ.get("CUSTOM_SEARCH",       "").strip()
NYAA_UPLOADER_URL   = os.environ.get("NYAA_UPLOADER_URL",   "").strip()
MOVIE_MODE          = os.environ.get("MOVIE_MODE", "0").strip() == "1"
SELECT_FILES        = os.environ.get("SELECT_FILES", "").strip()

HS_TITLE_TPL        = os.environ.get("HS_TITLE_TPL",       "Detective Conan - {ep} HS")
SS_TITLE_TPL        = os.environ.get("SS_TITLE_TPL",       "Detective Conan - {ep} SS")
MOVIE_HS_TITLE_TPL  = os.environ.get("MOVIE_HS_TITLE_TPL", "Detective Conan Movie - {num} HS")
MOVIE_SS_TITLE_TPL  = os.environ.get("MOVIE_SS_TITLE_TPL", "Detective Conan Movie - {num} SS")

HTML_FILE           = os.environ.get("HTML_FILE", "index.html")

UPLOAD_RETRIES      = 3
RETRY_DELAY         = 15

_dood_server_url    = None
SUB_MAP             = {}  # episode_number -> external subtitle file path


# ==============================================================================
# EPISODE / MOVIE DETECTION
# ==============================================================================

def parse_file_info(filename):
    """
    Auto-detect episode vs movie and extract the number from the filename.
    Returns (number, is_movie).
    """
    base = os.path.basename(filename)

    if MOVIE_MODE:
        m = re.search(r"\bMovie\s*[-]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        return (int(m.group(1)) if m else None), True

    if re.search(r"\b(Movie|Film|OVA)\b", base, re.IGNORECASE):
        m = re.search(r"\b(?:Movie|Film|OVA)\s*[-]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        return (int(m.group(1)) if m else None), True

    m = re.search(r"Detective Conan\s*[-]\s*(\d{3,4})\b", base, re.IGNORECASE)
    if m:
        return int(m.group(1)), False

    m = re.search(r"\b(\d{3,4})\b", base)
    if m:
        return int(m.group(1)), False

    return None, False


def get_auto_episode():
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    return BASE_EPISODE + max(0, (datetime.now() - base_dt).days // 7)


def parse_episode_override(raw):
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
                print(f"  WARNING: bad range '{part}' - skipped", file=sys.stderr)
        else:
            try:
                episodes.append(int(part))
            except ValueError:
                print(f"  WARNING: bad value '{part}' - skipped", file=sys.stderr)

    if not episodes:
        print("  WARNING: no valid episodes - using auto", file=sys.stderr)
        return [get_auto_episode()]

    seen, unique = set(), []
    for ep in episodes:
        if ep not in seen:
            seen.add(ep)
            unique.append(ep)
    return unique


def parse_select_files(raw):
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
                start, end = int(halves[0].strip()), int(halves[1].strip())
                if start > end:
                    start, end = end, start
                parts.append(f"{start}-{end}")
            except ValueError:
                print(f"  WARNING: bad file range '{part}' - skipped", file=sys.stderr)
        else:
            try:
                parts.append(str(int(part)))
            except ValueError:
                print(f"  WARNING: bad file index '{part}' - skipped", file=sys.stderr)
    return ",".join(parts)


# ==============================================================================
# NYAA SEARCH  (6 strategies, most specific to broadest)
# ==============================================================================

def _nyaa_magnets(url):
    try:
        resp = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except Exception as e:
        print(f"    Nyaa fetch error: {e}", file=sys.stderr)
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    return [
        (row, a["href"])
        for row in soup.select("tr.success, tr.default")
        for a in row.find_all("a", href=True)
        if a["href"].startswith("magnet:")
    ]


def _best_magnet(rows_magnets):
    if not rows_magnets:
        return None
    for row, mag in rows_magnets:
        if "1080" in row.get_text():
            return mag
    return rows_magnets[0][1]


def search_nyaa(episode):
    ep3 = str(episode).zfill(3)
    ep4 = str(episode)
    base_uploader = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else ""

    strategies = []
    if CUSTOM_SEARCH:
        strategies.append(("Custom search",
            f"https://nyaa.si/?f=0&c=1_2&q={requests.utils.quote(CUSTOM_SEARCH)}"))
    if base_uploader:
        for q in [f"Detective+Conan+-+{ep4}", f"Detective+Conan+-+{ep3}"]:
            strategies.append(("Custom uploader", f"{base_uploader}?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep3}+1080p",
              f"Detective+Conan+-+{ep4}", f"Detective+Conan+-+{ep3}"]:
        strategies.append(("SubsPlease", f"https://nyaa.si/user/subsplease?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep4}"]:
        strategies.append(("Erai-raws", f"https://nyaa.si/user/Erai-raws?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep4}"]:
        strategies.append(("Global anime-English", f"https://nyaa.si/?f=0&c=1_2&q={q}"))
    strategies.append(("Global fallback",
        f"https://nyaa.si/?f=0&c=0_0&q=Detective+Conan+{ep4}"))

    for name, url in strategies:
        print(f"  [{name}] {url}")
        mag = _best_magnet(_nyaa_magnets(url))
        if mag:
            print(f"  Found via: {name}")
            return mag

    print(f"  Episode {episode} not found after all strategies.", file=sys.stderr)
    return None


# ==============================================================================
# DOWNLOADER
# ==============================================================================

def download_magnet(magnet, select_files=""):
    """
    Download a magnet with aria2c.
    Returns (valid_mkv_files, external_subtitle_files).
    """
    sub_exts = (".srt", ".ass", ".ssa", ".sub", ".vtt")
    before_mkv  = set(glob.glob("**/*.mkv", recursive=True))
    before_subs = set(f for f in glob.glob("**/*", recursive=True)
                      if os.path.splitext(f)[1].lower() in sub_exts)

    print(f"  Downloading: {magnet[:100]}...")

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--bt-enable-lpd=true",
        "--enable-dht=true",
        "--enable-dht6=true",
        "--enable-peer-exchange=true",
        "--max-connection-per-server=8",
        "--split=8",
        "--min-split-size=5M",
        "--file-allocation=none",
        "--bt-stop-timeout=600",
        "--disk-cache=64M",
        "--summary-interval=60",
        "--console-log-level=notice",
    ]
    if select_files:
        cmd.append(f"--select-file={select_files}")
        print(f"  File selection: {select_files}")
    cmd.append(magnet)

    try:
        subprocess.run(cmd, check=True, timeout=7200)
    except subprocess.TimeoutExpired:
        print("  aria2c: timeout - checking for completed files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c exit {e.returncode} - checking for completed files", file=sys.stderr)

    after_mkv  = set(glob.glob("**/*.mkv", recursive=True))
    after_subs = set(f for f in glob.glob("**/*", recursive=True)
                     if os.path.splitext(f)[1].lower() in sub_exts)

    new_mkv  = sorted(after_mkv  - before_mkv,  key=os.path.getmtime)
    new_subs = sorted(after_subs - before_subs)

    valid_mkv  = [f for f in new_mkv  if os.path.getsize(f) > 50 * 1024 * 1024]
    valid_subs = [f for f in new_subs if os.path.getsize(f) > 100]

    skipped = set(new_mkv) - set(valid_mkv)
    if skipped:
        print(f"  Skipped {len(skipped)} .mkv file(s) under 50 MB (likely incomplete):",
              file=sys.stderr)
        for f in skipped:
            print(f"    {f}  ({os.path.getsize(f) // 1024} KB)", file=sys.stderr)

    if valid_subs:
        print(f"  External subtitle files found: {len(valid_subs)}")
        for s in valid_subs:
            print(f"    {s}")

    print(f"  Valid .mkv files: {valid_mkv or 'none'}")
    return valid_mkv, valid_subs


def download_subtitle_magnet(magnet):
    """
    Download a subtitle-only magnet.
    Returns list of subtitle file paths found after download.
    """
    sub_exts = (".srt", ".ass", ".ssa", ".sub", ".vtt")
    before   = set(f for f in glob.glob("**/*", recursive=True)
                   if os.path.splitext(f)[1].lower() in sub_exts)

    print(f"  [Subtitle Magnet] Downloading: {magnet[:100]}...")

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--bt-enable-lpd=true",
        "--enable-dht=true",
        "--enable-dht6=true",
        "--enable-peer-exchange=true",
        "--max-connection-per-server=8",
        "--split=8",
        "--file-allocation=none",
        "--bt-stop-timeout=300",
        "--summary-interval=30",
        "--console-log-level=notice",
        magnet,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=3600)
    except subprocess.TimeoutExpired:
        print("  [Subtitle Magnet] Timeout - checking for completed files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  [Subtitle Magnet] aria2c exit {e.returncode} - checking files",
              file=sys.stderr)

    after = set(f for f in glob.glob("**/*", recursive=True)
                if os.path.splitext(f)[1].lower() in sub_exts)
    new_subs = sorted(after - before)
    valid    = [f for f in new_subs if os.path.getsize(f) > 100]

    print(f"  [Subtitle Magnet] Found {len(valid)} subtitle file(s):")
    for s in valid:
        print(f"    {s}")
    return valid


# ==============================================================================
# SUBTITLE MATCHING
# ==============================================================================

def _ep_from_path(path):
    """Extract episode number from a filename, handling zero-padding."""
    base = os.path.basename(path)
    m = re.search(r"Detective Conan\s*[-]\s*(\d{3,4})\b", base, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Zero-padded 3-digit e.g. "0174" -> 174
    m = re.search(r"\b0*(\d{3,4})\b", base)
    if m:
        return int(m.group(1))
    return None


def build_subtitle_map(sub_files):
    """
    Build a dict mapping episode_number -> best subtitle file path.
    Priority: .ass/.ssa > .srt > .sub > .vtt
    English-tagged files are preferred over same-format untagged ones.
    """
    ext_priority = {".ass": 0, ".ssa": 1, ".srt": 2, ".sub": 3, ".vtt": 4}
    sub_map = {}  # ep -> (priority, path)

    for path in sub_files:
        ep = _ep_from_path(path)
        if ep is None:
            continue
        ext  = os.path.splitext(path)[1].lower()
        prio = ext_priority.get(ext, 99)

        base_lower = os.path.basename(path).lower()
        if "english" in base_lower or "_en" in base_lower or ".en." in base_lower:
            prio -= 0.5

        if ep not in sub_map or prio < sub_map[ep][0]:
            sub_map[ep] = (prio, path)

    result = {ep: path for ep, (_, path) in sub_map.items()}
    if result:
        print(f"  Subtitle map: {len(result)} episode(s) have external subs")
        for ep, path in sorted(result.items()):
            print(f"    EP {ep} -> {os.path.basename(path)}")
    return result


# ==============================================================================
# DOODSTREAM UPLOAD
# ==============================================================================

def _get_dood_server():
    global _dood_server_url
    _dood_server_url = None
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


def _rename_dood(file_code, title):
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


def upload_to_doodstream(file_path, title, folder_id=""):
    """Upload an .mp4 to DoodStream with retries. Returns URL or None."""
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
                data = {}
                if folder_id:
                    data["fld_id"] = folder_id
                raw = requests.post(
                    f"{server}?key={DOODSTREAM_API_KEY}",
                    files={"file": (os.path.basename(file_path), fh, "video/mp4")},
                    data=data,
                    timeout=7200,
                )
                print(f"  [DoodStream] HTTP {raw.status_code}")
                try:
                    resp = raw.json()
                except Exception:
                    print(f"  [DoodStream] Non-JSON response (attempt {attempt}): {raw.text[:300]}",
                          file=sys.stderr)
                    resp = {}

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


# ==============================================================================
# FFMPEG - SS remux + HS encoder
# ==============================================================================

def _esc(path):
    p = path.replace("\\", "\\\\").replace("'", "\\'")
    return p.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")


def _remux_ok(path):
    return os.path.exists(path) and os.path.getsize(path) > 10 * 1024 * 1024


def remux_to_mp4(input_file, label):
    """
    Remux .mkv -> .mp4 for SS upload. Three attempts, most-likely first.
    Uses -movflags +faststart so DoodStream can process it immediately.
    Drops subtitles since ASS cannot go into MP4.
    """
    output = f"conan_{label}_ss.mp4"
    if os.path.exists(output):
        os.remove(output)

    print(f"  Remuxing MKV -> MP4 for SS -> {output}")

    attempts = [
        ("video+audio stream copy",          ["-c:v", "copy", "-c:a", "copy"]),
        ("video copy + audio re-encode AAC", ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]),
        ("full re-encode H.264 + AAC",       ["-c:v", "libx264", "-preset", "veryfast",
                                              "-crf", "22", "-c:a", "aac", "-b:a", "192k"]),
    ]

    for desc, codec_flags in attempts:
        if os.path.exists(output):
            os.remove(output)

        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            *codec_flags,
            "-sn",                      # drop subs - ASS cannot go into MP4
            "-movflags", "+faststart",  # moov atom at front - required by DoodStream
            output,
        ]

        print(f"  Remux attempt ({desc})...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)

        if result.returncode == 0 and _remux_ok(output):
            size_mb = os.path.getsize(output) // (1024 * 1024)
            print(f"  Remux OK ({size_mb} MB): {output}")
            return output

        print(f"  Remux failed [{desc}] rc={result.returncode}:", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr[-600:]}", file=sys.stderr)

    print(f"  All 3 remux attempts failed for {input_file}", file=sys.stderr)
    return None


def _find_english_subtitle_index(input_file):
    """
    Use ffprobe to find the English subtitle track index.
    Returns the 0-based index, or -1 if no subtitle streams exist at all.
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-select_streams", "s", input_file],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print("  [ffprobe] Error - defaulting to subtitle index 0", file=sys.stderr)
            return 0

        streams = json.loads(result.stdout).get("streams", [])
        print(f"  [ffprobe] Found {len(streams)} subtitle stream(s):")

        if not streams:
            return -1  # signal to skip HS entirely

        for i, s in enumerate(streams):
            lang  = s.get("tags", {}).get("language", "und")
            title = s.get("tags", {}).get("title", "")
            codec = s.get("codec_name", "?")
            print(f"    [{i}] lang={lang}  codec={codec}  title={title}")

        # Pass 1: exact language=eng tag
        for i, s in enumerate(streams):
            if s.get("tags", {}).get("language", "").lower() == "eng":
                print(f"  [ffprobe] Chose index {i} (language=eng)")
                return i

        # Pass 2: "english" in the title tag
        for i, s in enumerate(streams):
            title = s.get("tags", {}).get("title", "").lower()
            if "english" in title or "eng" in title:
                print(f"  [ffprobe] Chose index {i} (title contains english)")
                return i

        print("  [ffprobe] No English track found - defaulting to index 0", file=sys.stderr)
        return 0

    except Exception as e:
        print(f"  [ffprobe] Error: {e} - defaulting to index 0", file=sys.stderr)
        return 0


def hardsub(input_file, label, external_sub=None):
    """
    Burn subtitles into video using ffmpeg.

    If external_sub is provided (.srt/.ass from the subtitle magnet or torrent),
    it is used directly - no ffprobe scan needed.

    Otherwise, ffprobe scans the .mkv for embedded subtitle streams and picks
    the English one. Returns None if no subtitles available at all.

    Output: conan_{label}_hs.mp4
    """
    output = f"conan_{label}_hs.mp4"

    if external_sub:
        print(f"  [ffmpeg] Using external subtitle: {os.path.basename(external_sub)}")
        esc_sub = _esc(external_sub)
        vf_list = [
            f"subtitles='{esc_sub}'",
            f"subtitles={esc_sub}",
        ]
    else:
        sub_index = _find_english_subtitle_index(input_file)
        if sub_index == -1:
            print(f"  [ffmpeg] No subtitle streams in file - skipping HS",
                  file=sys.stderr)
            return None
        esc_sub = _esc(input_file)
        print(f"  [ffmpeg] Using embedded subtitle index {sub_index}")
        vf_list = [
            f"subtitles='{esc_sub}':si={sub_index}",
            f"subtitles={esc_sub}:si={sub_index}",
            f"subtitles='{esc_sub}'",
            f"subtitles={esc_sub}",
        ]

    print(f"  [ffmpeg] Hard-subbing -> {output}")
    for vf in vf_list:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        size_mb = os.path.getsize(output) // (1024 * 1024) if os.path.exists(output) else 0
        if result.returncode == 0 and size_mb > 10:
            print(f"  [ffmpeg] Hard-sub done ({size_mb} MB): {output}")
            return output
        elif result.returncode == 0 and size_mb <= 10:
            print(f"  [ffmpeg] Output too small ({size_mb} MB) - likely corrupt",
                  file=sys.stderr)
        print(f"  [ffmpeg] Attempt failed (rc={result.returncode}):", file=sys.stderr)
        if result.stderr:
            print(f"  {result.stderr[-500:]}", file=sys.stderr)

    # Clean up partial output
    if os.path.exists(output):
        try:
            os.remove(output)
        except OSError:
            pass
    print(f"  [ffmpeg] Hard-sub FAILED for {label}", file=sys.stderr)
    return None


# ==============================================================================
# PER-FILE PROCESSING
# ==============================================================================

def process_file(mkv_file):
    """
    Process one .mkv file end-to-end.
      SS -> remux .mkv to .mp4 (stream copy) -> DoodStream
      HS -> ffmpeg burn English subs -> .mp4  -> DoodStream
    Returns (num, is_movie, hs_url, ss_url). Never raises.
    """
    num, is_movie = parse_file_info(mkv_file)

    if num is None:
        num      = get_auto_episode()
        is_movie = MOVIE_MODE
        print(f"  Could not parse number - using calculated: EP {num}")
    else:
        kind = "Movie" if is_movie else "Episode"
        print(f"  Auto-detected: {kind} {num}  ({os.path.basename(mkv_file)})")

    label   = f"m{num}" if is_movie else str(num)
    ss_url  = None
    hs_url  = None
    ss_file = None
    hs_file = None

    # Look up any external subtitle matched to this episode
    ext_sub = SUB_MAP.get(num) if num else None
    if ext_sub:
        print(f"  External sub matched: {os.path.basename(ext_sub)}")

    # -- Soft Sub: remux -> DoodStream -------------------------------------
    try:
        ss_file = remux_to_mp4(mkv_file, label)
        if ss_file:
            ss_title = MOVIE_SS_TITLE_TPL.format(num=num) if is_movie \
                       else SS_TITLE_TPL.format(ep=num)
            ss_url   = upload_to_doodstream(ss_file, ss_title, SOFT_SUB_FOLDER_ID)
        else:
            print("  SS skipped - remux failed", file=sys.stderr)
    except Exception as e:
        print(f"  SS exception: {e}", file=sys.stderr)
    finally:
        if ss_file and os.path.exists(ss_file):
            try:
                os.remove(ss_file)
            except OSError:
                pass

    # -- Hard Sub: burn subs -> DoodStream ---------------------------------
    try:
        hs_file = hardsub(mkv_file, label, external_sub=ext_sub)
        if hs_file:
            hs_title = MOVIE_HS_TITLE_TPL.format(num=num) if is_movie \
                       else HS_TITLE_TPL.format(ep=num)
            hs_url   = upload_to_doodstream(hs_file, hs_title, HARD_SUB_FOLDER_ID)
        else:
            print("  HS skipped - no subtitles available", file=sys.stderr)
    except Exception as e:
        print(f"  HS exception: {e}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try:
                os.remove(hs_file)
            except OSError:
                pass

    try:
        os.remove(mkv_file)
    except OSError:
        pass

    return num, is_movie, hs_url, ss_url


# ==============================================================================
# HTML PATCHING + GIT
# ==============================================================================

def patch_html_batch(results):
    if not any(hs or ss for _, _m, hs, ss in results):
        print("\nNo URLs to patch - index.html unchanged.")
        return False

    html = read_html()
    for num, is_movie, hs_url, ss_url in results:
        if is_movie:
            if hs_url:
                html = patch_movie_hs(html, num, hs_url)
            if ss_url:
                html = patch_movie_ss(html, num, ss_url)
        else:
            if hs_url:
                html = patch_hs(html, num, hs_url)
            if ss_url:
                html = patch_ss(html, num, ss_url)
    write_html(html)
    return True


def git_commit_push(results):
    ep_parts  = [str(n) for n, m, hs, ss in results if not m and (hs or ss)]
    mov_parts = [f"M{n}" for n, m, hs, ss in results if m     and (hs or ss)]
    label     = ", ".join(sorted(ep_parts, key=int) + mov_parts) or "unknown"

    try:
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"],             check=True)
        subprocess.run(["git", "add", HTML_FILE],                                     check=True)
        subprocess.run(["git", "commit", "-m", f"chore: add links for {label}"],      check=True)
        rebase = subprocess.run(["git", "pull", "--rebase"],
                                capture_output=True, text=True)
        if rebase.returncode != 0:
            print(f"  Git rebase warning: {rebase.stderr.strip()}", file=sys.stderr)
        subprocess.run(["git", "push"], check=True)
        print(f"\n  Git pushed: {label}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ==============================================================================
# HELPERS
# ==============================================================================

def parse_magnet_list(raw):
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    global SUB_MAP
    all_mkv  = []
    all_subs = []

    # -- Step 0: download subtitle magnet first (if provided) --------------
    if SUBTITLE_MAGNET:
        print("\n-- Downloading subtitle magnet --")
        subtitle_files = download_subtitle_magnet(SUBTITLE_MAGNET)
        all_subs.extend(subtitle_files)
        if subtitle_files:
            print(f"  Subtitle magnet done - {len(subtitle_files)} file(s) ready")
        else:
            print("  WARNING: subtitle magnet produced no subtitle files",
                  file=sys.stderr)

    # -- Source: batch magnet links ----------------------------------------
    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s) | Movie mode: {MOVIE_MODE}")
        for i, magnet in enumerate(magnets, 1):
            print(f"\n[{i}/{len(magnets)}] Downloading...")
            new_files, new_subs = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not new_files:
                print("  No valid .mkv files - skipping this magnet", file=sys.stderr)
            else:
                all_mkv.extend(new_files)
                all_subs.extend(new_subs)

    # -- Source: Nyaa search by episode number(s) --------------------------
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        if not EPISODE_OVERRIDE.strip():
            print(f"Auto mode - episode {episodes[0]} (calculated) | Movie mode: {MOVIE_MODE}")
        else:
            print(f"Episode mode - {len(episodes)} ep(s): {episodes} | Movie mode: {MOVIE_MODE}")

        not_found = []
        for ep in episodes:
            print(f"\n-- Searching episode {ep} --")
            magnet = search_nyaa(ep)
            if not magnet:
                not_found.append(ep)
                continue
            new_files, new_subs = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not new_files:
                print(f"  No valid .mkv files for episode {ep}", file=sys.stderr)
            else:
                all_mkv.extend(new_files)
                all_subs.extend(new_subs)

        if not_found:
            print(f"\n  Not found on Nyaa: {not_found}", file=sys.stderr)

    if not all_mkv:
        print("Nothing to process.")
        sys.exit(0)

    # -- Build subtitle map from all collected external subs ---------------
    if all_subs:
        SUB_MAP = build_subtitle_map(all_subs)

    # -- Process every file ------------------------------------------------
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

    # -- Patch HTML + git push once for the whole batch --------------------
    if results:
        changed = patch_html_batch(results)
        if changed:
            git_commit_push(results)

    # -- Summary -----------------------------------------------------------
    print("\n" + "="*60)
    print("RUN SUMMARY")
    print("="*60)
    for num, is_movie, hs_url, ss_url in results:
        kind = "Movie" if is_movie else "EP"
        ss   = "OK  " if ss_url else "FAIL"
        hs   = "OK  " if hs_url else "FAIL"
        print(f"  {kind} {num:>4}  |  SS (DoodStream): {ss}  |  HS (DoodStream): {hs}")

    failed = [n for n, _m, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  Fully failed: {failed}")
        sys.exit(1)
    else:
        print(f"\n  All {len(results)} file(s) processed.")


if __name__ == "__main__":
    main()
