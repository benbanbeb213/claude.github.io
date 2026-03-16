"""
conan_automation_dood_only.py - Detective Conan Single DoodStream Automation

Both SS and HS upload to DoodStream.
Upload URL format: POST to upload_server_url?API_KEY  (original working format)

Features:
  - Episode range: 1000 / 1000-1005 / 1000,1005 / 1000,1003-1005 / blank=auto
  - Batch magnets (one per line or comma-separated)
  - Select specific files from torrent: 32 / 32-35 / 32,40
  - Subtitle magnet: separate magnet of subtitle files downloaded first
  - Auto movie/episode detection from filename (Movie/OVA/Film keywords)
  - English subtitle auto-selection via ffprobe
  - External subs (.srt/.ass) matched by episode number
  - 6 Nyaa search strategies before giving up
  - SS: remux mkv->mp4 (stream copy, faststart, no re-encode)
  - HS: ffmpeg burn English subs, libx264 veryfast crf22
  - Single git commit+push at end of entire run
  - Per-file error isolation - 1 failure never kills the batch
  - Upload retries x3 with fresh server URL each attempt
  - Title set via DoodStream rename API after upload
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

from update import patch_hs, patch_ss, patch_movie_hs, patch_movie_ss, read_html, write_html


# ==============================================================================
# CONFIG
# ==============================================================================

DOODSTREAM_API_KEY  = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")

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

SUB_MAP             = {}   # episode_number -> external subtitle file path


# ==============================================================================
# EPISODE / MOVIE DETECTION
# ==============================================================================

def parse_file_info(filename):
    """Returns (number, is_movie) from filename."""
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
                pass
        else:
            try:
                parts.append(str(int(part)))
            except ValueError:
                pass
    return ",".join(parts)


# ==============================================================================
# NYAA SEARCH
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
        strategies.append(("Custom",
            f"https://nyaa.si/?f=0&c=1_2&q={requests.utils.quote(CUSTOM_SEARCH)}"))
    if base_uploader:
        for q in [f"Detective+Conan+-+{ep4}", f"Detective+Conan+-+{ep3}"]:
            strategies.append(("Custom uploader",
                f"{base_uploader}?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep3}+1080p",
              f"Detective+Conan+-+{ep4}", f"Detective+Conan+-+{ep3}"]:
        strategies.append(("SubsPlease",
            f"https://nyaa.si/user/subsplease?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep4}"]:
        strategies.append(("Erai-raws",
            f"https://nyaa.si/user/Erai-raws?f=0&c=0_0&q={q}"))
    for q in [f"Detective+Conan+-+{ep4}+1080p", f"Detective+Conan+-+{ep4}"]:
        strategies.append(("Global",
            f"https://nyaa.si/?f=0&c=1_2&q={q}"))
    strategies.append(("Fallback",
        f"https://nyaa.si/?f=0&c=0_0&q=Detective+Conan+{ep4}"))

    for name, url in strategies:
        print(f"  [{name}] {url}")
        mag = _best_magnet(_nyaa_magnets(url))
        if mag:
            print(f"  Found via: {name}")
            return mag
    print(f"  Episode {episode} not found.", file=sys.stderr)
    return None


# ==============================================================================
# DOWNLOADER
# ==============================================================================

def _run_aria2c(magnet, select_files=""):
    cmd = [
        "aria2c", "--seed-time=0",
        "--bt-enable-lpd=true", "--enable-dht=true", "--enable-dht6=true",
        "--enable-peer-exchange=true",
        "--max-connection-per-server=8", "--split=8", "--min-split-size=5M",
        "--file-allocation=none", "--bt-stop-timeout=600",
        "--disk-cache=64M", "--summary-interval=60",
        "--console-log-level=notice",
    ]
    if select_files:
        cmd.append(f"--select-file={select_files}")
        print(f"  File selection: {select_files}")
    cmd.append(magnet)
    try:
        subprocess.run(cmd, check=True, timeout=7200)
    except subprocess.TimeoutExpired:
        print("  aria2c timeout - checking for completed files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c exit {e.returncode} - checking files", file=sys.stderr)


def download_magnet(magnet, select_files=""):
    """Returns (valid_mkv_list, subtitle_file_list)."""
    sub_exts    = (".srt", ".ass", ".ssa", ".sub", ".vtt")
    before_mkv  = set(glob.glob("**/*.mkv", recursive=True))
    before_subs = set(f for f in glob.glob("**/*", recursive=True)
                      if os.path.splitext(f)[1].lower() in sub_exts)

    print(f"  Downloading: {magnet[:100]}...")
    _run_aria2c(magnet, select_files)

    after_mkv  = set(glob.glob("**/*.mkv", recursive=True))
    after_subs = set(f for f in glob.glob("**/*", recursive=True)
                     if os.path.splitext(f)[1].lower() in sub_exts)

    new_mkv  = sorted(after_mkv  - before_mkv,  key=os.path.getmtime)
    new_subs = sorted(after_subs - before_subs)

    valid_mkv  = [f for f in new_mkv  if os.path.getsize(f) > 50  * 1024 * 1024]
    valid_subs = [f for f in new_subs if os.path.getsize(f) > 100]

    skipped = set(new_mkv) - set(valid_mkv)
    if skipped:
        for f in skipped:
            print(f"  Skipped (too small): {f}", file=sys.stderr)
    if valid_subs:
        print(f"  Subtitle files found: {len(valid_subs)}")
    print(f"  Valid .mkv files: {valid_mkv or 'none'}")
    return valid_mkv, valid_subs


def download_subtitle_magnet(magnet):
    """Download subtitle-only magnet. Returns list of subtitle file paths."""
    sub_exts = (".srt", ".ass", ".ssa", ".sub", ".vtt")
    before   = set(f for f in glob.glob("**/*", recursive=True)
                   if os.path.splitext(f)[1].lower() in sub_exts)

    print(f"  [Subtitle Magnet] Downloading: {magnet[:100]}...")
    cmd = [
        "aria2c", "--seed-time=0",
        "--bt-enable-lpd=true", "--enable-dht=true", "--enable-dht6=true",
        "--enable-peer-exchange=true",
        "--max-connection-per-server=8", "--split=8",
        "--file-allocation=none", "--bt-stop-timeout=300",
        "--summary-interval=30", "--console-log-level=notice",
        magnet,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=3600)
    except subprocess.TimeoutExpired:
        print("  [Subtitle Magnet] Timeout - checking files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  [Subtitle Magnet] Exit {e.returncode}", file=sys.stderr)

    after = set(f for f in glob.glob("**/*", recursive=True)
                if os.path.splitext(f)[1].lower() in sub_exts)
    valid = [f for f in sorted(after - before) if os.path.getsize(f) > 100]
    print(f"  [Subtitle Magnet] Found {len(valid)} subtitle file(s)")
    for s in valid:
        print(f"    {s}")
    return valid


# ==============================================================================
# SUBTITLE MATCHING
# ==============================================================================

def _ep_from_path(path):
    base = os.path.basename(path)
    m = re.search(r"Detective Conan\s*[-]\s*(\d{3,4})\b", base, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"\b0*(\d{3,4})\b", base)
    if m:
        return int(m.group(1))
    return None


def build_subtitle_map(sub_files):
    """Maps episode_number -> best subtitle file. .ass > .srt > .sub > .vtt"""
    ext_prio = {".ass": 0, ".ssa": 1, ".srt": 2, ".sub": 3, ".vtt": 4}
    sub_map  = {}
    for path in sub_files:
        ep = _ep_from_path(path)
        if ep is None:
            continue
        prio = float(ext_prio.get(os.path.splitext(path)[1].lower(), 99))
        b    = os.path.basename(path).lower()
        if "english" in b or "_en" in b or ".en." in b:
            prio -= 0.5
        if ep not in sub_map or prio < sub_map[ep][0]:
            sub_map[ep] = (prio, path)
    result = {ep: path for ep, (_, path) in sub_map.items()}
    if result:
        print(f"  Subtitle map: {len(result)} episode(s)")
        for ep, path in sorted(result.items()):
            print(f"    EP {ep} -> {os.path.basename(path)}")
    return result


# ==============================================================================
# DOODSTREAM UPLOAD
# ==============================================================================

def _get_upload_server():
    try:
        resp = requests.get(
            f"https://doodapi.co/api/upload/server?key={DOODSTREAM_API_KEY}",
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            return resp["result"]
        print(f"  [DoodStream] Server error: {resp}", file=sys.stderr)
    except Exception as e:
        print(f"  [DoodStream] Server lookup failed: {e}", file=sys.stderr)
    return None


def _rename_dood(file_code, title):
    try:
        resp = requests.get(
            f"https://doodapi.co/api/file/rename"
            f"?key={DOODSTREAM_API_KEY}"
            f"&file_code={file_code}"
            f"&title={requests.utils.quote(title)}",
            timeout=15,
        ).json()
        if resp.get("status") == 200:
            print(f"  [DoodStream] Title set: '{title}'")
        else:
            print(f"  [DoodStream] Rename error: {resp}", file=sys.stderr)
    except Exception as e:
        print(f"  [DoodStream] Rename failed: {e}", file=sys.stderr)


def upload_to_doodstream(file_path, title):
    """Upload file to DoodStream. Returns embed URL or None."""
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  [DoodStream] Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        server = _get_upload_server()
        if not server:
            print(f"  [DoodStream] No server (attempt {attempt})", file=sys.stderr)
            time.sleep(RETRY_DELAY)
            continue

        try:
            with open(file_path, "rb") as fh:
                raw = requests.post(
                    f"{server}?key={DOODSTREAM_API_KEY}",
                    files={"file": (os.path.basename(file_path), fh, "video/mp4")},
                    timeout=7200,
                )

            print(f"  [DoodStream] HTTP {raw.status_code}")

            try:
                resp = raw.json()
            except Exception:
                print(f"  [DoodStream] Non-JSON response: {raw.text[:300]}",
                      file=sys.stderr)
                if attempt < UPLOAD_RETRIES:
                    time.sleep(RETRY_DELAY)
                continue

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
# FFMPEG
# ==============================================================================

def _esc(path):
    p = path.replace("\\", "\\\\").replace("'", "\\'")
    return p.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")


def remux_to_mp4(input_file, label):
    """
    Remux .mkv -> .mp4 for SS upload.
    - Drops subtitles (ASS cannot go into MP4)
    - Uses -movflags +faststart (moov atom at front, required by DoodStream)
    - Three attempts: stream copy first, then audio re-encode, then full re-encode
    """
    output = f"conan_{label}_ss.mp4"
    if os.path.exists(output):
        os.remove(output)

    print(f"  Remuxing -> {output}")

    attempts = [
        ("stream copy",              ["-c:v", "copy", "-c:a", "copy"]),
        ("copy video + AAC audio",   ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]),
        ("full re-encode H264+AAC",  ["-c:v", "libx264", "-preset", "veryfast",
                                      "-crf", "22", "-c:a", "aac", "-b:a", "192k"]),
    ]

    for desc, codec_flags in attempts:
        if os.path.exists(output):
            os.remove(output)
        cmd = ["ffmpeg", "-y", "-i", input_file, *codec_flags,
               "-sn", "-movflags", "+faststart", output]
        print(f"  Remux attempt ({desc})...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if (result.returncode == 0 and os.path.exists(output)
                and os.path.getsize(output) > 10 * 1024 * 1024):
            print(f"  Remux OK ({os.path.getsize(output) // (1024*1024)} MB)")
            return output
        if result.stderr:
            print(f"  {result.stderr[-300:]}", file=sys.stderr)

    print(f"  All remux attempts failed for {input_file}", file=sys.stderr)
    return None


def _find_english_sub_index(input_file):
    """
    Returns the 0-based subtitle stream index of the English track.
    Returns -1 if the file has NO subtitle streams at all (skip HS).
    Returns  0 as fallback if streams exist but none are tagged English.
    """
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_streams", "-select_streams", "s", input_file],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return 0
        streams = json.loads(r.stdout).get("streams", [])
        print(f"  [ffprobe] {len(streams)} subtitle stream(s)")
        if not streams:
            return -1
        for i, s in enumerate(streams):
            print(f"    [{i}] lang={s.get('tags',{}).get('language','?')} "
                  f"codec={s.get('codec_name','?')} "
                  f"title={s.get('tags',{}).get('title','')}")
        for i, s in enumerate(streams):
            if s.get("tags", {}).get("language", "").lower() == "eng":
                print(f"  [ffprobe] Using index {i} (eng)")
                return i
        for i, s in enumerate(streams):
            t = s.get("tags", {}).get("title", "").lower()
            if "english" in t or "eng" in t:
                print(f"  [ffprobe] Using index {i} (title=english)")
                return i
        print("  [ffprobe] No English - using index 0", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"  [ffprobe] Error: {e} - using 0", file=sys.stderr)
        return 0


def hardsub(input_file, label, external_sub=None):
    """
    Burn subtitles into video.
    Uses external_sub if provided, otherwise scans embedded streams.
    Returns None if no subtitles available.
    Output: conan_{label}_hs.mp4
    """
    output = f"conan_{label}_hs.mp4"

    if external_sub:
        print(f"  [ffmpeg] External sub: {os.path.basename(external_sub)}")
        esc = _esc(external_sub)
        vf_list = [f"subtitles='{esc}'", f"subtitles={esc}"]
    else:
        idx = _find_english_sub_index(input_file)
        if idx == -1:
            print("  [ffmpeg] No subtitle streams - skipping HS", file=sys.stderr)
            return None
        esc = _esc(input_file)
        print(f"  [ffmpeg] Embedded sub index {idx}")
        vf_list = [
            f"subtitles='{esc}':si={idx}",
            f"subtitles={esc}:si={idx}",
            f"subtitles='{esc}'",
            f"subtitles={esc}",
        ]

    print(f"  [ffmpeg] Hard-subbing -> {output}")
    for vf in vf_list:
        cmd = ["ffmpeg", "-y", "-i", input_file, "-vf", vf,
               "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
               "-c:a", "aac", "-b:a", "192k", output]
        r   = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        smb = os.path.getsize(output) // (1024*1024) if os.path.exists(output) else 0
        if r.returncode == 0 and smb > 10:
            print(f"  [ffmpeg] Done ({smb} MB): {output}")
            return output
        if r.returncode == 0 and smb <= 10:
            print(f"  [ffmpeg] Output too small ({smb} MB)", file=sys.stderr)
        if r.stderr:
            print(f"  {r.stderr[-300:]}", file=sys.stderr)

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
    """Process one .mkv: SS + HS -> DoodStream. Returns (num, is_movie, hs_url, ss_url)."""
    num, is_movie = parse_file_info(mkv_file)
    if num is None:
        num      = get_auto_episode()
        is_movie = MOVIE_MODE
        print(f"  Could not parse number - using: EP {num}")
    else:
        kind = "Movie" if is_movie else "Episode"
        print(f"  Detected: {kind} {num} ({os.path.basename(mkv_file)})")

    label   = f"m{num}" if is_movie else str(num)
    ss_url  = None
    hs_url  = None
    ss_file = None
    hs_file = None

    ext_sub = SUB_MAP.get(num) if num else None
    if ext_sub:
        print(f"  External sub matched: {os.path.basename(ext_sub)}")

    # -- SS: remux -> DoodStream -------------------------------------------
    try:
        ss_file = remux_to_mp4(mkv_file, label)
        if ss_file:
            t      = (MOVIE_SS_TITLE_TPL.format(num=num) if is_movie
                      else SS_TITLE_TPL.format(ep=num))
            ss_url = upload_to_doodstream(ss_file, t)
        else:
            print("  SS skipped - remux failed", file=sys.stderr)
    except Exception as e:
        print(f"  SS exception: {e}", file=sys.stderr)
    finally:
        if ss_file and os.path.exists(ss_file):
            try: os.remove(ss_file)
            except OSError: pass

    # -- HS: burn subs -> DoodStream ---------------------------------------
    try:
        hs_file = hardsub(mkv_file, label, external_sub=ext_sub)
        if hs_file:
            t      = (MOVIE_HS_TITLE_TPL.format(num=num) if is_movie
                      else HS_TITLE_TPL.format(ep=num))
            hs_url = upload_to_doodstream(hs_file, t)
        else:
            print("  HS skipped - no subtitles", file=sys.stderr)
    except Exception as e:
        print(f"  HS exception: {e}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try: os.remove(hs_file)
            except OSError: pass

    try: os.remove(mkv_file)
    except OSError: pass

    return num, is_movie, hs_url, ss_url


# ==============================================================================
# HTML PATCHING + GIT
# ==============================================================================

def patch_html_batch(results):
    if not any(hs or ss for _, _m, hs, ss in results):
        print("No URLs to patch - index.html unchanged.")
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


def git_commit_push(results):
    ep_parts  = [str(n) for n, m, hs, ss in results if not m and (hs or ss)]
    mov_parts = [f"M{n}" for n, m, hs, ss in results if m     and (hs or ss)]
    label     = ", ".join(sorted(ep_parts, key=int) + mov_parts) or "unknown"
    try:
        subprocess.run(["git", "config", "user.email",
                        "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name", "GitHub Actions"], check=True)
        subprocess.run(["git", "add", HTML_FILE], check=True)
        subprocess.run(["git", "commit", "-m",
                        f"chore: add links for {label}"], check=True)
        rb = subprocess.run(["git", "pull", "--rebase"],
                            capture_output=True, text=True)
        if rb.returncode != 0:
            print(f"  Rebase warning: {rb.stderr.strip()}", file=sys.stderr)
        subprocess.run(["git", "push"], check=True)
        print(f"\n  Git pushed: {label}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ==============================================================================
# MAIN
# ==============================================================================

def parse_magnet_list(raw):
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


def main():
    global SUB_MAP
    all_mkv  = []
    all_subs = []

    # -- Step 0: download subtitle magnet first (if provided) ---------------
    if SUBTITLE_MAGNET:
        print("\n-- Downloading subtitle magnet --")
        subs = download_subtitle_magnet(SUBTITLE_MAGNET)
        all_subs.extend(subs)
        print(f"  {len(subs)} subtitle file(s) ready")

    # -- Source A: batch magnet links ---------------------------------------
    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s) | Movie mode: {MOVIE_MODE}")
        for i, magnet in enumerate(magnets, 1):
            print(f"\n[{i}/{len(magnets)}] Downloading...")
            files, subs = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not files:
                print("  No valid .mkv files - skipping", file=sys.stderr)
            else:
                all_mkv.extend(files)
                all_subs.extend(subs)

    # -- Source B: Nyaa search by episode -----------------------------------
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        if not EPISODE_OVERRIDE.strip():
            print(f"Auto mode - episode {episodes[0]} | Movie mode: {MOVIE_MODE}")
        else:
            print(f"Episode mode - {episodes} | Movie mode: {MOVIE_MODE}")

        not_found = []
        for ep in episodes:
            print(f"\n-- Searching episode {ep} --")
            magnet = search_nyaa(ep)
            if not magnet:
                not_found.append(ep)
                continue
            files, subs = download_magnet(magnet, parse_select_files(SELECT_FILES))
            if not files:
                print(f"  No valid .mkv files for EP {ep}", file=sys.stderr)
            else:
                all_mkv.extend(files)
                all_subs.extend(subs)

        if not_found:
            print(f"\n  Not found on Nyaa: {not_found}", file=sys.stderr)

    if not all_mkv:
        print("Nothing to process.")
        sys.exit(0)

    if all_subs:
        SUB_MAP = build_subtitle_map(all_subs)

    # -- Process ------------------------------------------------------------
    print(f"\nProcessing {len(all_mkv)} file(s)...")
    results = []
    for i, mkv in enumerate(all_mkv, 1):
        print(f"\n{'='*60}")
        print(f"[{i}/{len(all_mkv)}] {os.path.basename(mkv)}")
        print(f"{'='*60}")
        try:
            results.append(process_file(mkv))
        except Exception as e:
            print(f"  FATAL: {e}", file=sys.stderr)

    if results:
        changed = patch_html_batch(results)
        if changed:
            git_commit_push(results)

    # -- Summary ------------------------------------------------------------
    print("\n" + "="*60)
    print("RUN SUMMARY")
    print("="*60)
    for num, is_movie, hs_url, ss_url in results:
        kind = "Movie" if is_movie else "EP"
        print(f"  {kind} {num:>4}  SS:{'OK' if ss_url else 'FAIL'}  "
              f"HS:{'OK' if hs_url else 'FAIL'}")

    failed = [n for n, _m, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  Fully failed: {failed}")
        sys.exit(1)
    else:
        print(f"\n  All {len(results)} done.")


if __name__ == "__main__":
    main()
