"""
conan_automation_github.py — Detective Conan automated downloader + uploader

Fixes in this version:
  • SS upload: remuxes .mkv → .mp4 (stream copy, ~30 sec) so DoodStream accepts it
  • Titles set via DoodStream rename API after upload (upload endpoint ignores title param)
  • Episode/movie number always taken from the original filename
  • Movie mode: set MOVIE_MODE=1 to treat all files as movies
  • Single git commit+push at end of run — never mid-loop
  • Per-episode error isolation — 1 failure never kills the batch
  • Upload retries x3 with server URL refresh each attempt
"""

import os
import re
import sys
import glob
import time
import subprocess
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from conan_utils import xor_encrypt
from update import patch_hs, patch_ss, patch_movie_hs, patch_movie_ss, read_html, write_html

# ── Config ────────────────────────────────────────────────────────────────────
DOODSTREAM_API_KEY  = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID  = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID  = os.environ.get("SOFT_SUB_FOLDER_ID", "")

BASE_EPISODE        = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE           = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE    = os.environ.get("EPISODE_OVERRIDE",   "").strip()
MAGNET_LINKS        = os.environ.get("MAGNET_LINKS",        "").strip()
CUSTOM_SEARCH       = os.environ.get("CUSTOM_SEARCH",       "").strip()
NYAA_UPLOADER_URL   = os.environ.get("NYAA_UPLOADER_URL",   "").strip()

# Set MOVIE_MODE=1 to treat all downloaded files as movies instead of episodes
MOVIE_MODE          = os.environ.get("MOVIE_MODE", "0").strip() == "1"

# Title templates — {ep} = episode number, {num} = movie number
HS_TITLE_TPL        = os.environ.get("HS_TITLE_TPL", "Detective Conan - {ep} HS")
SS_TITLE_TPL        = os.environ.get("SS_TITLE_TPL", "Detective Conan - {ep} SS")
MOVIE_HS_TITLE_TPL  = os.environ.get("MOVIE_HS_TITLE_TPL", "Detective Conan Movie - {num} HS")
MOVIE_SS_TITLE_TPL  = os.environ.get("MOVIE_SS_TITLE_TPL", "Detective Conan Movie - {num} SS")

HTML_FILE           = os.environ.get("HTML_FILE", "index.html")

UPLOAD_RETRIES      = 3
RETRY_DELAY         = 10

_upload_server_url: str | None = None


# ── Episode / movie number from filename ──────────────────────────────────────

def parse_file_info(filename: str) -> tuple:
    """
    Auto-detect whether a file is a movie or episode, and extract the number.
    Returns (number, is_movie).

    Movie detection — any of these in the filename triggers movie mode:
      "Movie" keyword:   "Detective Conan Movie 28 - ..."
      OVA keyword:       "Detective Conan OVA 18 - ..."
      "Film" keyword:    "Detective Conan Film 5 ..."
      MOVIE_MODE env:    override forces movie=True for all files

    Episode detection:
      "Detective Conan - 1194 ..."  →  episode 1194
    """
    base = os.path.basename(filename)

    # MOVIE_MODE env var overrides per-file detection
    if MOVIE_MODE:
        # Try to pull a 1-3 digit movie number
        m = re.search(r"\bMovie\s*[-\u2013]?\s*(\d{1,3})\b", base, re.IGNORECASE)
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        num = int(m.group(1)) if m else None
        return num, True

    # ── Auto-detect movie from filename ───────────────────────────────────
    # Check for Movie / OVA / Film keywords BEFORE the episode dash pattern
    movie_kw = re.search(r"\b(Movie|Film|OVA)\b", base, re.IGNORECASE)
    if movie_kw:
        # Extract the number that follows the keyword, or any 1-3 digit number
        m = re.search(
            r"\b(?:Movie|Film|OVA)\s*[-\u2013]?\s*(\d{1,3})\b",
            base, re.IGNORECASE
        )
        if not m:
            m = re.search(r"\b(\d{1,3})\b", base)
        num = int(m.group(1)) if m else None
        return num, True

    # ── Standard episode: "Detective Conan - 1194" ────────────────────────
    m = re.search(r"Detective Conan\s*[-\u2013]\s*(\d{3,4})\b", base, re.IGNORECASE)
    if m:
        return int(m.group(1)), False

    # Fallback: any 3-4 digit number → treat as episode
    m = re.search(r"\b(\d{3,4})\b", base)
    if m:
        return int(m.group(1)), False

    return None, False


def get_auto_episode() -> int:
    """Return the single auto-calculated episode based on BASE_DATE."""
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    weeks   = max(0, (datetime.now() - base_dt).days // 7)
    return BASE_EPISODE + weeks


def parse_episode_override(raw: str) -> list:
    """
    Parse EPISODE_OVERRIDE into a list of episode numbers.

    Formats supported:
      "1000"        -> [1000]               single episode
      "1000-1005"   -> [1000,1001,...,1005]  inclusive range
      "1000,1005"   -> [1000, 1005]          specific list (no in-between)
      ""            -> [auto-calculated]     blank = this week's episode

    You can also mix comma entries with ranges:
      "1000,1003-1005,1010"  -> [1000, 1003, 1004, 1005, 1010]
    """
    raw = raw.strip()
    if not raw:
        return [get_auto_episode()]

    episodes = []
    # Split on commas first, then handle each part
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            # Range like "1000-1005"
            halves = part.split("-", 1)
            try:
                start = int(halves[0].strip())
                end   = int(halves[1].strip())
                if start > end:
                    start, end = end, start   # tolerate reversed input
                episodes.extend(range(start, end + 1))
            except ValueError:
                print(f"  WARNING: could not parse range '{part}' — skipping", file=sys.stderr)
        else:
            try:
                episodes.append(int(part))
            except ValueError:
                print(f"  WARNING: could not parse episode '{part}' — skipping", file=sys.stderr)

    if not episodes:
        print("  WARNING: episode override produced no valid numbers — using auto", file=sys.stderr)
        return [get_auto_episode()]

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for ep in episodes:
        if ep not in seen:
            seen.add(ep)
            unique.append(ep)
    return unique


# ── Nyaa search ───────────────────────────────────────────────────────────────

def search_nyaa(episode: int) -> str | None:
    query = CUSTOM_SEARCH if CUSTOM_SEARCH else f"Detective Conan - {episode} 1080p"
    base  = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else "https://nyaa.si"
    url   = f"{base}?f=0&c=1_2&q={requests.utils.quote(query)}"
    print(f"  Searching Nyaa: {url}")

    try:
        soup = BeautifulSoup(requests.get(url, timeout=30).text, "html.parser")
    except Exception as e:
        print(f"  Nyaa error: {e}", file=sys.stderr)
        return None

    for row in soup.select("tr.success, tr.default"):
        title_cell = row.find("td", {"colspan": "2"}) or row.find("a", title=True)
        if title_cell and "1080p" not in title_cell.get_text():
            continue
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    for row in soup.select("tr.success, tr.default"):
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_magnet(magnet: str) -> list:
    before = set(glob.glob("**/*.mkv", recursive=True))
    print(f"  Downloading: {magnet[:90]}...")

    try:
        subprocess.run([
            "aria2c", "--seed-time=0",
            "--max-connection-per-server=4", "--split=4",
            "--file-allocation=none", "--bt-stop-timeout=300",
            magnet,
        ], check=True, timeout=7200)
    except subprocess.TimeoutExpired:
        print("  aria2c timeout — checking partial files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c error: {e}", file=sys.stderr)

    after = set(glob.glob("**/*.mkv", recursive=True))
    new   = sorted(after - before, key=os.path.getmtime)
    print(f"  New .mkv files: {new or 'none'}")
    return new


# ── ffmpeg helpers ────────────────────────────────────────────────────────────

def _esc(path: str) -> str:
    p = path.replace("\\", "\\\\").replace("'", "\\'")
    return p.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")


def _remux_ok(path: str) -> bool:
    """Return True if path exists and is at least 10 MB (not corrupt/empty)."""
    return os.path.exists(path) and os.path.getsize(path) > 10 * 1024 * 1024


def remux_to_mp4(input_file: str, label: str) -> str | None:
    """
    Remux .mkv -> .mp4 for SS upload. Three attempts, most-likely first.

    Why not -c:s mov_text?
      SubsPlease uses ASS subtitles. ASS cannot be stream-copied into MP4 --
      ffmpeg always errors on this. We drop the subtitle stream every time.

    Why -movflags +faststart?
      Without it the MP4 moov atom ends up at the END of the file.
      DoodStream requires moov at the START to process/stream the upload.
      Without faststart the upload returns 200 but the video never processes.
      This was the primary silent-failure cause for SS uploads.

    Attempt 1 -- video copy + audio copy        (fastest, H.264 + AAC)
    Attempt 2 -- video copy + audio re-encode   (handles Opus audio which
                 cannot be stream-copied into MP4 -- newer SubsPlease releases)
    Attempt 3 -- full re-encode H.264 + AAC     (nuclear fallback, always works)
    """
    output = f"conan_{label}_ss.mp4"

    if os.path.exists(output):
        os.remove(output)

    print(f"  Remuxing MKV -> MP4 for SS -> {output}")

    attempts = [
        ("video+audio stream copy",         ["-c:v", "copy", "-c:a", "copy"]),
        ("video copy + audio re-encode AAC",["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"]),
        ("full re-encode H.264 + AAC",      ["-c:v", "libx264", "-preset", "veryfast",
                                             "-crf", "22", "-c:a", "aac", "-b:a", "192k"]),
    ]

    for desc, codec_flags in attempts:
        if os.path.exists(output):
            os.remove(output)

        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            *codec_flags,
            "-sn",                      # always drop subs -- ASS cannot go into MP4
            "-movflags", "+faststart",  # moov atom at front -- required by DoodStream
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


def hardsub(input_file: str, label: str) -> str | None:
    """Burn subtitles into video. Unique output name per episode."""
    output = f"conan_{label}_hs.mp4"
    print(f"  Hard-subbing -> {output}")

    for vf in [f"subtitles='{_esc(input_file)}'", f"subtitles={_esc(input_file)}"]:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=7200)
            print(f"  Hard-sub complete: {output}")
            return output
        except subprocess.CalledProcessError as e:
            print(f"  ffmpeg attempt failed:\n{e.stderr[-600:]}", file=sys.stderr)

    print(f"  Hard-sub FAILED for {label}", file=sys.stderr)
    return None


# ── DoodStream upload ─────────────────────────────────────────────────────────

def get_upload_server() -> str | None:
    global _upload_server_url
    if _upload_server_url:
        return _upload_server_url
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            _upload_server_url = resp["result"]
            return _upload_server_url
    except Exception as e:
        print(f"  Upload server error: {e}", file=sys.stderr)
    return None


def rename_dood_file(file_code: str, title: str) -> None:
    """Set the DoodStream file title via the rename API."""
    try:
        resp = requests.get(
            "https://doodapi.co/api/file/rename",
            params={"key": DOODSTREAM_API_KEY, "file_code": file_code, "title": title},
            timeout=15,
        ).json()
        if resp.get("status") == 200:
            print(f"  Title set: '{title}'")
        else:
            print(f"  Rename API returned: {resp}", file=sys.stderr)
    except Exception as e:
        print(f"  Rename API error: {e}", file=sys.stderr)


def upload_file(file_path: str, title: str, folder_id: str = "") -> str | None:
    """
    Upload file to DoodStream, then rename it.
    Returns the embed/download URL or None.
    Retries up to UPLOAD_RETRIES times.
    """
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        global _upload_server_url
        _upload_server_url = None          # always refresh server URL
        server = get_upload_server()
        if not server:
            print(f"  [attempt {attempt}] No upload server", file=sys.stderr)
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
                result     = resp["result"][0]
                file_code  = result.get("file_code") or result.get("filecode") or ""
                url        = result.get("download_url") or result.get("embed_url") or ""

                # Set title via rename API (upload endpoint ignores title param)
                if file_code:
                    rename_dood_file(file_code, title)

                print(f"  Uploaded! {url}")
                return url
            else:
                print(f"  [attempt {attempt}] Bad response: {resp}", file=sys.stderr)

        except Exception as e:
            print(f"  [attempt {attempt}] Exception: {e}", file=sys.stderr)

        if attempt < UPLOAD_RETRIES:
            print(f"  Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    print(f"  All {UPLOAD_RETRIES} attempts failed for '{title}'", file=sys.stderr)
    return None


# ── Per-file processing ───────────────────────────────────────────────────────

def process_file(mkv_file: str):
    """
    Process one .mkv — detect number from original filename, upload SS+HS.
    Returns (number, is_movie, hs_url, ss_url). Never raises.
    """
    num, is_movie = parse_file_info(mkv_file)

    if num is None:
        num      = get_auto_episode()
        is_movie = MOVIE_MODE
        print(f"  Could not parse from filename — using calculated: EP {num}")
    else:
        kind = "Movie" if is_movie else "Episode"
        print(f"  Auto-detected: {kind} {num}  ({os.path.basename(mkv_file)})")

    label    = f"m{num}" if is_movie else str(num)
    hs_url   = None
    ss_url   = None
    ss_file  = None
    hs_file  = None

    # ── SS: remux mkv→mp4 (stream copy) then upload ───────────────────────
    try:
        ss_file = remux_to_mp4(mkv_file, label)
        if ss_file:
            if is_movie:
                title = MOVIE_SS_TITLE_TPL.format(num=num)
            else:
                title = SS_TITLE_TPL.format(ep=num)
            ss_url = upload_file(ss_file, title, SOFT_SUB_FOLDER_ID)
        else:
            print(f"  Remux failed — skipping SS upload", file=sys.stderr)
    except Exception as e:
        print(f"  SS exception: {e}", file=sys.stderr)
    finally:
        if ss_file and os.path.exists(ss_file):
            try:
                os.remove(ss_file)
            except OSError:
                pass

    # ── HS: burn subs then upload ─────────────────────────────────────────
    try:
        hs_file = hardsub(mkv_file, label)
        if hs_file:
            if is_movie:
                title = MOVIE_HS_TITLE_TPL.format(num=num)
            else:
                title = HS_TITLE_TPL.format(ep=num)
            hs_url = upload_file(hs_file, title, HARD_SUB_FOLDER_ID)
    except Exception as e:
        print(f"  HS exception: {e}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try:
                os.remove(hs_file)
            except OSError:
                pass

    # Cleanup source
    try:
        os.remove(mkv_file)
    except OSError:
        pass

    return num, is_movie, hs_url, ss_url


# ── Batch HTML patch + single git push ───────────────────────────────────────

def patch_html_batch(results: list) -> bool:
    if not any(hs or ss for _, _m, hs, ss in results):
        print("\nNo URLs obtained — index.html unchanged.")
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


def git_commit_push(results: list) -> None:
    ep_parts  = [str(n) for n, m, hs, ss in results if not m and (hs or ss)]
    mov_parts = [f"M{n}" for n, m, hs, ss in results if m and (hs or ss)]
    label     = ", ".join(sorted(ep_parts) + mov_parts) or "unknown"

    try:
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"], check=True)
        subprocess.run(["git", "add", HTML_FILE], check=True)
        subprocess.run(
            ["git", "commit", "-m", f"chore: add links for {label}"],
            check=True,
        )
        subprocess.run(["git", "pull", "--rebase"], check=False)
        subprocess.run(["git", "push"], check=True)
        print(f"\n  Git pushed: {label}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ── Magnet list parser ────────────────────────────────────────────────────────

def parse_magnet_list(raw: str) -> list:
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    all_mkv = []

    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s) | Movie mode: {MOVIE_MODE}")
        for i, magnet in enumerate(magnets, 1):
            print(f"\n[{i}/{len(magnets)}] Downloading...")
            new_files = download_magnet(magnet)
            if not new_files:
                print("  No .mkv files found — skipping", file=sys.stderr)
            else:
                all_mkv.extend(new_files)
    else:
        episodes = parse_episode_override(EPISODE_OVERRIDE)
        if len(episodes) == 1 and not EPISODE_OVERRIDE.strip():
            print(f"Auto mode — episode {episodes[0]} (calculated) | Movie mode: {MOVIE_MODE}")
        else:
            print(f"Episode mode — {len(episodes)} episode(s): {episodes} | Movie mode: {MOVIE_MODE}")

        not_found = []
        for ep in episodes:
            print(f"\n  Searching for episode {ep}...")
            magnet = search_nyaa(ep)
            if not magnet:
                print(f"  Episode {ep} not found on Nyaa — skipping", file=sys.stderr)
                not_found.append(ep)
                continue
            new_files = download_magnet(magnet)
            if not new_files:
                print(f"  No .mkv files downloaded for episode {ep}", file=sys.stderr)
            else:
                all_mkv.extend(new_files)

        if not_found:
            print(f"\n  Episodes not found on Nyaa: {not_found}", file=sys.stderr)
        if not all_mkv:
            print("No files downloaded at all.", file=sys.stderr)
            sys.exit(0)

    if not all_mkv:
        print("Nothing to process.")
        sys.exit(0)

    print(f"\nProcessing {len(all_mkv)} file(s)...")

    results = []
    for i, mkv in enumerate(all_mkv, 1):
        print(f"\n[{i}/{len(all_mkv)}] {os.path.basename(mkv)}")
        try:
            results.append(process_file(mkv))
        except Exception as e:
            print(f"  FATAL ERROR: {e}", file=sys.stderr)

    if results:
        changed = patch_html_batch(results)
        if changed:
            git_commit_push(results)

    # ── Summary ───────────────────────────────────────────────────────────
    print("\n── Run summary ──")
    for num, is_movie, hs_url, ss_url in results:
        kind = "Movie" if is_movie else "EP"
        hs   = "OK" if hs_url else "FAIL"
        ss   = "OK" if ss_url else "FAIL"
        print(f"  {kind} {num:>4}  SS:{ss}  HS:{hs}")

    failed = [n for n, _m, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  {len(failed)} fully failed: {failed}")
        sys.exit(1)
    else:
        print(f"\n  All {len(results)} done.")


if __name__ == "__main__":
    main()
