"""
conan_automation_github.py — Detective Conan automated downloader + uploader

Features:
  • Auto-calculates the current episode from BASE_DATE / BASE_EPISODE
  • Searches Nyaa.si for SubsPlease (or a custom uploader) 1080p releases
  • Accepts MAGNET_LINKS env var for batch magnet processing (newline or comma separated)
  • Downloads via aria2c (1080p, English subtitles, no seeding)
  • Uploads the original .mkv as a Soft-Sub (SS) to DoodStream
  • Hard-subs with ffmpeg and uploads the .mp4 as a Hard-Sub (HS)
  • Folder routing: SS → SOFT_SUB_FOLDER_ID, HS → HARD_SUB_FOLDER_ID
  • Title format: "Detective Conan - {ep} SS" / "Detective Conan - {ep} HS"
    (customisable via HS_TITLE_TPL / SS_TITLE_TPL env vars)
  • Patches index.html after every upload and git commits + pushes
"""

import os
import re
import sys
import subprocess
import glob
import shutil
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from conan_utils import xor_encrypt
from update import patch_hs, patch_ss, read_html, write_html

# ── Config ────────────────────────────────────────────────────────────────────
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID = os.environ.get("SOFT_SUB_FOLDER_ID", "")

BASE_EPISODE = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE    = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE    = os.environ.get("EPISODE_OVERRIDE", "").strip()
MAGNET_LINKS        = os.environ.get("MAGNET_LINKS", "").strip()
CUSTOM_SEARCH       = os.environ.get("CUSTOM_SEARCH", "").strip()
NYAA_UPLOADER_URL   = os.environ.get("NYAA_UPLOADER_URL", "").strip()

# Title templates — use {ep} as placeholder
HS_TITLE_TPL = os.environ.get("HS_TITLE_TPL", "Detective Conan - {ep} HS")
SS_TITLE_TPL = os.environ.get("SS_TITLE_TPL", "Detective Conan - {ep} SS")

HTML_FILE   = os.environ.get("HTML_FILE", "index.html")
WORK_DIR    = os.path.abspath(".")

# ── Episode helpers ───────────────────────────────────────────────────────────

def get_expected_episode() -> int:
    if EPISODE_OVERRIDE and EPISODE_OVERRIDE.isdigit():
        return int(EPISODE_OVERRIDE)
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    now = datetime.now()
    weeks = max(0, (now - base_dt).days // 7)
    return BASE_EPISODE + weeks


def parse_episode_from_filename(filename: str) -> int | None:
    """Extract episode number from a filename like '[SubsPlease] Detective Conan - 1194 ...'"""
    m = re.search(r"Detective Conan\s*[-–]\s*(\d{3,4})\b", filename, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Fallback: any 3-4 digit number in the name
    m = re.search(r"\b(\d{3,4})\b", os.path.basename(filename))
    if m:
        return int(m.group(1))
    return None


# ── Nyaa search ───────────────────────────────────────────────────────────────

def search_nyaa(episode: int) -> str | None:
    """Search Nyaa.si and return the first matching magnet link."""
    if CUSTOM_SEARCH:
        query = CUSTOM_SEARCH
    else:
        query = f"Detective Conan - {episode} 1080p"

    base_url = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else "https://nyaa.si"
    # If it's a user URL like https://nyaa.si/user/SubsPlease use it directly
    if "/user/" in base_url:
        url = f"{base_url}?f=0&c=0_0&q={requests.utils.quote(query)}"
    else:
        url = f"https://nyaa.si/?f=0&c=1_2&q={requests.utils.quote(query)}"

    print(f"  Searching Nyaa: {url}")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Nyaa search failed: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    for row in soup.select("tr.success, tr.default"):
        # Only accept 1080p English releases
        title_cell = row.find("td", {"colspan": "2"}) or row.find("a", title=True)
        title_text = title_cell.get_text() if title_cell else ""
        if "1080p" not in title_text:
            continue
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    # Broaden search if nothing found
    print("  No 1080p match — retrying without filter…")
    for row in soup.select("tr.success, tr.default"):
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_magnet(magnet: str) -> list[str]:
    """Download a magnet with aria2c and return all .mkv files found."""
    print(f"  Downloading: {magnet[:80]}…")
    before = set(glob.glob("*.mkv"))

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--max-connection-per-server=4",
        "--split=4",
        "--file-allocation=none",
        "--bt-stop-timeout=300",   # stop if stalled 5 min
        magnet,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=3600)
    except subprocess.TimeoutExpired:
        print("  aria2c timeout — checking for partial files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c error: {e}", file=sys.stderr)

    after = set(glob.glob("*.mkv"))
    new_files = sorted(after - before, key=os.path.getmtime)
    print(f"  Downloaded: {new_files}")
    return new_files


# ── ffmpeg hard-sub ───────────────────────────────────────────────────────────

def _escape(path: str) -> str:
    """Escape a path for ffmpeg's subtitles= filter."""
    p = path.replace("\\", "\\\\")
    p = p.replace("'", "\\'")
    p = p.replace(":", "\\:")
    p = p.replace("[", "\\[").replace("]", "\\]")
    return p


def hardsub(input_file: str, ep: int) -> str | None:
    """Burn subtitles into video. Returns output path or None on failure."""
    output = f"conan_{ep}_hs.mp4"
    print(f"  Hard-subbing → {output}")

    for vf in [f"subtitles='{_escape(input_file)}'",
               f"subtitles={_escape(input_file)}"]:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=7200)
            print(f"  Hard-sub done: {output}")
            return output
        except subprocess.CalledProcessError as e:
            print(f"  ffmpeg attempt failed:\n{e.stderr[-1000:]}", file=sys.stderr)

    return None


# ── DoodStream upload ─────────────────────────────────────────────────────────

def get_upload_server() -> str | None:
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            return resp["result"]
    except Exception as e:
        print(f"  DoodStream server error: {e}", file=sys.stderr)
    return None


def upload_file(file_path: str, title: str, folder_id: str = "") -> str | None:
    """Upload a file to DoodStream. Returns the embed URL or None."""
    server = get_upload_server()
    if not server:
        print("  Could not get upload server", file=sys.stderr)
        return None

    print(f"  Uploading '{title}' ({os.path.getsize(file_path) // (1024*1024)} MB)…")
    try:
        with open(file_path, "rb") as fh:
            data = {"api_key": DOODSTREAM_API_KEY, "title": title}
            if folder_id:
                data["fld_id"] = folder_id
            resp = requests.post(
                server,
                files={"file": (os.path.basename(file_path), fh)},
                data=data,
                timeout=7200,
            ).json()

        if resp.get("status") == 200:
            result = resp["result"][0]
            url = result.get("download_url") or result.get("embed_url") or ""
            print(f"  Uploaded! URL: {url}")
            return url
        else:
            print(f"  Upload failed: {resp}", file=sys.stderr)
    except Exception as e:
        print(f"  Upload exception: {e}", file=sys.stderr)

    return None


# ── index.html patching + git ─────────────────────────────────────────────────

def patch_html(ep: int, hs_url: str | None, ss_url: str | None) -> None:
    html = read_html()
    if hs_url:
        html = patch_hs(html, ep, hs_url)
    if ss_url:
        html = patch_ss(html, ep, ss_url)
    write_html(html)


def git_commit_push(ep: int) -> None:
    try:
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"], check=True)
        subprocess.run(["git", "add", HTML_FILE], check=True)
        subprocess.run(
            ["git", "commit", "-m", f"chore: add episode {ep} SS+HS links"],
            check=True,
        )
        subprocess.run(["git", "push"], check=True)
        print(f"  Git pushed for episode {ep}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ── Per-episode processing ────────────────────────────────────────────────────

def process_episode(mkv_file: str) -> None:
    ep = parse_episode_from_filename(mkv_file)
    if ep is None:
        ep = get_expected_episode()
        print(f"  Could not parse episode from filename — using calculated: {ep}")
    else:
        print(f"  Episode detected: {ep}")

    hs_url = None
    ss_url = None

    # ── Soft Sub upload (original .mkv) ───────────────────────────────────
    ss_title = SS_TITLE_TPL.format(ep=ep)
    ss_url = upload_file(mkv_file, ss_title, SOFT_SUB_FOLDER_ID)

    # ── Hard Sub: burn subs → upload ──────────────────────────────────────
    hs_file = hardsub(mkv_file, ep)
    if hs_file:
        hs_title = HS_TITLE_TPL.format(ep=ep)
        hs_url = upload_file(hs_file, hs_title, HARD_SUB_FOLDER_ID)
        os.remove(hs_file)
    else:
        print(f"  WARNING: hard-sub failed for episode {ep}", file=sys.stderr)

    # ── Patch HTML ────────────────────────────────────────────────────────
    if hs_url or ss_url:
        patch_html(ep, hs_url, ss_url)
        git_commit_push(ep)
    else:
        print(f"  No URLs obtained for episode {ep} — HTML not patched", file=sys.stderr)

    # Cleanup source
    try:
        os.remove(mkv_file)
    except OSError:
        pass


# ── Main ──────────────────────────────────────────────────────────────────────

def parse_magnet_list(raw: str) -> list[str]:
    """Split newline- or comma-separated magnet links."""
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


def main() -> None:
    os.chdir(WORK_DIR)

    # ── Mode 1: batch magnet links provided directly ───────────────────────
    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s)")
        for magnet in magnets:
            new_files = download_magnet(magnet)
            if not new_files:
                print("  No .mkv files found after download — skipping", file=sys.stderr)
                continue
            for mkv in new_files:
                process_episode(mkv)
        return

    # ── Mode 2: auto-search or single episode ─────────────────────────────
    episode = get_expected_episode()
    print(f"Auto mode — targeting episode {episode}")

    magnet = search_nyaa(episode)
    if not magnet:
        print(f"Episode {episode} not found on Nyaa yet — exiting cleanly.")
        sys.exit(0)

    new_files = download_magnet(magnet)
    if not new_files:
        print("Download produced no .mkv files", file=sys.stderr)
        sys.exit(1)

    for mkv in new_files:
        process_episode(mkv)


if __name__ == "__main__":
    main()
