"""
update.py — Detective Conan index.html sync utility

Patches index.html with new DoodStream links:
  Episodes:
    HS  → ENCRYPTED_REMASTERED_HARD dict  (XOR-encrypted)
    SS  → EP_DB[ep].original.soft          (plain URL)
  Movies:
    HS  → MOVIE_DB[num].original.hard      (plain URL)
    SS  → MOVIE_DB[num].original.soft      (plain URL)

Bulk sync: fetches every file from DoodStream, parses titles like
  "Detective Conan - 1194 HS"
  "Detective Conan - 1194 SS"
  "Detective Conan Movie - 5 HS"
  "Detective Conan Movie - 5 SS"
and updates the HTML for all of them.

Usage:
  python update.py --ep 1194 --hs https://doodstream.com/e/xxx
  python update.py --ep 1194 --ss https://doodstream.com/e/yyy
  python update.py --movie 5 --hs https://doodstream.com/e/zzz
  python update.py --movie 5 --ss https://doodstream.com/e/www
  python update.py --bulk-sync
"""

import argparse
import os
import re
import sys
import requests
from conan_utils import xor_encrypt

DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HTML_FILE          = os.environ.get("HTML_FILE", "index.html")
XOR_KEY            = "DetectiveConan2024"


# ── File I/O ──────────────────────────────────────────────────────────────────

def read_html() -> str:
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()

def write_html(content: str) -> None:
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Saved {HTML_FILE}")


# ── Episode patching ──────────────────────────────────────────────────────────

def patch_hs(html: str, ep: int, url: str) -> str:
    """Insert/replace a hard-sub entry in ENCRYPTED_REMASTERED_HARD."""
    encrypted  = xor_encrypt(url, XOR_KEY)
    new_entry  = f"      {ep}: \"{encrypted}\","

    existing = re.compile(rf"^\s+{ep}: \".*?\",\s*$", re.MULTILINE)
    if existing.search(html):
        html = existing.sub(new_entry, html)
        print(f"  [EP HS] Updated episode {ep}")
    else:
        # Insert before closing '    };' of the ENCRYPTED_REMASTERED_HARD block
        closing = re.compile(r"(      \d+: \"[^\"]+\",\n)(    \};)", re.MULTILINE)
        m = closing.search(html)
        if m:
            html = html[: m.start(2)] + new_entry + "\n" + html[m.start(2):]
            print(f"  [EP HS] Inserted episode {ep}")
        else:
            print(f"  [EP HS] ERROR: insertion point not found for episode {ep}", file=sys.stderr)
    return html


def patch_ss(html: str, ep: int, url: str) -> str:
    """Insert/replace soft-sub URL in EP_DB[ep].original."""
    pattern = re.compile(
        rf'(EP_DB\[{ep}\] = \{{\"original\": \{{)(.*?)(\}}, \"remastered\":)',
        re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        print(f"  [EP SS] ERROR: EP_DB[{ep}] not found", file=sys.stderr)
        return html

    block = m.group(2)
    block = re.sub(r',?\s*"soft":\s*"[^"]*"', "", block)
    block = block.rstrip(", ") + f', "soft": "{url}"'

    html = html[: m.start(2)] + block + html[m.end(2):]
    print(f"  [EP SS] Updated episode {ep}")
    return html


# ── Movie patching ────────────────────────────────────────────────────────────
# Movies use MOVIE_DB[num].original.hard / .soft lines after the forEach block.
# Format:  MOVIE_DB[5].original.hard = "url"; // optional comment

_MOVIE_HS_RE = re.compile(
    r'^([ \t]*MOVIE_DB\[{num}\]\.original\.hard\s*=\s*)"[^"]*"(;.*)?$',
    re.MULTILINE,
)
_MOVIE_SS_RE = re.compile(
    r'^([ \t]*MOVIE_DB\[{num}\]\.original\.soft\s*=\s*)"[^"]*"(;.*)?$',
    re.MULTILINE,
)

def _movie_pattern(num: int, field: str) -> re.Pattern:
    return re.compile(
        rf'^([ \t]*MOVIE_DB\[{num}\]\.original\.{field}\s*=\s*)"[^"]*"(;.*)?\s*$',
        re.MULTILINE,
    )

def _movie_anchor(html: str) -> int:
    """Return the char index right after the MOVIE_DB forEach block closing '});'"""
    m = re.search(r'\}\);\s*\n', html)
    if m:
        return m.end()
    # fallback: after last MOVIE_DB line
    m = re.search(r'(MOVIE_DB\[.*\n)', html)
    if m:
        return m.end()
    return len(html)


def patch_movie_hs(html: str, num: int, url: str) -> str:
    pat = _movie_pattern(num, "hard")
    new_line = f'    MOVIE_DB[{num}].original.hard = "{url}"; // Movie {num} HS'
    if pat.search(html):
        html = pat.sub(new_line, html)
        print(f"  [MV HS] Updated movie {num}")
    else:
        anchor = _movie_anchor(html)
        html = html[:anchor] + new_line + "\n" + html[anchor:]
        print(f"  [MV HS] Inserted movie {num}")
    return html


def patch_movie_ss(html: str, num: int, url: str) -> str:
    pat = _movie_pattern(num, "soft")
    new_line = f'    MOVIE_DB[{num}].original.soft = "{url}"; // Movie {num} SS'
    if pat.search(html):
        html = pat.sub(new_line, html)
        print(f"  [MV SS] Updated movie {num}")
    else:
        anchor = _movie_anchor(html)
        html = html[:anchor] + new_line + "\n" + html[anchor:]
        print(f"  [MV SS] Inserted movie {num}")
    return html


# ── Single-entry patch ────────────────────────────────────────────────────────

def apply_patch(ep: int | None = None, movie: int | None = None,
                hs_url: str | None = None, ss_url: str | None = None) -> None:
    if not hs_url and not ss_url:
        print("Nothing to patch.")
        return

    html = read_html()

    if ep is not None:
        if hs_url:
            html = patch_hs(html, ep, hs_url)
        if ss_url:
            html = patch_ss(html, ep, ss_url)
    elif movie is not None:
        if hs_url:
            html = patch_movie_hs(html, movie, hs_url)
        if ss_url:
            html = patch_movie_ss(html, movie, ss_url)

    write_html(html)


# ── Bulk sync ─────────────────────────────────────────────────────────────────

# Matches: "Detective Conan - 1194 HS"  or  "Detective Conan Movie - 5 SS"
TITLE_RE = re.compile(
    r"Detective Conan\s*(Movie)?\s*[-\u2013]\s*(\d+)\s+(HS|SS|DUB)",
    re.IGNORECASE,
)


def fetch_all_dood_files() -> list:
    files = []
    page  = 1
    while True:
        try:
            resp = requests.get(
                "https://doodapi.co/api/file/list",
                params={"key": DOODSTREAM_API_KEY, "page": page, "per_page": 200},
                timeout=30,
            ).json()
        except Exception as e:
            print(f"  DoodStream API error (page {page}): {e}", file=sys.stderr)
            break

        if resp.get("status") != 200:
            break

        results = resp.get("result", {}).get("results", [])
        if not results:
            break

        files.extend(results)
        if page >= resp.get("result", {}).get("pages", 1):
            break
        page += 1

    return files


def bulk_sync() -> None:
    print("Fetching all DoodStream files...")
    files = fetch_all_dood_files()
    print(f"  Found {len(files)} total files")

    html    = read_html()
    patched = 0

    for f in files:
        title = f.get("title", "")
        m     = TITLE_RE.search(title)
        if not m:
            continue

        is_movie = bool(m.group(1))
        num      = int(m.group(2))
        kind     = m.group(3).upper()
        url      = f.get("download_url") or f.get("embed_url") or ""
        if not url:
            continue

        if is_movie:
            if kind == "HS":
                html = patch_movie_hs(html, num, url)
            elif kind in ("SS", "DUB"):
                html = patch_movie_ss(html, num, url)
        else:
            if kind == "HS":
                html = patch_hs(html, num, url)
            elif kind in ("SS", "DUB"):
                html = patch_ss(html, num, url)

        patched += 1

    if patched:
        write_html(html)
        print(f"  Bulk sync complete — {patched} entries updated")
    else:
        print("  No matching files found")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Patch index.html with DoodStream links")
    parser.add_argument("--ep",        type=int, help="Episode number")
    parser.add_argument("--movie",     type=int, help="Movie number")
    parser.add_argument("--hs",        metavar="URL", help="Hard-sub URL")
    parser.add_argument("--ss",        metavar="URL", help="Soft-sub URL")
    parser.add_argument("--bulk-sync", action="store_true", help="Sync all files from DoodStream API")
    args = parser.parse_args()

    if args.bulk_sync:
        bulk_sync()
    elif args.ep or args.movie:
        apply_patch(ep=args.ep, movie=args.movie, hs_url=args.hs, ss_url=args.ss)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
