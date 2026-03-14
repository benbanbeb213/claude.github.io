"""
update.py — Detective Conan index.html sync utility

Patches index.html with new DoodStream links:
  • HS  → ENCRYPTED_REMASTERED_HARD dict  (XOR-encrypted)
  • SS  → EP_DB[ep].original.soft          (plain URL)

Also supports bulk sync: fetches every file from your DoodStream
account, parses titles like "Detective Conan - 1194 HS" or
"Detective Conan - 1194 SS", and updates the HTML for all of them.

Usage:
  python update.py --ep 1194 --hs https://doodstream.com/e/xxx
  python update.py --ep 1194 --ss https://doodstream.com/e/yyy
  python update.py --bulk-sync
"""

import argparse
import os
import re
import sys
import requests
from conan_utils import xor_encrypt

# ── Config ────────────────────────────────────────────────────────────────────
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HTML_FILE = os.environ.get("HTML_FILE", "index.html")
XOR_KEY = "DetectiveConan2024"


# ── Patching helpers ──────────────────────────────────────────────────────────

def patch_hs(html: str, ep: int, url: str) -> str:
    """Insert/replace a hard-sub entry in ENCRYPTED_REMASTERED_HARD."""
    encrypted = xor_encrypt(url, XOR_KEY)
    new_entry = f"      {ep}: \"{encrypted}\","

    # Replace existing entry
    existing = re.compile(rf"^\s+{ep}: \".*?\",\s*$", re.MULTILINE)
    if existing.search(html):
        html = existing.sub(new_entry, html)
        print(f"  [HS] Updated episode {ep} in ENCRYPTED_REMASTERED_HARD")
    else:
        # Insert before the closing }; of the block
        # The block ends with '    };' right after the last numeric entry
        closing = re.compile(r"(      \d+: \"[^\"]+\",\n)(    \};)", re.MULTILINE)
        m = closing.search(html)
        if m:
            insert_after = m.start(2)
            html = html[:insert_after] + new_entry + "\n" + html[insert_after:]
            print(f"  [HS] Inserted episode {ep} into ENCRYPTED_REMASTERED_HARD")
        else:
            print(f"  [HS] ERROR: could not find insertion point for episode {ep}", file=sys.stderr)
    return html


def patch_ss(html: str, ep: int, url: str) -> str:
    """Insert/replace soft-sub URL in EP_DB[ep].original.soft."""
    # Match the full EP_DB line for this episode
    pattern = re.compile(
        rf'(EP_DB\[{ep}\] = \{{\"original\": \{{)(.*?)(\}}, \"remastered\":)',
        re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        print(f"  [SS] ERROR: EP_DB[{ep}] not found", file=sys.stderr)
        return html

    original_block = m.group(2)  # e.g. '"dub": "...", "soft": "..."'

    # Remove existing soft entry if present
    original_block = re.sub(r',?\s*"soft":\s*"[^"]*"', "", original_block)
    # Append new soft entry
    original_block = original_block.rstrip(", ") + f', "soft": "{url}"'

    html = html[: m.start(2)] + original_block + html[m.end(2) :]
    print(f"  [SS] Updated episode {ep} soft-sub in EP_DB")
    return html


def read_html() -> str:
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()


def write_html(content: str) -> None:
    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Saved {HTML_FILE}")


# ── Single-episode patch ──────────────────────────────────────────────────────

def apply_patch(ep: int, hs_url: str | None = None, ss_url: str | None = None) -> None:
    if not hs_url and not ss_url:
        print("Nothing to patch.")
        return

    html = read_html()

    if hs_url:
        html = patch_hs(html, ep, hs_url)
    if ss_url:
        html = patch_ss(html, ep, ss_url)

    write_html(html)


# ── Bulk sync from DoodStream ─────────────────────────────────────────────────

TITLE_RE = re.compile(
    r"Detective Conan\s*[-–]\s*(\d+)\s+(HS|SS|DUB)", re.IGNORECASE
)


def fetch_all_dood_files() -> list[dict]:
    """Fetch every file from the DoodStream account."""
    files = []
    page = 1
    while True:
        try:
            resp = requests.get(
                "https://doodapi.co/api/file/list",
                params={"key": DOODSTREAM_API_KEY, "page": page, "per_page": 200},
                timeout=30,
            ).json()
        except Exception as e:
            print(f"  DoodStream API error on page {page}: {e}", file=sys.stderr)
            break

        if resp.get("status") != 200:
            print(f"  DoodStream returned status {resp.get('status')}", file=sys.stderr)
            break

        results = resp.get("result", {}).get("results", [])
        if not results:
            break

        files.extend(results)

        total_pages = resp.get("result", {}).get("pages", 1)
        if page >= total_pages:
            break
        page += 1

    return files


def bulk_sync() -> None:
    """Fetch all DoodStream files and patch index.html."""
    print("Fetching all DoodStream files…")
    files = fetch_all_dood_files()
    print(f"  Found {len(files)} total files")

    html = read_html()
    patched = 0

    for f in files:
        title = f.get("title", "")
        m = TITLE_RE.search(title)
        if not m:
            continue

        ep = int(m.group(1))
        kind = m.group(2).upper()
        url = f.get("download_url") or f.get("embed_url") or ""

        if not url:
            continue

        if kind == "HS":
            html = patch_hs(html, ep, url)
            patched += 1
        elif kind in ("SS", "DUB"):
            html = patch_ss(html, ep, url)
            patched += 1

    if patched:
        write_html(html)
        print(f"  Bulk sync complete — {patched} entries updated")
    else:
        print("  No matching files found to sync")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Patch index.html with DoodStream links")
    parser.add_argument("--ep", type=int, help="Episode number")
    parser.add_argument("--hs", metavar="URL", help="Hard-sub DoodStream URL")
    parser.add_argument("--ss", metavar="URL", help="Soft-sub DoodStream URL")
    parser.add_argument("--bulk-sync", action="store_true", help="Sync all files from DoodStream")
    args = parser.parse_args()

    if args.bulk_sync:
        bulk_sync()
    elif args.ep:
        apply_patch(args.ep, hs_url=args.hs, ss_url=args.ss)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
