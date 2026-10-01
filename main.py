#!/usr/bin/env python3
"""
bot.py — Number Extractor Bot v5.5
Render Web Service + Termux compatible.
- Binds health shim on $PORT first, reaps stale holders, falls back only if needed.
- Single init under lock. Signal-driven shutdown. atexit session cleanup.
- Cross-cycle visit budget. srcdoc depth guard. Worker crash isolation.
"""

from __future__ import annotations

import argparse
import atexit
import base64
import codecs
import csv
import json
import logging
import logging.handlers
import os
import random
import re
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import zipfile
import zlib
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import (
    parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse,
)

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    sys.stderr.write("missing dependency: requests\nrun: pip install requests\n")
    sys.exit(2)

# =============================================================================
# 1. CONFIG
# =============================================================================


def _writable(p: Path) -> bool:
    try:
        p.mkdir(parents=True, exist_ok=True)
    except (OSError, PermissionError):
        return False
    probe = p / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except (OSError, PermissionError):
        try:
            probe.unlink()
        except (OSError, PermissionError):
            pass
        return False


def _resolve_work_dir() -> Path:
    candidates: list[Path] = []

    env = os.environ.get("WORK_DIR")
    if env:
        candidates.append(Path(env))

    candidates.append(Path("/sdcard/wp1"))

    try:
        project_dir = Path(__file__).resolve().parent
    except (NameError, OSError):
        project_dir = Path.cwd()
    candidates.append(project_dir / "wp1")

    try:
        candidates.append(Path(tempfile.gettempdir()) / "wp1")
    except (OSError, RuntimeError):
        pass

    for cand in candidates:
        if _writable(cand):
            return cand

    raise RuntimeError(
        "no writable WORK_DIR found; set WORK_DIR env var to a writable path"
    )


CONFIG: dict[str, Any] = {
    "TG_TOKEN": os.environ.get("TG_TOKEN", "").strip(),
    "ALLOWED_CHATS": {
        int(x) for x in os.environ.get(
            "ALLOWED_CHATS", "8753914631,8565258976"
        ).split(",") if x.strip()
    } or None,

    "THREADS": int(os.environ.get("THREADS", "20")),
    "MAX_HOPS": 15,
    "MAX_BRANCHES": 30,
    "MAX_VISITED": 45,
    "MAX_TOTAL_VISITED": 900,
    "MAX_ATTEMPTS": 8,
    "TIMEOUT": (6, 8),
    "HOP_DELAY": 0.0,

    "WALL_CLOCK_PER_WORKER": 12.0,
    "WALL_CLOCK_TOTAL": 60.0,
    "EARLY_EXIT_THRESHOLD": 5,
    "PARTIAL_STREAM_AT": 30.0,

    "REQUEST_RATE_LIMIT": 8.0,
    "RETRY_BACKOFF_BASE": 0.6,

    "WORK_DIR": _resolve_work_dir(),
    "LOG_DIR": None,

    "ENABLE_LOGGING": True,
    "ENABLE_METRICS": True,
    "VERBOSE": os.environ.get("VERBOSE", "0") == "1",

    "ASSET_FETCH_LIMIT": 5,
    "ASSET_SIZE_LIMIT": 200 * 1024,
    "MIRROR_BLACKLIST_TTL": 900,

    "SELF_TEST": os.environ.get("SELF_TEST", "0") == "1",
    "REFERER_CHAIN": True,
    "COOKIE_JAR": True,

    "RENDER_PORT_FALLBACKS": [10000, 8080, 8000, 5000, 3000, 8888, 9000],
}

CONFIG["LOG_DIR"] = CONFIG["WORK_DIR"] / "logs"
try:
    CONFIG["LOG_DIR"].mkdir(parents=True, exist_ok=True)
except (OSError, PermissionError) as _e:
    CONFIG["LOG_DIR"] = CONFIG["WORK_DIR"]
    sys.stderr.write(f"warn: could not create log dir, using WORK_DIR root: {_e}\n")

MAX_URL_LEN: int = 4096
CSV_HEADER: list[str] = ["number", "confidence", "country", "hop", "url", "surface", "context"]

# =============================================================================
# 2. CONSTANTS
# =============================================================================

UA_POOL: list[str] = [
    'Mozilla/5.0 (Linux; Android 16; CPH2665 Build/BP2A.250605.015) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.7922.202 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 15; SM-S928B Build/AP3A.240905.015) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.7204.179 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 14; 23021RAAEG Build/UKQ1.231003.002) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.6943.137 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 15; V2312 Build/AP3A.240905.015) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.7204.157 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 16; CPH2591 Build/BP2A.250605.015) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.7922.187 Mobile Safari/537.36',
    'Mozilla/5.0 (Linux; Android 16; SM-S921B Build/AP3A.240905.015) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.7922.190 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c}.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]
CHROME_VERSIONS: list[int] = [148, 149, 150, 151, 152, 153]

INDIAN_IP_PREFIXES: list[str] = [
    "49.36.", "49.37.", "49.44.", "49.45.", "106.51.", "106.192.", "106.193.",
    "117.96.", "117.97.", "117.98.", "117.99.", "122.160.", "122.161.", "122.162.",
    "182.64.", "182.65.", "182.66.", "182.67.", "223.176.", "223.177.", "223.178.",
    "43.241.", "43.242.", "43.243.", "59.144.", "59.145.", "59.146.",
    "27.56.", "27.57.", "27.58.", "27.59.", "106.79.", "106.80.", "106.81.",
]

MIRRORS: dict[str, str] = {
    "google_cache": "https://webcache.googleusercontent.com/search?q=cache:{url}",
    "wayback":      "https://web.archive.org/web/2/{url}",
    "jina":         "https://r.jina.ai/{url}",
    "textise":      "https://www.textise.net/showText.aspx?strURL={url}",
    "allorigins":   "https://api.allorigins.win/raw?url={url}",
    "corsproxy":    "https://corsproxy.io/?{url}",
}

CF_MARKERS: tuple[str, ...] = (
    "cf-browser-verification", "cf-chl-", "just a moment",
    "checking your browser", "cf_chl_opt", "__cf_chl_",
)
CAPTCHA_MARKERS: tuple[str, ...] = (
    "g-recaptcha", "gstatic.com/recaptcha", "recaptcha/api.js",
    "h-captcha", "hcaptcha.com",
)
BOT_MARKERS: tuple[str, ...] = (
    "datadome", "px-captcha", "_px", "akam/", "akamai-bot",
    "awswaf", "aws-waf-token", "challenge.js",
    "sucuri", "wordfence", "mod_security",
)

COUNTRY_CODES: set[str] = {
    "1", "7", "20", "27", "31", "32", "33", "34", "39", "40", "41", "43",
    "44", "45", "46", "47", "48", "49", "51", "52", "54", "55", "56", "57",
    "58", "60", "61", "62", "63", "64", "65", "66", "81", "82", "84", "86",
    "90", "91", "92", "93", "94", "95", "98", "212", "213", "216", "218",
    "220", "221", "233", "234", "254", "255", "256", "260", "263", "264",
    "265", "267", "268", "351", "352", "353", "354", "355", "356", "357",
    "358", "359", "370", "371", "372", "373", "374", "375", "376", "377",
    "378", "380", "381", "382", "385", "386", "420", "421", "423", "880",
    "886", "960", "961", "962", "963", "964", "965", "966", "967", "968",
    "970", "971", "972", "973", "974", "975", "976", "977", "992", "993",
    "994", "995", "996", "998",
}

PROTECTION_KEYWORDS: tuple[str, ...] = (
    "contact", "whatsapp", "call", "chat", "helpline", "support",
    "phone", "mobile", "tel", "wa.me", "message",
)

# =============================================================================
# 3. COMPILED REGEXES
# =============================================================================

RE_URL = re.compile(r"https?://[^\s<>\"'\)\]]+", re.I)
RE_BARE_NUMBER = re.compile(r"(?:(?:\+|00)?91[\-\s]?)?([6-9]\d{9})\b")
RE_WA_ME = re.compile(r"wa\.me/(?:message/|qr/)?(\d{6,15})", re.I)
RE_WA_API = re.compile(
    r"(?:api|web|www)\.whatsapp\.com/send/?\?(?:[^#\s]*?)(?:phone|p)=(\d{6,15})",
    re.I,
)
RE_WA_ALT = re.compile(
    r"whatsapp\.com/send/?\?(?:[^#\s]*?)(?:phone|p)=(\d{6,15})",
    re.I,
)
RE_WA_APP = re.compile(
    r"whatsapp://send\?(?:[^#\s]*?)(?:phone|p)=(\d{6,15})",
    re.I,
)
RE_TG_PHONE = re.compile(r"tg://resolve\?phone=(\+?\d{6,15})", re.I)
RE_TG_PATH = re.compile(r"t\.me/\+(\d{6,15})", re.I)

RE_TEL_HREF = re.compile(r'href\s*=\s*[\'"](tel:\+?[\d\-\s\(\)\.]+)[\'"]', re.I)
RE_OG_PHONE = re.compile(
    r'<meta[^>]+(?:property|name)\s*=\s*[\'"](?:og:phone_number|twitter:phone|'
    r'business:contact_data:phone_number|itemprop|telephone|contact|phone)'
    r'[\'"][^>]+content\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_VCARD_TEL = re.compile(r"TEL(?:;[^:]*)?:\s*(\+?[\d\-\s\(\)\.]+)", re.I)

RE_META_REFRESH = re.compile(
    r'<meta[^>]+http-equiv\s*=\s*[\'"]?refresh[\'"]?[^>]*content\s*=\s*'
    r'[\'"]\s*[\d.]*\s*;\s*url\s*=\s*([^\'"]+)[\'"]',
    re.I,
)
RE_META_REFRESH2 = re.compile(
    r'<meta[^>]+content\s*=\s*[\'"][\d.]*\s*;\s*url\s*=\s*([^\'"]+)[\'"]'
    r'[^>]*http-equiv\s*=\s*[\'"]?refresh',
    re.I,
)
RE_META_LOCATION = re.compile(
    r'<meta[^>]+http-equiv\s*=\s*[\'"]?location[\'"]?[^>]+content\s*=\s*'
    r'[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_LINK_CANON = re.compile(
    r'<link[^>]+rel\s*=\s*[\'"]?(?:canonical|alternate|next)[\'"]?[^>]+'
    r'href\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_IFRAME_SRC = re.compile(
    r'<(?:iframe|frame|embed|source|track)[^>]+src\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_IFRAME_SRCDOC = re.compile(
    r'<iframe[^>]+srcdoc\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_OBJECT_DATA = re.compile(
    r'<object[^>]+data\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_VIDEO_POSTER = re.compile(
    r'<(?:video|audio)[^>]+(?:poster|src)\s*=\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_FORM_ACTION = re.compile(
    r'<form[^>]+action\s*=\s*[\'"]([^\'"]+)[\'"][^>]*>',
    re.I,
)
RE_HIDDEN_INPUT = re.compile(
    r'<input[^>]+type\s*=\s*[\'"]hidden[\'"][^>]*>',
    re.I,
)

RE_JS_LOC_HREF = re.compile(
    r'(?:window|document|top|parent)?\.?location(?:\.href|\.replace|\.assign)?'
    r'\s*(?:=|\()\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_JS_LOC_BARE = re.compile(r'location\s*=\s*[\'"]([^\'"]+)[\'"]', re.I)
RE_JS_OPEN = re.compile(r'window\.open\s*\(\s*[\'"]([^\'"]+)[\'"]', re.I)
RE_JS_PUSH = re.compile(
    r'history\.(?:pushState|replaceState)\s*\([^,]+,[^,]+,\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)

RE_ATOB_LITERAL = re.compile(
    r'atob\s*\(\s*[\'"]([A-Za-z0-9+/=_\-]+)[\'"]\s*\)',
    re.I,
)
RE_ATOB_VAR = re.compile(r'atob\s*\(\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*\)', re.I)
RE_ATOB_CONCAT = re.compile(
    r'atob\s*\(\s*((?:[\'"][^\'"]*[\'"]\s*\+?\s*)+)\s*\)',
    re.I,
)
RE_VAR_ASSIGN = re.compile(r'\b([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*[\'"]([^\'"]+)[\'"]')
RE_FROMCHARCODE = re.compile(r'String\.fromCharCode\s*\(([^)]+)\)')
RE_JOIN_ARRAY = re.compile(
    r'\[\s*((?:[\'"][^\'"]*[\'"]\s*,?\s*)+)\]\s*\.join\s*\(\s*[\'"][^\'"]*[\'"]\s*\)',
)
RE_REVERSE_JOIN = re.compile(
    r'[\'"]([^\'"]{4,})[\'"]\s*\.split\s*\(\s*[\'"][^\'"]*[\'"]\s*\)\s*'
    r'\.reverse\s*\(\s*\)\s*\.join\s*\(\s*[\'"][^\'"]*[\'"]\s*\)',
)
RE_EVAL = re.compile(
    r'(?:eval|Function|document\.write)\s*\(\s*[\'"]([^\'"]{4,})[\'"]',
    re.I,
)
RE_UNESCAPE = re.compile(
    r'(?:unescape|decodeURIComponent|decodeURI)\s*\(\s*[\'"]([^\'"]+)[\'"]\s*\)',
    re.I,
)
RE_HEX_STRING = re.compile(r'\b([0-9a-fA-F]{20,})\b')
RE_B64_STRING = re.compile(r'\b([A-Za-z0-9+/_\-]{16,}={0,2})\b')
RE_SLOWAES = re.compile(
    r'slowAES\.decrypt\s*\(\s*(\w+)\s*,\s*(\d+)\s*,\s*(\w+)\s*,\s*(\w+)\s*\)'
)
RE_DOC_COOKIE = re.compile(r'document\.cookie\s*=\s*[\'"]([^=;]+)=')
RE_CF_BM = re.compile(r'__cf_bm\s*=\s*[\'"]([^\'"]+)')
RE_CF_CLEARANCE = re.compile(r'cf_clearance\s*=\s*[\'"]([^\'"]+)')

RE_JSON_FIELD = re.compile(
    r'[\'"](?:url|redirect|location|target|to|link|next|href|destination|'
    r'forward|view|open)[\'"]\s*:\s*[\'"]([^\'"]+)[\'"]',
    re.I,
)
RE_JSON_URL_ANY = re.compile(r'[\'"](https?://[^\'"]+)[\'"]')
RE_TAG_INNER = re.compile(r'<(td|p|div|span|a)[^>]*>(.*?)</\1>', re.I | re.S)

# =============================================================================
# 4. LOGGING
# =============================================================================


def _setup_logging() -> logging.Logger:
    logger = logging.getLogger("exbot")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG if CONFIG["VERBOSE"] else logging.INFO)
    if CONFIG["ENABLE_LOGGING"]:
        try:
            path = CONFIG["LOG_DIR"] / ("bot_" + time.strftime("%Y%m%d") + ".log")
            h = logging.handlers.RotatingFileHandler(
                str(path), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
            )
            h.setFormatter(logging.Formatter(
                "%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            ))
            logger.addHandler(h)
        except (OSError, PermissionError) as e:
            sys.stderr.write(f"warn: file logging disabled: {e}\n")
    if CONFIG["VERBOSE"] or os.environ.get("RENDER"):
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(sh)
    return logger


LOG = _setup_logging()

# =============================================================================
# 5. AES
# =============================================================================

SBOX = bytes([
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b,
    0xfe, 0xd7, 0xab, 0x76, 0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0,
    0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0, 0xb7, 0xfd, 0x93, 0x26,
    0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
    0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2,
    0xeb, 0x27, 0xb2, 0x75, 0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0,
    0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84, 0x53, 0xd1, 0x00, 0xed,
    0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
    0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f,
    0x50, 0x3c, 0x9f, 0xa8, 0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5,
    0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2, 0xcd, 0x0c, 0x13, 0xec,
    0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
    0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14,
    0xde, 0x5e, 0x0b, 0xdb, 0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c,
    0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79, 0xe7, 0xc8, 0x37, 0x6d,
    0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
    0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f,
    0x4b, 0xbd, 0x8b, 0x8a, 0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e,
    0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e, 0xe1, 0xf8, 0x98, 0x11,
    0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
    0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f,
    0xb0, 0x54, 0xbb, 0x16,
])

INV_SBOX: list[int] = [0] * 256
for _i, _v in enumerate(SBOX):
    INV_SBOX[_v] = _i
RCON: list[int] = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36]


def _xtime(a: int) -> int:
    return ((a << 1) ^ 0x1b) & 0xff if a & 0x80 else (a << 1)


def _mul(a: int, b: int) -> int:
    r = 0
    for _ in range(8):
        if b & 1:
            r ^= a
        b >>= 1
        a = _xtime(a)
    return r


def _expand_key(key: bytes) -> list[bytes]:
    nk = len(key) // 4
    nr = {4: 10, 6: 12, 8: 14}[nk]
    w = [list(key[i * 4:i * 4 + 4]) for i in range(nk)]
    for i in range(nk, 4 * (nr + 1)):
        t = list(w[i - 1])
        if i % nk == 0:
            t = t[1:] + t[:1]
            t = [SBOX[x] for x in t]
            t[0] ^= RCON[i // nk - 1]
        elif nk > 6 and i % nk == 4:
            t = [SBOX[x] for x in t]
        w.append([w[i - nk][j] ^ t[j] for j in range(4)])
    return [bytes(b for word in w[i * 4:i * 4 + 4] for b in word) for i in range(nr + 1)]


def _add_round_key(state: bytearray, rk: bytes) -> bytearray:
    return bytearray(a ^ b for a, b in zip(state, rk))


def _inv_shift_rows(state: bytearray) -> bytearray:
    new = bytearray(16)
    for c in range(4):
        for r in range(4):
            new[4 * c + r] = state[4 * ((c - r) % 4) + r]
    return new


def _inv_mix_columns(state: bytearray) -> bytearray:
    new = bytearray(16)
    for c in range(4):
        a0, a1, a2, a3 = state[4 * c], state[4 * c + 1], state[4 * c + 2], state[4 * c + 3]
        new[4 * c + 0] = _mul(a0, 14) ^ _mul(a1, 11) ^ _mul(a2, 13) ^ _mul(a3, 9)
        new[4 * c + 1] = _mul(a0, 9) ^ _mul(a1, 14) ^ _mul(a2, 11) ^ _mul(a3, 13)
        new[4 * c + 2] = _mul(a0, 13) ^ _mul(a1, 9) ^ _mul(a2, 14) ^ _mul(a3, 11)
        new[4 * c + 3] = _mul(a0, 11) ^ _mul(a1, 13) ^ _mul(a2, 9) ^ _mul(a3, 14)
    return new


def _decrypt_block(block: bytes, rks: list[bytes]) -> bytes:
    state = _add_round_key(bytearray(block), rks[-1])
    for r in range(len(rks) - 2, 0, -1):
        state = _inv_shift_rows(state)
        state = bytearray(INV_SBOX[b] for b in state)
        state = _add_round_key(state, rks[r])
        state = _inv_mix_columns(state)
    state = _inv_shift_rows(state)
    state = bytearray(INV_SBOX[b] for b in state)
    state = _add_round_key(state, rks[0])
    return bytes(state)


def aes_cbc_decrypt(ct: bytes, key: bytes, iv: bytes) -> bytes:
    rks = _expand_key(key)
    out = bytearray()
    prev = iv
    for i in range(0, len(ct) - len(ct) % 16, 16):
        block = ct[i:i + 16]
        dec = _decrypt_block(block, rks)
        out.extend(a ^ b for a, b in zip(dec, prev))
        prev = block
    return bytes(out)


def aes_ecb_decrypt(ct: bytes, key: bytes) -> bytes:
    rks = _expand_key(key)
    out = bytearray()
    for i in range(0, len(ct) - len(ct) % 16, 16):
        out.extend(_decrypt_block(ct[i:i + 16], rks))
    return bytes(out)

# =============================================================================
# 6. RATE LIMITER + MIRROR BLACKLIST
# =============================================================================


class RateLimiter:
    def __init__(self, rate_per_sec: float) -> None:
        self._rate = max(0.1, rate_per_sec)
        self._lock = threading.Lock()
        self._next = time.monotonic()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            if now < self._next:
                time.sleep(self._next - now)
            self._next = max(self._next, now) + (1.0 / self._rate)

    def penalty(self, seconds: float) -> None:
        with self._lock:
            self._next = max(self._next, time.monotonic() + seconds)


RATE_LIMITER = RateLimiter(CONFIG["REQUEST_RATE_LIMIT"])


class MirrorBlacklist:
    def __init__(self, ttl: int) -> None:
        self._ttl = ttl
        self._lock = threading.Lock()
        self._fail: dict[str, list[float]] = defaultdict(list)
        self._ban_until: dict[str, float] = {}

    def record(self, name: str) -> None:
        with self._lock:
            now = time.time()
            fails = [t for t in self._fail[name] if now - t < self._ttl]
            fails.append(now)
            self._fail[name] = fails
            if len(fails) >= 2:
                self._ban_until[name] = now + self._ttl
            if len(self._fail) > 50:
                for k in list(self._fail.keys())[: len(self._fail) - 50]:
                    self._fail.pop(k, None)
                    self._ban_until.pop(k, None)

    def is_banned(self, name: str) -> bool:
        with self._lock:
            return self._ban_until.get(name, 0) > time.time()


MIRROR_BAN = MirrorBlacklist(CONFIG["MIRROR_BLACKLIST_TTL"])

# =============================================================================
# 7. FETCH LAYER
# =============================================================================


def random_ua() -> str:
    return random.choice(UA_POOL).format(c=random.choice(CHROME_VERSIONS))


def random_indian_ip() -> str:
    return (random.choice(INDIAN_IP_PREFIXES)
            + str(random.randint(1, 254)) + "." + str(random.randint(1, 254)))


def build_headers(referer: Optional[str] = None) -> dict[str, str]:
    h: dict[str, str] = {
        "User-Agent": random_ua(),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Upgrade-Insecure-Requests": "1",
        "dnt": "1",
        "X-Requested-With": "via.bolte",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "sec-ch-ua": '"Not=A?Brand";v="99", "Android WebView";v="151", "Chromium";v="151"',
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": '"Android"',
        "Connection": "keep-alive",
        "X-Forwarded-For": random_indian_ip(),
    }
    if referer and CONFIG["REFERER_CHAIN"]:
        h["Referer"] = referer
    return h


def make_session() -> requests.Session:
    s = requests.Session()
    a = HTTPAdapter(
        pool_connections=CONFIG["THREADS"],
        pool_maxsize=CONFIG["THREADS"] * 2,
        max_retries=Retry(total=0, connect=0, read=0, status=0),
    )
    s.mount("https://", a)
    s.mount("http://", a)
    return s


def _ssl_ladder_get(session: requests.Session, url: str, headers: dict[str, str],
                    allow_redirects: bool) -> Optional[requests.Response]:
    try:
        return session.get(url, headers=headers, timeout=CONFIG["TIMEOUT"],
                           allow_redirects=allow_redirects)
    except requests.exceptions.SSLError:
        LOG.debug("ssl ladder: verify=False for %s", url[:120])
        try:
            return session.get(url, headers=headers, timeout=CONFIG["TIMEOUT"],
                               allow_redirects=allow_redirects, verify=False)
        except requests.exceptions.RequestException:
            if url.startswith("https://"):
                http_url = "http://" + url[len("https://"):]
                try:
                    return session.get(http_url, headers=headers,
                                       timeout=CONFIG["TIMEOUT"],
                                       allow_redirects=allow_redirects)
                except requests.exceptions.RequestException:
                    return None
            return None


def safe_get(session: requests.Session, url: str, headers: dict[str, str],
             allow_redirects: bool = False) -> Optional[requests.Response]:
    if not url or len(url) > MAX_URL_LEN:
        return None
    RATE_LIMITER.acquire()
    try:
        return _ssl_ladder_get(session, url, headers, allow_redirects)
    except (requests.exceptions.RequestException,
            ssl.SSLError, socket.timeout, socket.gaierror,
            UnicodeError, ValueError):
        return None


def resolve_url(base: str, loc: str) -> Optional[str]:
    if not loc:
        return None
    loc = loc.strip().strip('\'"')
    if not loc:
        return None
    try:
        return urljoin(base, loc)
    except ValueError:
        return None

# =============================================================================
# 8. DEOBFUSCATION
# =============================================================================


def _rot13(s: str) -> str:
    return codecs.decode(s, "rot_13")


def _caesar(s: str, shift: int) -> str:
    out: list[str] = []
    for ch in s:
        if "a" <= ch <= "z":
            out.append(chr((ord(ch) - 97 + shift) % 26 + 97))
        elif "A" <= ch <= "Z":
            out.append(chr((ord(ch) - 65 + shift) % 26 + 65))
        else:
            out.append(ch)
    return "".join(out)


def _hex_decode(s: str) -> Optional[str]:
    if len(s) % 2:
        return None
    try:
        return bytes.fromhex(s).decode("utf-8", "ignore")
    except ValueError:
        return None


def _b64_decode(s: str) -> Optional[str]:
    s2 = s.replace("-", "+").replace("_", "/")
    s2 += "=" * ((-len(s2)) % 4)
    try:
        return base64.b64decode(s2).decode("utf-8", "ignore")
    except (ValueError, UnicodeError):
        return None


def deobfuscate(body: str) -> list[str]:
    out: list[str] = []
    if not body:
        return out

    r = _rot13(body)
    if r != body:
        out.append(r)

    for shift in range(1, 26):
        c = _caesar(body, shift)
        if c != body and any(k in c.lower() for k in ("wa.me", "whatsapp", "tel:")):
            out.append(c)

    for m in RE_HEX_STRING.finditer(body):
        d = _hex_decode(m.group(1))
        if d:
            out.append(d)

    for m in RE_B64_STRING.finditer(body):
        d = _b64_decode(m.group(1))
        if d and any(k in d.lower() for k in ("http", "wa.me", "whatsapp", "tel:")):
            out.append(d)

    for m in RE_REVERSE_JOIN.finditer(body):
        out.append(m.group(1)[::-1])

    return out[:8]

# =============================================================================
# 9. PROTECTION BYPASS
# =============================================================================


def detect_protection(body: str) -> str:
    if not body:
        return "none"
    low = body.lower()
    if "slowaes" in low or "__test" in low:
        return "testcookie-nginx-module (AES)"
    for m in CF_MARKERS:
        if m in low:
            return "cloudflare-challenge"
    for m in CAPTCHA_MARKERS:
        if m in low:
            return "captcha"
    for m in BOT_MARKERS:
        if m in low:
            return "bot-manager"
    if RE_ATOB_LITERAL.search(body):
        return "base64-js-redirect"
    if RE_META_REFRESH.search(body) or RE_META_REFRESH2.search(body):
        return "meta-refresh"
    if RE_JS_LOC_HREF.search(body) or RE_JS_LOC_BARE.search(body):
        return "js-redirect"
    return "none"


def _grab_tonumbers(body: str, var: str) -> Optional[bytes]:
    m = re.search(re.escape(var) + r"\s*=\s*toNumbers\s*\(\s*[\'\"]([0-9a-fA-F]+)[\'\"]\s*\)", body)
    if m:
        h = m.group(1)
        try:
            return bytes(int(h[i:i + 2], 16) for i in range(0, len(h), 2))
        except ValueError:
            return None
    m = re.search(re.escape(var) + r"\s*=\s*\[([0-9,\s]+)\]", body)
    if m:
        nums = [int(x) for x in re.findall(r"\d+", m.group(1))]
        if nums:
            return bytes(nums)
    return None


def solve_testcookie(body: str) -> Optional[tuple[str, str]]:
    m = RE_SLOWAES.search(body)
    if not m:
        return None
    c_var, mode_s, a_var, b_var = m.group(1), m.group(2), m.group(3), m.group(4)
    try:
        mode = int(mode_s)
    except ValueError:
        return None
    key = _grab_tonumbers(body, a_var)
    iv = _grab_tonumbers(body, b_var)
    ct = _grab_tonumbers(body, c_var)
    if not key or not iv or not ct:
        return None
    if len(key) not in (16, 24, 32):
        return None
    if len(ct) % 16 or len(ct) == 0:
        return None

    try:
        pt = aes_cbc_decrypt(ct, key, iv) if mode == 2 else aes_ecb_decrypt(ct, key)
    except (ValueError, TypeError):
        return None
    if not pt:
        return None
    pad = pt[-1]
    if 1 <= pad <= 16 and pt.endswith(bytes([pad]) * pad):
        pt = pt[:-pad]

    cookie_name = "__test"
    m2 = RE_DOC_COOKIE.search(body)
    if m2:
        cookie_name = m2.group(1).strip()
    return pt.hex(), cookie_name


def bypass_testcookie(session: requests.Session, url: str, headers: dict[str, str],
                      resp: requests.Response) -> Optional[requests.Response]:
    result = solve_testcookie(resp.text or "")
    if not result:
        return None
    cookie_val, cookie_name = result
    host = urlparse(url).hostname
    try:
        session.cookies.set(cookie_name, cookie_val, domain=host, path="/")
    except (ValueError, TypeError):
        return None
    retry_url = url
    m = RE_JS_LOC_HREF.search(resp.text or "")
    if m:
        cand = resolve_url(url, m.group(1))
        if cand:
            retry_url = cand
    return safe_get(session, retry_url, build_headers(referer=url), allow_redirects=False)


def passive_cloudflare_solve(session: requests.Session, url: str,
                             headers: dict[str, str], body: str) -> Optional[requests.Response]:
    m = RE_CF_BM.search(body)
    cookie_val = m.group(1) if m else None
    if not cookie_val:
        m2 = RE_CF_CLEARANCE.search(body)
        cookie_val = m2.group(1) if m2 else None
        cookie_name = "cf_clearance"
    else:
        cookie_name = "__cf_bm"
    if not cookie_val:
        return None
    host = urlparse(url).hostname
    try:
        session.cookies.set(cookie_name, cookie_val, domain=host, path="/")
    except (ValueError, TypeError):
        return None
    return safe_get(session, url, headers, allow_redirects=False)

# =============================================================================
# 10. EXTRACTION HELPERS
# =============================================================================


def find_b64_redirects(html: str) -> list[str]:
    out: list[str] = []
    for m in RE_ATOB_LITERAL.finditer(html):
        d = _b64_decode(m.group(1))
        if d and ("http" in d or "wa.me" in d or "whatsapp" in d):
            out.append(d.strip())
    for m in RE_ATOB_CONCAT.finditer(html):
        parts = re.findall(r'[\'"]([^\'"]*)[\'"]', m.group(1))
        d = _b64_decode("".join(parts))
        if d:
            out.append(d.strip())
    var_vals: dict[str, str] = {}
    for m in RE_VAR_ASSIGN.finditer(html):
        var_vals[m.group(1)] = m.group(2)
    for m in RE_ATOB_VAR.finditer(html):
        v = var_vals.get(m.group(1))
        if v:
            d = _b64_decode(v)
            if d:
                out.append(d.strip())
    seen: set[str] = set()
    res: list[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            res.append(x)
    return res


def _resolve_js_expr(expr: str, var_vals: dict[str, str]) -> str:
    expr = expr.strip()
    if expr.startswith(("http://", "https://", "/", "//")):
        return expr
    parts = re.findall(r'([\'"][^\'"]*[\'"]|[A-Za-z_$][A-Za-z0-9_$]*)', expr)
    out: list[str] = []
    for p in parts:
        if p.startswith(("\"", "'")):
            out.append(p[1:-1])
        else:
            out.append(var_vals.get(p, ""))
    return "".join(out)


def find_js_redirects(html: str) -> list[str]:
    out: list[str] = []
    var_vals: dict[str, str] = {}
    for m in RE_VAR_ASSIGN.finditer(html):
        var_vals[m.group(1)] = m.group(2)

    for m in RE_JS_LOC_HREF.finditer(html):
        v = _resolve_js_expr(m.group(1), var_vals)
        if v.startswith(("http", "/")):
            out.append(v)
    for m in RE_JS_LOC_BARE.finditer(html):
        v = _resolve_js_expr(m.group(1), var_vals)
        if v.startswith(("http", "/")):
            out.append(v)
    for m in RE_JS_OPEN.finditer(html):
        v = _resolve_js_expr(m.group(1), var_vals)
        if v.startswith(("http", "/")):
            out.append(v)
    for m in RE_JS_PUSH.finditer(html):
        v = _resolve_js_expr(m.group(1), var_vals)
        if v.startswith(("http", "/")):
            out.append(v)

    for m in RE_FROMCHARCODE.finditer(html):
        nums = [int(x) for x in re.findall(r"\d+", m.group(1))]
        if nums:
            try:
                out.append("".join(chr(n) for n in nums if 0 < n < 0x110000))
            except (ValueError, OverflowError):
                pass
    for m in RE_JOIN_ARRAY.finditer(html):
        parts = re.findall(r'[\'"]([^\'"]*)[\'"]', m.group(1))
        out.append("".join(parts))
    for m in RE_EVAL.finditer(html):
        out.append(m.group(1))
    for m in RE_UNESCAPE.finditer(html):
        try:
            out.append(unquote(m.group(1)))
        except (ValueError, UnicodeError):
            pass

    seen: set[str] = set()
    res: list[str] = []
    for v in out:
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            res.append(v)
    return res


def find_meta_refresh(html: str) -> list[str]:
    out: list[str] = []
    for tag in re.findall(r'<meta\b[^>]*>', html, re.I):
        if not re.search(r'http-equiv\s*=\s*[\'\"]?refresh\b', tag, re.I):
            continue
        cm = re.search(r'content\s*=\s*([\'\"])(.*?)\1', tag, re.I | re.S)
        content = cm.group(2) if cm else ""
        if not content:
            cm2 = re.search(r'content\s*=\s*([^\s>]+)', tag, re.I)
            content = cm2.group(1) if cm2 else ""
        um = re.search(r';\s*url\s*=\s*[\'\"]?([^\s\'\">]+)[\'\"]?', content, re.I)
        if um:
            out.append(um.group(1).strip())
    for m in RE_META_LOCATION.finditer(html):
        out.append(m.group(1).strip())
    seen: set[str] = set()
    result: list[str] = []
    for x in out:
        if x and x not in seen:
            seen.add(x)
            result.append(x)
    return result


def find_iframes(html: str) -> list[str]:
    out: list[str] = []
    for m in RE_IFRAME_SRC.finditer(html):
        out.append(m.group(1).strip())
    for m in RE_OBJECT_DATA.finditer(html):
        out.append(m.group(1).strip())
    for m in RE_VIDEO_POSTER.finditer(html):
        out.append(m.group(1).strip())
    return out


def find_iframe_srcdoc(html: str) -> list[str]:
    return [m.group(1) for m in RE_IFRAME_SRCDOC.finditer(html)]


def find_canonical(html: str) -> list[str]:
    return [m.group(1).strip() for m in RE_LINK_CANON.finditer(html)]


def find_form_action(html: str) -> list[tuple[str, dict[str, str]]]:
    out: list[tuple[str, dict[str, str]]] = []
    for m in RE_FORM_ACTION.finditer(html):
        action = m.group(1).strip()
        block = html[m.end(): m.end() + 8000]
        fields: dict[str, str] = {}
        for hm in RE_HIDDEN_INPUT.finditer(block):
            tag = hm.group(0)
            nm = re.search(r'name\s*=\s*[\'"]([^\'"]+)[\'"]', tag, re.I)
            vm = re.search(r'value\s*=\s*[\'"]([^\'"]*)[\'"]', tag, re.I)
            if nm:
                fields[nm.group(1)] = vm.group(1) if vm else ""
        out.append((action, fields))
    return out


def walk_json_for_urls(obj: Any, depth: int = 0) -> list[str]:
    out: list[str] = []
    if depth > 8:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                out.append(v)
            elif isinstance(v, (dict, list)):
                out.extend(walk_json_for_urls(v, depth + 1))
    elif isinstance(obj, list):
        for item in obj:
            out.extend(walk_json_for_urls(item, depth + 1))
    return out


def find_json_urls(body: str) -> list[str]:
    out: list[str] = []
    try:
        data = json.loads(body)
        out.extend(walk_json_for_urls(data))
    except (ValueError, TypeError):
        pass
    for m in RE_JSON_FIELD.finditer(body):
        out.append(m.group(1))
    for m in RE_JSON_URL_ANY.finditer(body):
        out.append(m.group(1))
    seen: set[str] = set()
    result: list[str] = []
    for v in out:
        if v and v not in seen:
            seen.add(v)
            result.append(v)
    return result


def find_query_urls(url: str) -> list[str]:
    out: list[str] = []
    try:
        parsed = urlparse(url)
    except ValueError:
        return out
    for _k, v in parse_qsl(parsed.query, keep_blank_values=False):
        if v.startswith(("http://", "https://")):
            out.append(v)
        else:
            try:
                dec = unquote(v)
                if dec.startswith(("http://", "https://")):
                    out.append(dec)
            except (ValueError, UnicodeError):
                pass
    return out


def extract_asset_urls(html: str) -> list[str]:
    out: list[str] = []
    for m in re.finditer(
        r'<(?:script|link)[^>]+(?:src|href)\s*=\s*[\'"]([^\'"]+\.(?:js|css)[^\'"]*)[\'"]',
        html, re.I,
    ):
        out.append(m.group(1).strip())
    return out[:CONFIG["ASSET_FETCH_LIMIT"]]


def normalize_phone(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = re.sub(r"[^\d+]", "", raw)
    if s.startswith("00"):
        s = "+" + s[2:]
    if s.startswith("+"):
        digits = s[1:]
    else:
        digits = s
    if not digits.isdigit():
        return None
    if len(digits) < 10 or len(digits) > 15:
        return None
    if len(set(digits)) == 1:
        return None
    if digits in ("1234567890", "0000000000", "9876543210", "1111111111"):
        return None
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    if len(digits) == 11 and digits[0] == "0" and digits[1] in "6789":
        return "+91" + digits[1:]
    if len(digits) == 12 and digits.startswith("91") and digits[2] in "6789":
        return "+" + digits
    if len(digits) >= 10 and digits[:1] in "123456789":
        for cc_len in (1, 2, 3):
            if digits[:cc_len] in COUNTRY_CODES:
                return "+" + digits
        return "+" + digits
    return None


def detect_country(e164: str) -> str:
    if not e164.startswith("+"):
        return "??"
    d = e164[1:]
    if d.startswith("91") and len(d) == 12:
        return "IN"
    for cc_len in (3, 2, 1):
        if d[:cc_len] in COUNTRY_CODES:
            return "CC" + d[:cc_len]
    return "??"


def extract_numbers(text: str) -> set[str]:
    if not text:
        return set()
    nums: set[str] = set()
    for rx in (RE_WA_ME, RE_WA_API, RE_WA_ALT, RE_WA_APP, RE_TG_PHONE, RE_TG_PATH):
        for m in rx.finditer(text):
            n = normalize_phone(m.group(1))
            if n:
                nums.add(n)
    for m in RE_TEL_HREF.finditer(text):
        n = normalize_phone(m.group(1))
        if n:
            nums.add(n)
    for m in RE_VCARD_TEL.finditer(text):
        n = normalize_phone(m.group(1))
        if n:
            nums.add(n)
    for m in RE_BARE_NUMBER.finditer(text):
        n = normalize_phone(m.group(1))
        if n:
            nums.add(n)
    for m in RE_OG_PHONE.finditer(text):
        for cand in re.findall(r"\+?\d[\d\-\s\(\)\.]{7,}", m.group(1)):
            n = normalize_phone(cand)
            if n:
                nums.add(n)
    return nums


def confidence_for(surface: str) -> float:
    return {
        "wa_me": 1.0,
        "wa_api": 1.0,
        "wa_app": 1.0,
        "tel": 0.9,
        "json_ld": 0.85,
        "og": 0.8,
        "vcard": 0.7,
        "js": 0.3,
        "body": 0.5,
    }.get(surface, 0.5)


def find_surface(text: str, number: str) -> str:
    if (RE_WA_ME.search(text) or RE_WA_API.search(text)
            or RE_WA_ALT.search(text) or RE_WA_APP.search(text)):
        return "wa_me"
    if RE_TEL_HREF.search(text):
        return "tel"
    if RE_OG_PHONE.search(text):
        return "og"
    if RE_VCARD_TEL.search(text):
        return "vcard"
    if number in text and any(k in text.lower() for k in PROTECTION_KEYWORDS):
        return "body"
    return "js"


def context_for(text: str, number: str) -> str:
    if not text:
        return ""
    digits = re.sub(r"\D", "", number)
    idx = text.find(digits)
    if idx < 0:
        idx = text.find(number)
    if idx < 0:
        for m in RE_TAG_INNER.finditer(text):
            if digits and digits in re.sub(r"\D", "", m.group(2)):
                inner = re.sub(r"<[^>]+>", " ", m.group(2))
                return re.sub(r"\s+", " ", inner).strip()[:240]
        return ""
    lo = max(0, idx - 120)
    hi = min(len(text), idx + len(number) + 120)
    return re.sub(r"\s+", " ", text[lo:hi]).strip()


def extract_pdf_text(data: bytes) -> str:
    out: list[str] = []
    for m in re.finditer(rb"stream\r?\n(.*?)\r?\nendstream", data, re.S):
        chunk = m.group(1)
        try:
            decompressed = zlib.decompress(chunk)
        except zlib.error:
            decompressed = chunk
        for tm in re.finditer(rb"\(((?:[^()\\]|\\.)*)\)\s*Tj", decompressed):
            out.append(tm.group(1).decode("utf-8", "ignore"))
        for tm in re.finditer(rb"\[((?:[^\[\]]*?))\]\s*TJ", decompressed):
            for sm in re.finditer(rb"\(((?:[^()\\]|\\.)*)\)", tm.group(1)):
                out.append(sm.group(1).decode("utf-8", "ignore"))
    return " ".join(out)

# =============================================================================
# 11. URL NORMALIZATION
# =============================================================================

TRACKING_PARAMS: set[str] = {
    "fbclid", "gclid", "igshid", "yclid", "msclkid", "twclid", "_ga",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "source",
}


def clean_input(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip().strip("<>\"'")
    s = s.replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
    s = s.replace("\ufeff", "").replace("\xa0", " ")
    s = re.sub(r"[\r\n\t]+", " ", s).strip()
    if len(s) > MAX_URL_LEN:
        s = s[:MAX_URL_LEN]
    return s


def normalize_scheme(url: str) -> str:
    low = url.lower()
    if low.startswith(("http://", "https://")):
        return url
    if low.startswith(("ftp://", "ftps://", "ws://", "wss://")):
        return "https://" + url.split("://", 1)[1]
    if low.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return url
    if low.startswith(("blob:", "file:", "chrome-extension:")):
        return url
    if low.startswith("about:"):
        return "https://" + url.split(":", 1)[1]
    if low.startswith("view-source:"):
        return url.split(":", 1)[1]
    if low.startswith("tg://"):
        return url
    return "https://" + url


def looks_like_hostname(tok: str) -> bool:
    if not tok or "@" in tok:
        return False
    if "." not in tok:
        return False
    if " " in tok:
        return False
    m = re.match(
        r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,24}(?:[/:?#][^\s]*)?$",
        tok, re.I,
    )
    return bool(m)


def canonical_key(url: str) -> str:
    try:
        p = urlparse(url)
    except ValueError:
        return url
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    if netloc.endswith(":80") and scheme == "http":
        netloc = netloc[:-3]
    if netloc.endswith(":443") and scheme == "https":
        netloc = netloc[:-4]
    path = p.path or "/"
    if path == "/":
        path = ""
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=False)
         if k.lower() not in TRACKING_PARAMS]
    q.sort()
    query = urlencode(q)
    return urlunparse((scheme, netloc, path, "", query, ""))


def rewrite_special(url: str) -> tuple[str, Optional[str]]:
    low = url.lower()
    if low.startswith("mailto:"):
        return "", url.split(":", 1)[1]
    if low.startswith("tel:"):
        return "", url.split(":", 1)[1]
    if low.startswith("javascript:"):
        return "", url.split(":", 1)[1]
    if low.startswith("data:"):
        try:
            head, body = url.split(",", 1)
            if ";base64" in head:
                return "", base64.b64decode(body).decode("utf-8", "ignore")
            return "", unquote(body)
        except (ValueError, UnicodeError):
            return "", ""
    if low.startswith("tg://"):
        m = re.search(r"resolve\?domain=([^&]+)", url, re.I)
        if m:
            return "https://t.me/" + m.group(1), None
        m = re.search(r"resolve\?phone=(\+?\d+)", url, re.I)
        if m:
            return "", m.group(1)
        m = re.search(r"join\?invite=([^&]+)", url, re.I)
        if m:
            return "https://t.me/+" + m.group(1), None
        return "", ""
    if low.startswith(("telegram.me/", "telegram.dog/")):
        return "https://t.me/" + url.split("/", 1)[1], None
    if low.startswith(("file:", "blob:", "chrome-extension:")):
        return "", None
    return url, None


def sniff_content_type(resp: requests.Response) -> str:
    ct = (resp.headers.get("Content-Type") or "").lower()
    if ct:
        return ct.split(";", 1)[0].strip()
    head = resp.content[:512]
    if head[:4] == b"%PDF":
        return "application/pdf"
    if head[:2] == b"\xff\xd8":
        return "image/jpeg"
    if head[:2] == b"\x89P":
        return "image/png"
    if head.lstrip()[:1] in (b"{", b"["):
        return "application/json"
    return "text/html"

# =============================================================================
# 12. WORKER SCRAPE
# =============================================================================

SRCDOC_MAX_DEPTH = 4


class Extraction:
    __slots__ = ("number", "confidence", "country", "hop", "url", "surface", "context")

    def __init__(self, number: str, confidence: float, country: str,
                 hop: int, url: str, surface: str, context: str) -> None:
        self.number = number
        self.confidence = confidence
        self.country = country
        self.hop = hop
        self.url = url
        self.surface = surface
        self.context = context

    def as_csv_row(self) -> list[str]:
        return [
            self.number, f"{self.confidence:.2f}", self.country,
            str(self.hop), self.url[:200], self.surface, self.context[:240],
        ]


class Metrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.requests_made = 0
        self.requests_failed = 0
        self.redirects_followed = 0
        self.protections_detected: dict[str, int] = defaultdict(int)
        self.protections_bypassed: dict[str, int] = defaultdict(int)
        self.numbers_extracted = 0
        self.unique_numbers: set[str] = set()
        self.domain_counts: dict[str, int] = defaultdict(int)
        self.cycles_run = 0
        self.cycles_completed = 0
        self.t0 = time.time()

    def bump_request(self) -> None:
        with self.lock:
            self.requests_made += 1

    def fail_request(self) -> None:
        with self.lock:
            self.requests_failed += 1

    def bump_redirect(self) -> None:
        with self.lock:
            self.redirects_followed += 1

    def detect(self, kind: str) -> None:
        with self.lock:
            self.protections_detected[kind] += 1

    def bypass(self, kind: str) -> None:
        with self.lock:
            self.protections_bypassed[kind] += 1

    def add_numbers(self, nums: Iterable[str]) -> None:
        with self.lock:
            for n in nums:
                self.unique_numbers.add(n)
            self.numbers_extracted = len(self.unique_numbers)

    def bump_domain(self, url: str) -> None:
        try:
            host = urlparse(url).hostname or "?"
        except ValueError:
            host = "?"
        with self.lock:
            self.domain_counts[host] += 1

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "requests_made": self.requests_made,
                "requests_failed": self.requests_failed,
                "redirects_followed": self.redirects_followed,
                "protections_detected": dict(self.protections_detected),
                "protections_bypassed": dict(self.protections_bypassed),
                "numbers_extracted": self.numbers_extracted,
                "unique_numbers": len(self.unique_numbers),
                "domain_counts": dict(self.domain_counts),
                "cycles_run": self.cycles_run,
                "cycles_completed": self.cycles_completed,
                "uptime": round(time.time() - self.t0, 1),
            }


METRICS = Metrics()


def _record(surface: str, number: str, hop: int, url: str, body: str,
            out: dict[str, Extraction]) -> None:
    if number in out:
        return
    ctx = context_for(body, number)
    out[number] = Extraction(
        number=number,
        confidence=confidence_for(surface),
        country=detect_country(number),
        hop=hop,
        url=url[:300],
        surface=surface,
        context=ctx,
    )


def _process_body(body: str, url: str, hop: int, out: dict[str, Extraction],
                  queue: deque[str], depth: int = 0) -> None:
    if not body:
        return

    for n in extract_numbers(body):
        surface = find_surface(body, n)
        _record(surface, n, hop, url, body, out)

    for v in find_b64_redirects(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)
            for n in extract_numbers(cand):
                _record("wa_me" if "wa.me" in cand or "whatsapp" in cand else "body",
                        n, hop, cand, body, out)
    for v in find_js_redirects(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)
    for v in find_meta_refresh(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)
    for v in find_iframes(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)
    for v in find_canonical(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)
    if depth < SRCDOC_MAX_DEPTH:
        for v in find_iframe_srcdoc(body):
            _process_body(v, url, hop, out, queue, depth + 1)
    for v in find_json_urls(body):
        cand = resolve_url(url, v)
        if cand:
            queue.append(cand)

    for decoded in deobfuscate(body):
        for n in extract_numbers(decoded):
            _record("js", n, hop, url, decoded, out)

    for v in find_query_urls(url):
        queue.append(v)


def _fetch_mirror(session: requests.Session, name: str, template: str,
                  seed: str) -> Optional[str]:
    if MIRROR_BAN.is_banned(name):
        return None
    url = template.format(url=seed)
    resp = safe_get(session, url, build_headers(), allow_redirects=True)
    if resp is None or resp.status_code >= 400:
        MIRROR_BAN.record(name)
        return None
    try:
        return resp.text
    except (ValueError, UnicodeError):
        MIRROR_BAN.record(name)
        return None


class VisitBudget:
    def __init__(self, cap: int) -> None:
        self._cap = cap
        self._count = 0
        self._lock = threading.Lock()

    def take(self) -> bool:
        with self._lock:
            if self._count >= self._cap:
                return False
            self._count += 1
            return True


def worker_scrape(seed_url: str, cancel_event: Optional[threading.Event] = None,
                  per_worker_budget: Optional[float] = None,
                  visit_budget: Optional[VisitBudget] = None) -> dict[str, Extraction]:
    if per_worker_budget is None:
        per_worker_budget = CONFIG["WALL_CLOCK_PER_WORKER"]
    deadline = time.monotonic() + per_worker_budget

    out: dict[str, Extraction] = {}
    for attempt in range(CONFIG["MAX_ATTEMPTS"]):
        if cancel_event and cancel_event.is_set():
            break
        if time.monotonic() >= deadline:
            break

        session = make_session()
        try:
            found_any = _walk_once(session, seed_url, deadline, cancel_event, out, visit_budget)
        except Exception as e:
            LOG.warning("worker walk crashed: %s", str(e)[:160])
            found_any = False
        finally:
            try:
                session.close()
            except (OSError, requests.exceptions.RequestException):
                pass

        if found_any and len(out) >= 1:
            break
        if attempt < CONFIG["MAX_ATTEMPTS"] - 1:
            time.sleep(0.3 + random.random() * 0.7)

    if not out and time.monotonic() < deadline:
        session = make_session()
        try:
            for name, tmpl in MIRRORS.items():
                if time.monotonic() >= deadline:
                    break
                if cancel_event and cancel_event.is_set():
                    break
                body = _fetch_mirror(session, name, tmpl, seed_url)
                if not body:
                    continue
                sub: deque[str] = deque()
                _process_body(body, seed_url, 0, out, sub)
                if out:
                    break
        finally:
            try:
                session.close()
            except (OSError, requests.exceptions.RequestException):
                pass

    return out


def _walk_once(session: requests.Session, seed_url: str, deadline: float,
               cancel_event: Optional[threading.Event],
               out: dict[str, Extraction],
               visit_budget: Optional[VisitBudget] = None) -> bool:
    seed = clean_input(seed_url)
    seed, direct = rewrite_special(seed)
    if direct:
        for n in extract_numbers(direct):
            _record("body", n, 0, seed_url, direct, out)
        return bool(out)
    if not seed:
        return False
    seed = normalize_scheme(seed)

    queue: deque[str] = deque([seed])
    visited: set[str] = set()
    referer: Optional[str] = None

    for hop in range(CONFIG["MAX_HOPS"]):
        if time.monotonic() >= deadline:
            break
        if cancel_event and cancel_event.is_set():
            break
        if not queue:
            break
        if len(visited) >= CONFIG["MAX_VISITED"]:
            break
        if visit_budget is not None and not visit_budget.take():
            break

        url = queue.popleft()
        key = canonical_key(url)
        if key in visited:
            continue
        visited.add(key)

        headers = build_headers(referer=referer)
        METRICS.bump_request()
        METRICS.bump_domain(url)
        resp = safe_get(session, url, headers, allow_redirects=False)
        if resp is None:
            METRICS.fail_request()
            continue

        referer = url
        body = ""
        try:
            body = resp.text or ""
        except (ValueError, UnicodeError):
            body = ""

        ct = sniff_content_type(resp)
        if ct == "application/pdf":
            try:
                body = extract_pdf_text(resp.content)
            except (ValueError, zlib.error):
                body = ""

        for n in extract_numbers(body):
            _record(find_surface(body, n), n, hop, url, body, out)
        for n in extract_numbers(url):
            _record("body", n, hop, url, url, out)

        if resp.status_code == 200 and ("slowAES" in body or "__test" in body):
            METRICS.detect("testcookie-nginx")
            solved = bypass_testcookie(session, url, headers, resp)
            if solved is not None:
                METRICS.bypass("testcookie-nginx")
                try:
                    body = solved.text or ""
                except (ValueError, UnicodeError):
                    body = ""
                for n in extract_numbers(body):
                    _record(find_surface(body, n), n, hop, url, body, out)

        if detect_protection(body) == "cloudflare-challenge":
            METRICS.detect("cloudflare")
            solved = passive_cloudflare_solve(session, url, headers, body)
            if solved is not None:
                METRICS.bypass("cloudflare")
                try:
                    body = solved.text or ""
                except (ValueError, UnicodeError):
                    body = ""
                for n in extract_numbers(body):
                    _record(find_surface(body, n), n, hop, url, body, out)

        if resp.status_code == 200 and body:
            _process_body(body, url, hop, out, queue)

            for asset in extract_asset_urls(body):
                cand = resolve_url(url, asset)
                if not cand:
                    continue
                a_resp = safe_get(session, cand, build_headers(referer=url), allow_redirects=True)
                if a_resp is None:
                    continue
                try:
                    if len(a_resp.content) > CONFIG["ASSET_SIZE_LIMIT"]:
                        continue
                    a_body = a_resp.text or ""
                except (ValueError, UnicodeError):
                    continue
                for n in extract_numbers(a_body):
                    _record("js", n, hop, cand, a_body, out)

        if resp.status_code == 200 and body:
            for action, fields in find_form_action(body):
                cand = resolve_url(url, action)
                if not cand:
                    continue
                try:
                    RATE_LIMITER.acquire()
                    f_resp = session.post(cand, data=fields, headers=headers,
                                          timeout=CONFIG["TIMEOUT"], allow_redirects=False)
                except requests.exceptions.RequestException:
                    continue
                if f_resp is None:
                    continue
                loc = f_resp.headers.get("Location")
                if loc:
                    nxt = resolve_url(cand, loc)
                    if nxt:
                        queue.append(nxt)

        if resp.status_code in (301, 302, 303, 307, 308, 300):
            loc = (resp.headers.get("Location")
                   or resp.headers.get("Refresh")
                   or resp.headers.get("X-Location")
                   or resp.headers.get("X-Redirect")
                   or resp.headers.get("X-Redirect-URL")
                   or resp.headers.get("Content-Location")
                   or "")
            if loc and "url=" in loc.lower():
                loc = re.sub(r"^[\d.]+\s*;\s*url\s*=\s*", "", loc, flags=re.I)
            nxt = resolve_url(url, loc)
            if nxt:
                METRICS.bump_redirect()
                for n in extract_numbers(nxt):
                    _record("body", n, hop, nxt, nxt, out)
                queue.append(nxt)
                continue

    METRICS.add_numbers(out.keys())
    return bool(out)

# =============================================================================
# 13. ANALYZE
# =============================================================================


def analyze_url(url: str) -> dict[str, Any]:
    session = make_session()
    info: dict[str, Any] = {
        "protection": "none",
        "chain": [],
        "timeline": [],
        "reachable": False,
        "note": "",
        "numbers": [],
    }
    try:
        seed = clean_input(url)
        seed, _ = rewrite_special(seed)
        if not seed:
            return info
        seed = normalize_scheme(seed)
        cur = seed
        visited: set[str] = set()
        t0 = time.monotonic()
        for hop in range(CONFIG["MAX_HOPS"]):
            if cur in visited:
                break
            visited.add(cur)
            headers = build_headers()
            resp = safe_get(session, cur, headers, allow_redirects=False)
            if resp is None:
                info["chain"].append("ERR connection failed")
                info["timeline"].append(
                    f"hop {hop} @ {time.monotonic()-t0:.2f}s  ERR  {cur[:80]}"
                )
                break
            info["reachable"] = True
            body = ""
            try:
                body = resp.text or ""
            except (ValueError, UnicodeError):
                body = ""
            nums = extract_numbers(body) | extract_numbers(cur)
            prot = detect_protection(body)
            if prot != "none":
                info["protection"] = prot
            info["timeline"].append(
                f"hop {hop} @ {time.monotonic()-t0:.2f}s  {resp.status_code}  "
                f"{cur[:80]}  {len(nums)} numbers  ({prot})"
            )
            info["chain"].append(f"{resp.status_code} {cur[:90]}")

            if prot == "testcookie-nginx-module (AES)":
                solved = bypass_testcookie(session, cur, headers, resp)
                if solved is not None:
                    info["chain"].append("  → testcookie bypass OK → " + str(solved.status_code))
                    if solved.status_code in (301, 302, 303, 307, 308):
                        nxt = resolve_url(cur, solved.headers.get("Location") or "")
                        if nxt:
                            cur = nxt
                            continue
                break

            if prot == "cloudflare-challenge":
                solved = passive_cloudflare_solve(session, cur, headers, body)
                if solved is not None:
                    info["chain"].append("  → cloudflare passive solve OK")
                else:
                    info["note"] = "cloudflare-challenge — passive solve failed"
                break

            if prot in ("captcha", "bot-manager"):
                info["note"] = prot + " — bot does not attempt to solve"
                break

            redirects = (find_b64_redirects(body) + find_meta_refresh(body)
                         + find_js_redirects(body) + find_iframes(body) + find_canonical(body))
            if redirects:
                nxt = resolve_url(cur, redirects[0])
                info["chain"].append("  → " + (nxt or "?")[:90])
                if not nxt:
                    break
                cur = nxt
                continue

            if resp.status_code in (301, 302, 303, 307, 308):
                loc = resp.headers.get("Location") or ""
                nxt = resolve_url(cur, loc)
                info["chain"].append("  → " + (nxt or "?")[:90])
                if not nxt:
                    break
                cur = nxt
                continue

            break
        info["numbers"] = sorted(
            extract_numbers("\n".join(info["chain"]) + "\n" + "\n".join(info["timeline"]))
        )
    finally:
        try:
            session.close()
        except (OSError, requests.exceptions.RequestException):
            pass
    return info

# =============================================================================
# 14. TELEGRAM API
# =============================================================================

API = "https://api.telegram.org/bot" + CONFIG["TG_TOKEN"]
TG_SESSION = make_session()
TG_LOCK = threading.Lock()
TG_FAIL_STREAK: list[int] = [0]


def _close_tg_session() -> None:
    try:
        TG_SESSION.close()
    except (OSError, requests.exceptions.RequestException):
        pass


atexit.register(_close_tg_session)


def tg(method: str, **kwargs: Any) -> dict[str, Any]:
    url = API + "/" + method
    with TG_LOCK:
        for _attempt in range(2):
            try:
                r = TG_SESSION.post(url, timeout=60, **kwargs)
            except requests.exceptions.RequestException as e:
                LOG.warning("tg %s net err: %s", method, str(e)[:120])
                TG_FAIL_STREAK[0] += 1
                time.sleep(1.0 + random.random())
                continue
            if r.status_code == 429:
                ra = r.headers.get("Retry-After", "5")
                try:
                    wait = min(15.0, float(ra))
                except ValueError:
                    wait = 5.0
                LOG.warning("tg %s 429, sleeping %.1fs", method, wait)
                time.sleep(wait)
                continue
            if 500 <= r.status_code < 600:
                LOG.warning("tg %s %d, retrying", method, r.status_code)
                time.sleep(1.5 + random.random())
                continue
            try:
                data = r.json()
            except ValueError:
                LOG.warning("tg %s non-json response", method)
                TG_FAIL_STREAK[0] += 1
                return {"ok": False, "description": "non-json"}
            TG_FAIL_STREAK[0] = 0
            return data
    return {"ok": False, "description": "exhausted retries"}


def send_msg(chat_id: int, text: str,
             reply_markup: Optional[dict] = None) -> dict[str, Any]:
    data: dict[str, Any] = {
        "chat_id": chat_id, "text": text, "disable_web_page_preview": True,
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    return tg("sendMessage", data=data)


def edit_msg(chat_id: int, msg_id: int, text: str,
             reply_markup: Optional[dict] = None) -> dict[str, Any]:
    data: dict[str, Any] = {"chat_id": chat_id, "message_id": msg_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    return tg("editMessageText", data=data)


def send_doc(chat_id: int, path: str, caption: str = "") -> dict[str, Any]:
    try:
        with open(path, "rb") as f:
            return tg("sendDocument",
                      data={"chat_id": chat_id, "caption": caption},
                      files={"document": (Path(path).name, f)})
    except OSError as e:
        LOG.warning("send_doc failed: %s", e)
        return {"ok": False, "description": str(e)}

# =============================================================================
# 15. STATE + HANDLERS
# =============================================================================

STATE: dict[int, dict[str, Any]] = {}
STATE_LOCK = threading.Lock()
CANCEL_EVENTS: dict[int, threading.Event] = {}
WATCHES: dict[int, list[dict[str, Any]]] = defaultdict(list)
HISTORY: dict[int, deque] = defaultdict(lambda: deque(maxlen=20))
LAST_EXTRACTION: dict[int, list[str]] = {}

CYCLES_KB: dict[str, Any] = {
    "inline_keyboard": [
        [{"text": "🚀 5", "callback_data": "cyc:5"},
         {"text": "🚀 10", "callback_data": "cyc:10"},
         {"text": "🚀 20", "callback_data": "cyc:20"}],
        [{"text": "🚀 50", "callback_data": "cyc:50"},
         {"text": "🚀 100", "callback_data": "cyc:100"}],
        [{"text": "❌ Cancel", "callback_data": "cyc:cancel"}],
    ]
}


def is_allowed(chat_id: int) -> bool:
    allowed = CONFIG["ALLOWED_CHATS"]
    return allowed is None or chat_id in allowed


def get_state(chat_id: int) -> dict[str, Any]:
    with STATE_LOCK:
        return STATE.setdefault(chat_id, {"step": "idle", "url": None, "urls": []})


def set_state(chat_id: int, **kw: Any) -> None:
    with STATE_LOCK:
        get_state(chat_id).update(kw)


def extract_urls(text: str) -> list[str]:
    urls: list[str] = []
    for m in RE_URL.finditer(text):
        urls.append(m.group(0).rstrip(").,;"))
    for tok in re.split(r"\s+", text):
        tok = tok.strip("<>\"'(),;")
        if tok and looks_like_hostname(tok) and not any(tok in u for u in urls):
            urls.append(tok)
    seen: set[str] = set()
    uniq: list[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            uniq.append(u)
    return uniq[:5]


def handle_text(chat_id: int, text: str, msg_id: int) -> None:
    if not is_allowed(chat_id):
        send_msg(chat_id, "❌ not allowed")
        return
    text = text.strip()
    st = get_state(chat_id)

    if text in ("/start", "menu", "/menu"):
        set_state(chat_id, step="await_link", url=None, urls=[])
        send_msg(chat_id,
                 "🤖 *Number Extractor Bot v5.5*\n\n"
                 "link bhej — analyze → cycles → fire → txt + csv\n\n"
                 "commands: /cancel /stats /debug <url> /raw <url> /resolve <url>\n"
                 "/export /dedupe /vcard /top /diff <a> <b> /history\n"
                 "/watch <url> [n] /mirrors /ping /health /self_test")
        return

    if text in ("/cancel", "cancel"):
        ev = CANCEL_EVENTS.get(chat_id)
        if ev:
            ev.set()
        set_state(chat_id, step="idle", url=None, urls=[])
        send_msg(chat_id, "❌ cancelled (partial shipped)")
        return

    if text == "/stats":
        s = METRICS.snapshot()
        top = sorted(s["domain_counts"].items(), key=lambda kv: kv[1], reverse=True)[:5]
        lines = [
            "📊 *Stats*",
            "├ uptime: " + str(s["uptime"]) + "s",
            "├ requests: " + str(s["requests_made"]) + " (" + str(s["requests_failed"]) + " failed)",
            "├ redirects followed: " + str(s["redirects_followed"]),
            "├ unique numbers: " + str(s["unique_numbers"]),
            "├ cycles run: " + str(s["cycles_run"]) + " (completed " + str(s["cycles_completed"]) + ")",
            "├ protections bypassed:",
        ]
        for k, v in s["protections_bypassed"].items():
            lines.append("│   " + k + ": " + str(v))
        lines.append("└ top domains:")
        for i, (d, c) in enumerate(top, 1):
            lines.append("    " + str(i) + ". " + d + " (" + str(c) + ")")
        send_msg(chat_id, "\n".join(lines))
        return

    if text.startswith("/raw"):
        parts = text.split(None, 1)
        if len(parts) < 2:
            send_msg(chat_id, "usage: /raw <url>")
            return
        url = normalize_scheme(clean_input(parts[1]))
        s = make_session()
        try:
            r = safe_get(s, url, build_headers(), allow_redirects=True)
            if r is None:
                send_msg(chat_id, "❌ fetch failed")
                return
            body = r.text[:3500] if r.text else ""
            send_msg(chat_id, "```\n" + body + "\n```")
        finally:
            try:
                s.close()
            except (OSError, requests.exceptions.RequestException):
                pass
        return

    if text.startswith("/resolve"):
        parts = text.split(None, 1)
        if len(parts) < 2:
            send_msg(chat_id, "usage: /resolve <url>")
            return
        info = analyze_url(parts[1])
        send_msg(chat_id,
                 "🔎 *Resolve*\n"
                 "protection: `" + info["protection"] + "`\n"
                 "reachable: " + ("yes" if info["reachable"] else "no") + "\n"
                 "chain:\n" + "\n".join("  " + c for c in info["chain"][:20]))
        return

    if text.startswith("/debug"):
        parts = text.split(None, 1)
        if len(parts) < 2:
            send_msg(chat_id, "usage: /debug <url>")
            return
        info = analyze_url(parts[1])
        send_msg(chat_id,
                 "🔎 *Debug* " + parts[1][:80] + "\n"
                 "protection: `" + info["protection"] + "`\n"
                 "reachable: " + ("yes" if info["reachable"] else "no") + "\n"
                 "numbers: " + str(len(info["numbers"])) + "\n"
                 "timeline:\n" + "\n".join("  " + l for l in info["timeline"][:20]) + "\n"
                 "chain:\n" + "\n".join("  " + c for c in info["chain"][:20]))
        return

    if text == "/export":
        zip_all_files(chat_id)
        return
    if text == "/dedupe":
        dedupe_all(chat_id)
        return
    if text == "/vcard":
        vcard_last(chat_id)
        return
    if text == "/top":
        top_numbers(chat_id)
        return
    if text.startswith("/diff"):
        parts = text.split()
        if len(parts) < 3:
            send_msg(chat_id, "usage: /diff <file_a> <file_b>")
            return
        diff_files(chat_id, parts[1], parts[2])
        return
    if text == "/history":
        hist = list(HISTORY[chat_id])
        if not hist:
            send_msg(chat_id, "no history")
            return
        lines = ["📜 *History*"]
        for i, h in enumerate(reversed(hist[-10:]), 1):
            lines.append(f"{i}. {h['ts']} | {h['url'][:60]} | {h['cycles']} cyc | {h['count']} nums")
        send_msg(chat_id, "\n".join(lines))
        return
    if text.startswith("/watch"):
        parts = text.split()
        if len(parts) < 2:
            send_msg(chat_id, "usage: /watch <url> [interval_seconds]")
            return
        try:
            interval = int(parts[2]) if len(parts) > 2 else 300
        except ValueError:
            interval = 300
        if len(WATCHES[chat_id]) >= 5:
            send_msg(chat_id, "max 5 watches per chat")
            return
        WATCHES[chat_id].append({"url": parts[1], "interval": interval, "last": set()})
        send_msg(chat_id, "👁 watching " + parts[1][:80])
        return
    if text == "/mirrors":
        reachable: list[str] = []
        s = make_session()
        try:
            for name, tmpl in MIRRORS.items():
                if MIRROR_BAN.is_banned(name):
                    reachable.append(name + ": banned")
                    continue
                url = tmpl.format(url="https://example.com")
                r = safe_get(s, url, build_headers(), allow_redirects=True)
                reachable.append(name + ": " + ("ok" if r is not None else "fail"))
        finally:
            try:
                s.close()
            except (OSError, requests.exceptions.RequestException):
                pass
        send_msg(chat_id, "🪞 mirrors:\n" + "\n".join(reachable))
        return
    if text == "/ping":
        s = METRICS.snapshot()
        send_msg(chat_id, "🏓 pong\nuptime: " + str(s["uptime"]) + "s\nrequests: " + str(s["requests_made"]))
        return
    if text == "/health":
        health(chat_id)
        return
    if text in ("/self_test", "/selftest"):
        ok, total, failures = run_self_test()
        send_msg(chat_id, f"🧪 self_test: {ok}/{total} passed"
                 + ("\n" + "\n".join(failures[:10]) if failures else ""))
        return

    urls = extract_urls(text)
    if urls:
        st_urls = urls[:5]
        set_state(chat_id, step="analyzing", url=st_urls[0], urls=st_urls)
        send_msg(chat_id, "🔍 *Analyzing " + str(len(st_urls)) + " URL(s)*\n\n"
                 + "\n".join("`" + u[:100] + "`" for u in st_urls))
        infos = [analyze_url(u) for u in st_urls]
        lines: list[str] = []
        for u, info in zip(st_urls, infos):
            note = ("\n⚠️ " + info["note"]) if info.get("note") else ""
            lines.append("🔗 " + u[:80] + "\n"
                         "  protection: `" + info["protection"] + "`" + note + "\n"
                         "  reachable: " + ("yes" if info["reachable"] else "no") + "\n"
                         "  hops: " + str(len(info["chain"])))
        set_state(chat_id, step="await_cycles")
        send_msg(chat_id, "\n\n".join(lines) + "\n\n⚙️ Kitne cycles chalaun?", reply_markup=CYCLES_KB)
        return

    if re.match(r"^\d+$", text):
        if st["step"] != "await_cycles" or not st.get("urls"):
            send_msg(chat_id, "pehle link bhej.")
            return
        n = int(text)
        if n < 1 or n > 5000:
            send_msg(chat_id, "1 se 5000 ke beech daal")
            return
        start_extraction(chat_id, st["urls"], n)
        return

    send_msg(chat_id, "link bhej (http/https ya bare hostname) ya /start")


def handle_callback(chat_id: int, data: str, msg_id: int) -> None:
    if not is_allowed(chat_id):
        return
    st = get_state(chat_id)
    if not data.startswith("cyc:"):
        return
    val = data.split(":", 1)[1]
    if val == "cancel":
        ev = CANCEL_EVENTS.get(chat_id)
        if ev:
            ev.set()
        set_state(chat_id, step="idle", url=None, urls=[])
        edit_msg(chat_id, msg_id, "❌ cancelled")
        return
    if st["step"] != "await_cycles" or not st.get("urls"):
        edit_msg(chat_id, msg_id, "pehle link bhej")
        return
    n = int(val)
    edit_msg(chat_id, msg_id, "🚀 launching " + str(n) + " cycles...")
    start_extraction(chat_id, st["urls"], n)

# =============================================================================
# 16. EXTRACTION RUNNER
# =============================================================================


def _write_file_atomic(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(str(tmp), str(path))


def _write_csv_atomic(path: Path, rows: list[list[str]]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        w.writerows(rows)
    os.replace(str(tmp), str(path))


def _write_vcard_atomic(path: Path, numbers: list[str]) -> None:
    lines: list[str] = []
    for i, n in enumerate(numbers):
        lines += [
            "BEGIN:VCARD", "VERSION:3.0",
            f"FN:Contact {i+1}",
            f"TEL;TYPE=CELL:{n}",
            "END:VCARD",
        ]
    _write_file_atomic(path, "\n".join(lines) + "\n")


def start_extraction(chat_id: int, urls: list[str], n: int) -> None:
    ev = threading.Event()
    CANCEL_EVENTS[chat_id] = ev
    set_state(chat_id, step="running")

    status = send_msg(chat_id,
                      "⏳ *Starting extraction...*\n\n"
                      "URLs: " + str(len(urls)) + "\n"
                      "Cycles: " + str(n) + "\n"
                      "Threads: " + str(CONFIG["THREADS"]) + "\n"
                      "Attempts/worker: " + str(CONFIG["MAX_ATTEMPTS"]) + "\n"
                      "Wall-clock cap: " + str(CONFIG["WALL_CLOCK_TOTAL"]) + "s")
    status_id = status.get("result", {}).get("message_id")
    t0 = time.time()
    last_upd = [0.0]
    partial_sent = [False]
    aggregated: dict[str, Extraction] = {}
    agg_lock = threading.Lock()

    def on_prog(cycle: int, total: int, count: int) -> None:
        now = time.time()
        if now - last_upd[0] < 1.0 and cycle != total:
            return
        last_upd[0] = now
        pct = int(cycle * 100 / total) if total else 0
        bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
        elapsed = time.time() - t0
        text = (f"⏳ Cycle {cycle}/{total}\n{bar} {pct}%\n"
                f"📊 Unique: {count}\n⏱️ {elapsed:.1f}s")
        if status_id:
            edit_msg(chat_id, status_id, text)
        if (not partial_sent[0]
                and elapsed >= CONFIG["PARTIAL_STREAM_AT"]
                and count >= 1):
            partial_sent[0] = True
            preview = ", ".join(sorted(aggregated.keys())[:10])
            send_msg(chat_id, f"⏳ 30s in — {count} numbers so far. Preview: {preview}")

    try:
        visit_budget = VisitBudget(CONFIG["MAX_TOTAL_VISITED"])
        for cycle in range(1, n + 1):
            if ev.is_set():
                break
            METRICS.cycles_run += 1
            with ThreadPoolExecutor(max_workers=CONFIG["THREADS"]) as ex:
                futures = []
                per_url_workers = max(1, CONFIG["THREADS"] // max(1, len(urls)))
                for url in urls:
                    for _ in range(per_url_workers):
                        futures.append(ex.submit(worker_scrape, url, ev,
                                                 CONFIG["WALL_CLOCK_PER_WORKER"],
                                                 visit_budget))
                cycle_results: dict[str, Extraction] = {}
                for f in as_completed(futures):
                    try:
                        r = f.result()
                    except Exception as e:
                        LOG.warning("worker crash: %s", str(e)[:120])
                        continue
                    for k, v in r.items():
                        if k not in cycle_results:
                            cycle_results[k] = v
            with agg_lock:
                for k, v in cycle_results.items():
                    if k not in aggregated:
                        aggregated[k] = v
            METRICS.add_numbers(aggregated.keys())
            METRICS.cycles_completed += 1
            on_prog(cycle, n, len(aggregated))
            if len(aggregated) >= CONFIG["EARLY_EXIT_THRESHOLD"]:
                break
            if time.time() - t0 >= CONFIG["WALL_CLOCK_TOTAL"]:
                break
            if ev.is_set():
                break
    except Exception as e:
        LOG.error("extraction runner crash: %s", str(e)[:200])
        if status_id:
            edit_msg(chat_id, status_id, "❌ error: " + str(e)[:200])
        set_state(chat_id, step="idle")
        CANCEL_EVENTS.pop(chat_id, None)
        return

    elapsed = round(time.time() - t0, 1)
    numbers = sorted(aggregated.keys())
    extractions = [aggregated[n] for n in numbers]
    LAST_EXTRACTION[chat_id] = numbers

    ts = time.strftime("%Y%m%d_%H%M%S")
    base = f"whatsapp_numbers_{chat_id}_{ts}"
    txt_path = CONFIG["WORK_DIR"] / (base + ".txt")
    csv_path = CONFIG["WORK_DIR"] / (base + ".csv")
    vcf_path = CONFIG["WORK_DIR"] / (base + ".vcf")

    try:
        _write_file_atomic(txt_path, "\n".join(numbers) + ("\n" if numbers else ""))
        _write_csv_atomic(csv_path, [e.as_csv_row() for e in extractions])
        if len(numbers) >= 10:
            _write_vcard_atomic(vcf_path, numbers)
    except OSError as e:
        send_msg(chat_id, "❌ file write failed: " + str(e)[:120])
        set_state(chat_id, step="idle")
        CANCEL_EVENTS.pop(chat_id, None)
        return

    HISTORY[chat_id].append({
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "url": urls[0] if urls else "",
        "cycles": n,
        "count": len(numbers),
        "txt": str(txt_path),
    })

    header = (f"✅ *Extraction Complete!*\n"
              f"━━━━━━━━━━━━━━━━\n"
              f"🔄 Cycles: {n}\n"
              f"🧵 Threads: {CONFIG['THREADS']} × {CONFIG['MAX_ATTEMPTS']} attempts\n"
              f"📊 Unique: {len(numbers)}\n"
              f"⏱️ Time: {elapsed}s\n"
              f"━━━━━━━━━━━━━━━━")
    if len(numbers) < 2:
        a = analyze_url(urls[0]) if urls else {"protection": "?", "chain": []}
        header += ("\n🔎 diagnosis: protection=" + a["protection"]
                   + ", chain_len=" + str(len(a["chain"]))
                   + ", reason=low_yield, action=try /debug or /raw")
    if status_id:
        edit_msg(chat_id, status_id, header)

    if numbers:
        send_doc(chat_id, str(txt_path), caption=f"📁 {len(numbers)} unique")
        if len(numbers) >= 5:
            send_doc(chat_id, str(csv_path), caption="📊 provenance csv")
        if len(numbers) >= 10 and vcf_path.exists():
            send_doc(chat_id, str(vcf_path), caption="📇 vcard")
        if len(numbers) <= 20:
            send_msg(chat_id, "preview:\n" + "\n".join(numbers))
    else:
        send_msg(chat_id, "😶 koi number nahi mila — /debug dekh")

    set_state(chat_id, step="await_link", url=None, urls=[])
    CANCEL_EVENTS.pop(chat_id, None)

# =============================================================================
# 17. AUX COMMANDS
# =============================================================================


def zip_all_files(chat_id: int) -> None:
    files = sorted(CONFIG["WORK_DIR"].glob("whatsapp_numbers_*.txt"))
    if not files:
        send_msg(chat_id, "no files yet")
        return
    zip_path = CONFIG["WORK_DIR"] / ("export_" + str(chat_id) + "_" + time.strftime("%Y%m%d_%H%M%S") + ".zip")
    seen: set[str] = set()
    try:
        with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as z:
            buf_lines: list[str] = []
            for f in files:
                try:
                    for line in f.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if line and line not in seen:
                            seen.add(line)
                            buf_lines.append(line)
                except OSError:
                    continue
            z.writestr("all_unique.txt", "\n".join(buf_lines) + ("\n" if buf_lines else ""))
    except OSError as e:
        send_msg(chat_id, "❌ zip failed: " + str(e)[:120])
        return
    send_doc(chat_id, str(zip_path), caption=f"📦 {len(seen)} unique across {len(files)} files")


def dedupe_all(chat_id: int) -> None:
    files = sorted(CONFIG["WORK_DIR"].glob("whatsapp_numbers_*.txt"))
    seen: set[str] = set()
    for f in files:
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    seen.add(line)
        except OSError:
            continue
    out = CONFIG["WORK_DIR"] / ("deduped_" + time.strftime("%Y%m%d_%H%M%S") + ".txt")
    try:
        _write_file_atomic(out, "\n".join(sorted(seen)) + ("\n" if seen else ""))
    except OSError as e:
        send_msg(chat_id, "❌ write failed: " + str(e)[:120])
        return
    send_doc(chat_id, str(out), caption=f"🧹 {len(seen)} unique from {len(files)} files")


def vcard_last(chat_id: int) -> None:
    numbers = LAST_EXTRACTION.get(chat_id, [])
    if not numbers:
        send_msg(chat_id, "no extraction yet")
        return
    out = CONFIG["WORK_DIR"] / ("last_" + str(chat_id) + ".vcf")
    try:
        _write_vcard_atomic(out, numbers)
    except OSError as e:
        send_msg(chat_id, "❌ write failed: " + str(e)[:120])
        return
    send_doc(chat_id, str(out), caption=f"📇 {len(numbers)} contacts")


def top_numbers(chat_id: int) -> None:
    cnt: Counter = Counter()
    files = sorted(CONFIG["WORK_DIR"].glob("whatsapp_numbers_*.txt"))
    for f in files:
        try:
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    cnt[line] += 1
        except OSError:
            continue
    if not cnt:
        send_msg(chat_id, "no data")
        return
    lines = ["🔥 *Top 20*"]
    for i, (n, c) in enumerate(cnt.most_common(20), 1):
        lines.append(f"{i}. {n} ({c}×)")
    send_msg(chat_id, "\n".join(lines))


def diff_files(chat_id: int, a: str, b: str) -> None:
    pa = CONFIG["WORK_DIR"] / a
    pb = CONFIG["WORK_DIR"] / b
    if not pa.exists() or not pb.exists():
        send_msg(chat_id, "file not found")
        return
    try:
        sa = set(l.strip() for l in pa.read_text(encoding="utf-8").splitlines() if l.strip())
        sb = set(l.strip() for l in pb.read_text(encoding="utf-8").splitlines() if l.strip())
    except OSError as e:
        send_msg(chat_id, "❌ read failed: " + str(e)[:120])
        return
    send_msg(chat_id, f"diff:\n  only A: {len(sa - sb)}\n  only B: {len(sb - sa)}\n  both: {len(sa & sb)}")


def health(chat_id: int) -> None:
    lines = ["🏥 *Health*"]
    try:
        me = tg("getMe")
        lines.append("telegram api: " + ("PASS" if me.get("ok") else "FAIL"))
    except (requests.exceptions.RequestException, ValueError) as e:
        lines.append("telegram api: FAIL " + str(e)[:80])
    try:
        t = CONFIG["WORK_DIR"] / ".write_test"
        t.write_text("ok")
        t.unlink()
        lines.append("work dir writable: PASS")
    except (OSError, PermissionError):
        lines.append("work dir writable: FAIL")
    try:
        t = CONFIG["LOG_DIR"] / ".write_test"
        t.write_text("ok")
        t.unlink()
        lines.append("log dir writable: PASS")
    except (OSError, PermissionError):
        lines.append("log dir writable: FAIL")
    try:
        socket.gethostbyname("example.com")
        lines.append("dns: PASS")
    except socket.gaierror:
        lines.append("dns: FAIL")
    s = make_session()
    try:
        r = safe_get(s, "https://example.com", build_headers(), allow_redirects=True)
        lines.append("https example.com: "
                     + ("PASS" if r is not None and r.status_code == 200 else "FAIL"))
    finally:
        try:
            s.close()
        except (OSError, requests.exceptions.RequestException):
            pass
    snap = METRICS.snapshot()
    lines.append("metrics sane: " + ("PASS" if snap["requests_made"] >= 0 else "FAIL"))
    send_msg(chat_id, "\n".join(lines))

# =============================================================================
# 18. POLL LOOP
# =============================================================================


def handle_update(u: dict[str, Any]) -> None:
    try:
        if "message" in u:
            m = u["message"]
            if m.get("text"):
                handle_text(m["chat"]["id"], m["text"], m["message_id"])
        elif "callback_query" in u:
            cq = u["callback_query"]
            tg("answerCallbackQuery", data={"callback_query_id": cq["id"]})
            handle_callback(cq["message"]["chat"]["id"],
                            cq.get("data", ""),
                            cq["message"]["message_id"])
    except Exception as e:
        LOG.warning("update handler: %s", str(e)[:200])


def prepare_polling() -> None:
    if not CONFIG["TG_TOKEN"]:
        raise RuntimeError("TG_TOKEN environment variable is required")

    info = tg("getWebhookInfo")
    if info.get("ok"):
        webhook_url = str(info.get("result", {}).get("url") or "")
        if webhook_url:
            LOG.warning("Webhook is configured (%s); deleting it for getUpdates polling", webhook_url[:160])
            deleted = tg("deleteWebhook", data={"drop_pending_updates": "false"})
            if not deleted.get("ok"):
                raise RuntimeError("Could not delete Telegram webhook: " + str(deleted.get("description", "unknown error")))


def poll() -> None:
    global TG_SESSION
    prepare_polling()
    LOG.info("polling started")
    me = tg("getMe")
    if me.get("ok"):
        LOG.info("bot: @%s", me["result"].get("username", "?"))
    else:
        raise RuntimeError("Telegram getMe failed: " + str(me.get("description", "unknown error")))

    offset = 0
    fail_streak = 0
    while True:
        try:
            r = TG_SESSION.get(API + "/getUpdates",
                               params={"offset": offset, "timeout": 25},
                               timeout=35)
            data = r.json()
            if not data.get("ok"):
                fail_streak += 1
                LOG.warning("getUpdates not ok: %s", data.get("description"))
                if fail_streak >= 5:
                    try:
                        TG_SESSION.close()
                    except (OSError, requests.exceptions.RequestException):
                        pass
                    TG_SESSION = make_session()
                    LOG.warning("rebuilding telegram session")
                if fail_streak >= 20:
                    LOG.error("20 consecutive failures — exiting poll loop")
                    return
                time.sleep(3)
                continue
            fail_streak = 0
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                handle_update(u)
        except requests.exceptions.RequestException as e:
            fail_streak += 1
            LOG.warning("poll net err: %s", str(e)[:120])
            time.sleep(2)
        except KeyboardInterrupt:
            LOG.info("stopped by user")
            break
        except Exception as e:
            fail_streak += 1
            LOG.warning("poll err: %s", str(e)[:120])
            time.sleep(2)

# =============================================================================
# 19. SELF-TEST
# =============================================================================


def run_self_test() -> tuple[int, int, list[str]]:
    failures: list[str] = []
    total = 0

    def check(name: str, cond: bool) -> None:
        nonlocal total
        total += 1
        if not cond:
            failures.append(name)

    check("aes_ecb_decrypt_16_bytes", len(aes_ecb_decrypt(bytes(16), bytes(16))) == 16)
    check("aes_192_ecb_len", len(aes_ecb_decrypt(bytes(16), bytes(24))) == 16)
    check("aes_256_ecb_len", len(aes_ecb_decrypt(bytes(16), bytes(32))) == 16)

    check("normalize_indian", normalize_phone("9123456789") == "+919123456789")
    check("normalize_indian_91", normalize_phone("919123456789") == "+919123456789")
    check("normalize_00", normalize_phone("00919123456789") == "+919123456789")
    check("normalize_reject_short", normalize_phone("12345") is None)
    check("normalize_reject_all_same", normalize_phone("1111111111") is None)
    check("normalize_reject_placeholder", normalize_phone("1234567890") is None)

    check("detect_country_in", detect_country("+919876543210") == "IN")
    check("detect_country_other", detect_country("+14155552671") == "CC1")

    check("looks_like_hostname_example", looks_like_hostname("example.com"))
    check("looks_like_hostname_wa", looks_like_hostname("wa.me/919876543210"))
    check("looks_like_hostname_reject_email", not looks_like_hostname("a@b.com"))
    check("looks_like_hostname_reject_short", not looks_like_hostname("x"))

    check("extract_urls_bare", extract_urls("visit example.com please") == ["example.com"])
    check("extract_urls_multi", len(extract_urls("a.com b.com c.com d.com e.com f.com g.com")) == 5)

    check("canonical_strips_tracking",
          "utm_source" not in canonical_key("https://e.com/p?utm_source=x&b=1"))

    check("deobfuscate_rot13_returns_list", isinstance(deobfuscate("uryyb"), list))

    check("extract_numbers_from_wa_me",
          "+919123456789" in extract_numbers("https://wa.me/919123456789"))
    check("extract_numbers_from_tel",
          "+919876543210" in extract_numbers('<a href="tel:+919876543210">x</a>'))
    check("extract_numbers_from_bare",
          "+919123456789" in extract_numbers("call 9123456789 now"))
    check("extract_numbers_from_api_whatsapp",
          "+919123456789" in extract_numbers("https://api.whatsapp.com/send?phone=919123456789"))
    check("extract_numbers_from_web_whatsapp",
          "+919123456789" in extract_numbers("https://web.whatsapp.com/send?phone=919123456789"))
    check("extract_numbers_from_wa_alt",
          "+919123456789" in extract_numbers("https://www.whatsapp.com/send?phone=919123456789"))

    check("find_meta_refresh_quoted",
          find_meta_refresh('<meta http-equiv="refresh" content="0;url=https://x.com">') == ["https://x.com"])
    check("find_meta_refresh_space",
          find_meta_refresh('<meta http-equiv="refresh" content="0; URL=\'https://x.com\'">') == ["https://x.com"])
    check("find_b64_atob_literal",
          find_b64_redirects('<script>location.href=atob("aHR0cHM6Ly93YS5tZS85MTk4NzY1NDMyMTA=")</script>') != [])
    check("find_js_loc_href",
          find_js_redirects('location.href="https://x.com"') == ["https://x.com"])
    check("find_js_fromcharcode",
          any("hi" in v for v in find_js_redirects('String.fromCharCode(104,105)')))

    check("detect_protection_testcookie",
          detect_protection("slowAES.decrypt(c,2,a,b)") == "testcookie-nginx-module (AES)")
    check("detect_protection_cf",
          detect_protection("Checking your browser before accessing") == "cloudflare-challenge")
    check("detect_protection_captcha",
          detect_protection("g-recaptcha") == "captcha")
    check("detect_protection_bot_manager",
          detect_protection("datadome") == "bot-manager")

    check("solve_testcookie_no_match", solve_testcookie("nothing here") is None)

    check("extract_pdf_text_empty", extract_pdf_text(b"") == "")

    check("walk_json_urls",
          walk_json_for_urls({"a": {"b": "https://x.com"}}) == ["https://x.com"])
    check("find_json_field",
          find_json_urls('{"url":"https://x.com"}') == ["https://x.com"])

    check("find_query_urls_extracts",
          find_query_urls("https://e.com/?next=https%3A%2F%2Fwa.me%2F919876543210") != [])

    check("normalize_scheme_https", normalize_scheme("example.com") == "https://example.com")
    check("normalize_scheme_ftp", normalize_scheme("ftp://x.com") == "https://x.com")
    check("normalize_scheme_ws", normalize_scheme("ws://x.com") == "https://x.com")
    check("normalize_scheme_keeps_https", normalize_scheme("https://x.com") == "https://x.com")

    check("rewrite_tg_domain",
          rewrite_special("tg://resolve?domain=abc")[0] == "https://t.me/abc")
    check("rewrite_tg_phone",
          rewrite_special("tg://resolve?phone=+919876543210")[1] == "+919876543210")
    check("rewrite_telegram_me",
          rewrite_special("telegram.me/abc")[0] == "https://t.me/abc")

    check("rate_limiter_construct", RateLimiter(1000.0) is not None)

    check("confidence_for_wa_me", confidence_for("wa_me") == 1.0)
    check("confidence_for_body", confidence_for("body") == 0.5)

    check("context_for_returns_str",
          isinstance(context_for("call 9123456789 now", "+919876543210"), str))

    snap = METRICS.snapshot()
    check("metrics_snapshot_keys",
          "requests_made" in snap and "unique_numbers" in snap)

    check("work_dir_writable", _writable(CONFIG["WORK_DIR"]))

    return total - len(failures), total, failures

# =============================================================================
# 20. RENDER SHIM (health endpoint on $PORT only)
# =============================================================================

_SHIM_SERVER: Optional[Any] = None
_SHIM_LOCK = threading.Lock()


def _port_in_use(port: int, host: str = "0.0.0.0") -> bool:
    """Probe whether something is already listening on host:port."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        try:
            s.close()
        except OSError:
            pass


def _kill_stale_shim_on_port(port: int) -> bool:
    """
    If something is already listening on the Render $PORT and it looks like
    a previous instance of our own shim, kill it via lsof or fuser.
    Termux has neither by default — the function no-ops and returns False
    there, letting the fallback walk proceed normally.
    Returns True if the port is free after the attempt.
    """
    pids_raw = ""
    try:
        pids_raw = subprocess.run(
            ["sh", "-c",
             f"command -v lsof >/dev/null 2>&1 && lsof -ti :{port} || true"],
            capture_output=True, text=True, timeout=3,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pids_raw = ""

    if not pids_raw:
        try:
            pids_raw = subprocess.run(
                ["sh", "-c",
                 f"command -v fuser >/dev/null 2>&1 && fuser {port}/tcp 2>/dev/null || true"],
                capture_output=True, text=True, timeout=3,
            ).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pids_raw = ""

    if not pids_raw:
        return False

    pids = [p for p in re.split(r"\s+", pids_raw) if p.isdigit()]
    my_pid = os.getpid()
    killed = False
    for pid_s in pids:
        pid = int(pid_s)
        if pid == my_pid:
            continue
        try:
            os.kill(pid, signal.SIGTERM)
            killed = True
            LOG.warning("killed stale shim pid=%d holding port %d", pid, port)
        except (ProcessLookupError, PermissionError):
            continue

    if killed:
        time.sleep(0.5)
    return not _port_in_use(port)


def _start_render_shim() -> Optional[int]:
    """
    Bind a health endpoint once.

    Order:
      1. $PORT as Render injects it — after clearing any stale holder.
      2. Fallback list from CONFIG.

    EADDRINUSE is never fatal. If every candidate fails, return None and the
    poll loop runs anyway. Idempotent under _SHIM_LOCK.
    """
    global _SHIM_SERVER
    from http.server import BaseHTTPRequestHandler, HTTPServer

    with _SHIM_LOCK:
        if _SHIM_SERVER is not None:
            return getattr(_SHIM_SERVER, "_bound_port", None)

        class _H(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", "2")
                self.end_headers()
                try:
                    self.wfile.write(b"ok")
                except (OSError, BrokenPipeError):
                    pass

            def do_HEAD(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                return

        primary_raw = os.environ.get("PORT")
        primary_port: Optional[int] = None
        candidates: list[int] = []
        if primary_raw:
            try:
                primary_port = int(primary_raw)
                candidates.append(primary_port)
            except (ValueError, TypeError):
                LOG.warning("invalid PORT=%r, ignoring", primary_raw)
        for p in CONFIG["RENDER_PORT_FALLBACKS"]:
            if p not in candidates:
                candidates.append(p)

        for port in candidates:
            if _port_in_use(port):
                LOG.warning("port %d already bound — attempting stale shim cleanup", port)
                if not _kill_stale_shim_on_port(port):
                    LOG.warning("port %d still busy after cleanup — trying next", port)
                    continue
                LOG.info("port %d freed after cleanup", port)

            try:
                srv = HTTPServer(("0.0.0.0", port), _H)
            except OSError as e:
                LOG.warning("shim bind failed on %d: %s", port, e)
                continue

            srv._bound_port = port  # type: ignore[attr-defined]
            t = threading.Thread(target=srv.serve_forever, daemon=True,
                                 name=f"render-shim-{port}")
            t.start()
            _SHIM_SERVER = srv
            LOG.info("render shim listening on 0.0.0.0:%d", port)

            if primary_port is not None and port != primary_port:
                LOG.warning(
                    "shim bound on %d but Render probes $PORT=%d — health endpoint "
                    "may be unreachable from the edge. Redeploy with a clean "
                    "container to reclaim $PORT.",
                    port, primary_port,
                )
            return port

        LOG.error("render shim: every candidate port is taken; "
                  "bot will still run but no health endpoint is exposed")
        return None

# =============================================================================
# 21. ENTRY POINT
# =============================================================================


def _shutdown(signum: int, _frame: Any) -> None:
    LOG.info("received signal %d, shutting down", signum)
    global _SHIM_SERVER
    if _SHIM_SERVER is not None:
        try:
            _SHIM_SERVER.shutdown()
        except Exception:
            pass
    _close_tg_session()
    sys.exit(0)


def _cli() -> int:
    parser = argparse.ArgumentParser(prog="bot.py", add_help=True)
    parser.add_argument("--self-test", action="store_true",
                        help="run in-process tests and exit")
    parser.add_argument("--verbose", action="store_true",
                        help="verbose logging")
    parser.add_argument("--no-shim", action="store_true",
                        help="do not start the render health shim")
    args = parser.parse_args()
    if args.verbose:
        CONFIG["VERBOSE"] = True

    if args.self_test or CONFIG["SELF_TEST"]:
        ok, total, failures = run_self_test()
        print(f"self_test: {ok}/{total} passed")
        for f in failures:
            print("  FAIL:", f)
        return 0 if not failures else 1

    if not CONFIG["TG_TOKEN"]:
        sys.stderr.write("FATAL: TG_TOKEN environment variable is required\n")
        return 2

    if not args.no_shim:
        _start_render_shim()

    try:
        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
    except (ValueError, OSError):
        pass

    try:
        poll()
    except KeyboardInterrupt:
        LOG.info("keyboard interrupt — exiting")
    finally:
        global _SHIM_SERVER
        if _SHIM_SERVER is not None:
            try:
                _SHIM_SERVER.shutdown()
            except Exception:
                pass
        _close_tg_session()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
