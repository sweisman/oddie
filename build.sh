#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ZLIB_FILES="compress.c deflate.c crc32.c adler32.c trees.c zutil.c zlib.h zconf.h deflate.h zutil.h trees.h inffixed.h inffast.h inftrees.h inflate.h crc32.h gzguts.h"

detect_compiler() {
    for cc in i686-w64-mingw32-gcc i686-pc-mingw32-gcc mingw32-gcc; do
        if command -v "$cc" >/dev/null 2>&1; then
            echo "$cc"
            return 0
        fi
    done
    echo "Error: no MinGW cross-compiler found. Tried:" >&2
    printf '  %s\n' i686-w64-mingw32-gcc i686-pc-mingw32-gcc mingw32-gcc >&2
    return 1
}

local_zlib_version() {
    if [ -f zlib.h ]; then
        for f in $ZLIB_FILES; do
            if [ ! -f "$f" ]; then
                echo "Local zlib incomplete (missing $f); will re-fetch." >&2
                return 0
            fi
        done
        grep '#define ZLIB_VERSION' zlib.h | grep -oE '"[^"]+"' | tr -d '"'
    fi
}

remote_zlib_version() {
    # Scrapes "zlib X.Y.Z" from zlib.net homepage; first match wins.
    curl -fsSL --connect-timeout 5 --max-time 10 https://zlib.net/ 2>/dev/null \
        | grep -oE 'zlib [0-9]+\.[0-9]+\.[0-9]+' \
        | head -1 \
        | awk '{print $2}' \
        || true
}

version_lt() {
    [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ] && [ "$1" != "$2" ]
}

fetch_zlib() {
    local ver="$1"
    local tarball="zlib-${ver}.tar.gz"
    echo "Downloading zlib ${ver}..."
    curl -fL --progress-bar "https://zlib.net/${tarball}" -o "$tarball"
    tar xf "$tarball"
    [ -d "zlib-${ver}" ] || { echo "Error: expected zlib-${ver}/ after extraction." >&2; exit 1; }
    for f in $ZLIB_FILES; do
        cp "zlib-${ver}/${f}" .
    done
    rm -rf "zlib-${ver}" "$tarball"
    echo "zlib ${ver} ready."
}

CC=$(detect_compiler) || exit 1
echo "Compiler: $CC"

LOCAL_VER=$(local_zlib_version)
echo "Checking for zlib updates..."
REMOTE_VER=$(remote_zlib_version)

if [ -n "$REMOTE_VER" ]; then
    if [ -z "$LOCAL_VER" ]; then
        echo "No local zlib found. Downloading ${REMOTE_VER}..."
        fetch_zlib "$REMOTE_VER"
    elif version_lt "$LOCAL_VER" "$REMOTE_VER"; then
        echo "Newer zlib available: ${REMOTE_VER} (have ${LOCAL_VER}). Updating..."
        fetch_zlib "$REMOTE_VER"
    else
        echo "zlib ${LOCAL_VER} is current."
    fi
else
    if [ -z "$LOCAL_VER" ]; then
        echo "Error: network unavailable and no local zlib found." >&2
        exit 1
    else
        echo "Warning: could not reach zlib.net. Using local zlib ${LOCAL_VER}."
    fi
fi

echo "Building oddie.exe..."
"$CC" -Wall -Wextra -pedantic -std=gnu99 -Werror -Os -s -static \
    -I . oddie.c md5.c \
    compress.c deflate.c crc32.c adler32.c trees.c zutil.c \
    -o oddie.exe -lodbc32 -Wl,-verbose,--subsystem,console

echo "Done: oddie.exe"
