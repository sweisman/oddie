# Build Script Design

**Date:** 2026-05-07
**Scope:** REGRESSIONS cleanups 3 and 4 — build system and zlib management

## Problem

There is no build system. Compiling requires typing a long `i686-w64-mingw32-gcc` command
by hand. Zlib source files must be manually downloaded and copied into the repo before
building, and nothing enforces which version is used.

## Solution

A single `build.sh` shell script handles everything: fetching (or updating) zlib and
running the compile. No Makefile; the project has one source file and one build target,
so a shell script is simpler and more readable.

## build.sh Behavior

### Step 1 — Determine local zlib version

Look for `zlib.h` in the repo directory. If present, extract the version string from the
`ZLIB_VERSION` define (e.g. `"1.3.1"`). If absent, local version is "none".

### Step 2 — Check remote version (network optional)

Attempt `curl` with a short timeout (5 s) to `https://zlib.net/`. Parse the current
release version from the page (the page reliably announces it as plain text near the top,
e.g. `zlib 1.3.1`). If curl fails or times out, skip the version check and proceed with
whatever is local (or abort if nothing is local).

### Step 3 — Download and extract if needed

If remote version is newer than local (or local is absent): download
`https://zlib.net/zlib-X.Y.Z.tar.gz`, extract it, then copy only the required files into
the repo root:

```
compress.c  deflate.c  crc32.c  adler32.c  trees.c  zutil.c  zlib.h  zconf.h
```

The downloaded tarball and extracted directory are removed after copying.

### Step 4 — Detect cross-compiler

Try each candidate in order using `command -v`, take the first found:

1. `i686-w64-mingw32-gcc` — Arch, Ubuntu/Debian (mingw-w64)
2. `i686-pc-mingw32-gcc` — older MinGW (non-w64)
3. `mingw32-gcc` — Fedora/RHEL

If none is found, exit 1 with a message listing all names tried.

### Step 5 — Compile

Run the cross-compile command using the detected compiler:

```sh
$CC -Wall -Wextra -pedantic -std=gnu99 -Werror -Os -s -static \
  -I . oddie.c md5.c \
  compress.c deflate.c crc32.c adler32.c trees.c zutil.c \
  -o oddie.exe -lodbc32 -Wl,-verbose,--subsystem,console
```

Note: `-I /opt/cmf/src/oddie` in the original command is replaced with `-I .` since
zlib headers are now in the repo directory.

### Error handling

- Missing cross-compiler: print a clear message listing all tried names, exit 1
- curl failure with no local zlib: print a message and exit 1
- curl failure with local zlib: print a warning, continue with local copy
- Compile failure: gcc exit code propagates; no special handling needed

## .gitignore

New file. Entries for the zlib source files so they are never committed:

```
compress.c
deflate.c
crc32.c
adler32.c
trees.c
zutil.c
zlib.h
zconf.h
zlib-*/
zlib*.tar.gz
```

## README update

Replace the manual zlib download instructions and the raw gcc command with:

```
./build.sh
```

A brief note explains that the script fetches zlib automatically if a newer version is
available (requires `curl`, `tar`, and a MinGW cross-compiler —
`i686-w64-mingw32-gcc`, `i686-pc-mingw32-gcc`, or `mingw32-gcc`).

## Files changed

| File | Change |
|------|--------|
| `build.sh` | New file |
| `.gitignore` | New file |
| `README.md` | Replace manual build instructions |
