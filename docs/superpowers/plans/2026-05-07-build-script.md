# Build Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the manual gcc command and manual zlib setup with a single `build.sh` that auto-fetches zlib if a newer version is available and cross-compiles `oddie.exe`.

**Architecture:** One shell script handles compiler detection, optional zlib update check/download, and compilation. A `.gitignore` keeps zlib source files out of the repo. README is updated to point to the script.

**Tech Stack:** bash, curl, tar, MinGW cross-compiler (`i686-w64-mingw32-gcc` or variant)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `build.sh` | Create | Compiler detection, zlib fetch, compile |
| `.gitignore` | Create | Keep zlib sources and build output untracked |
| `README.md` | Modify | Replace manual instructions with `./build.sh` |
| `REGRESSIONS` | Modify | Mark cleanups 3 and 4 as fixed |

---

### Task 1: Create `.gitignore`

**Files:**
- Create: `.gitignore`

- [ ] **Step 1: Write `.gitignore`**

```
# zlib source files (fetched by build.sh)
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

# build output
oddie.exe
```

- [ ] **Step 2: Verify it looks right**

```bash
cat .gitignore
```

Expected: the file contents above, no typos.

---

### Task 2: Create `build.sh`

**Files:**
- Create: `build.sh`

- [ ] **Step 1: Write `build.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

ZLIB_FILES="compress.c deflate.c crc32.c adler32.c trees.c zutil.c zlib.h zconf.h"

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
        grep '#define ZLIB_VERSION' zlib.h | grep -oE '"[^"]+"' | tr -d '"'
    fi
}

remote_zlib_version() {
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
```

- [ ] **Step 2: Make executable**

```bash
chmod +x build.sh
```

- [ ] **Step 3: Verify compiler detection error path**

```bash
PATH=/usr/bin bash build.sh 2>&1 | head -5
```

Expected output contains:
```
Error: no MinGW cross-compiler found. Tried:
  i686-w64-mingw32-gcc
  i686-pc-mingw32-gcc
  mingw32-gcc
```

- [ ] **Step 4: Verify version comparison logic**

```bash
bash -c '
version_lt() {
    [ "$(printf "%s\n%s\n" "$1" "$2" | sort -V | head -1)" = "$1" ] && [ "$1" != "$2" ]
}
version_lt 1.2.11 1.3.1 && echo "PASS: 1.2.11 < 1.3.1" || echo "FAIL"
version_lt 1.3.1 1.3.1 && echo "FAIL: equal should not be lt" || echo "PASS: 1.3.1 not < 1.3.1"
version_lt 1.3.1 1.2.11 && echo "FAIL: newer should not be lt" || echo "PASS: 1.3.1 not < 1.2.11"
'
```

Expected:
```
PASS: 1.2.11 < 1.3.1
PASS: 1.3.1 not < 1.3.1
PASS: 1.3.1 not < 1.2.11
```

- [ ] **Step 5: Full build test**

```bash
./build.sh
```

Expected: downloads zlib (first run), then compiler output, ending with `Done: oddie.exe`. Confirm `oddie.exe` exists:

```bash
ls -lh oddie.exe
file oddie.exe
```

Expected: `PE32 executable (console) Intel 80386` (or similar Windows PE32 description).

---

### Task 3: Update `README.md`

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Replace the Dependencies and build sections**

Find this block in `README.md` (near the end):

```markdown
### Dependencies:

MD5 implementation (included)

[Zlib](https://www.zlib.net/) (download latest and extract `*.c` and `*.h` files to the repo directory)

### To cross-compile using Linux (Windows using MinGW is similar):
```
i686-w64-mingw32-gcc -Wall -Wextra -pedantic -std=gnu99 -Werror -Os -s -static -I /opt/cmf/src/oddie oddie.c md5.c compress.c deflate.c crc32.c adler32.c trees.c zutil.c -o oddie.exe -lodbc32 -Wl,-verbose,--subsystem,console
```
```

Replace with:

```markdown
### Dependencies:

MD5 implementation (included)

Zlib (fetched automatically by `build.sh`)

### To build (cross-compile on Linux for Windows):
```
./build.sh
```

Requires `curl`, `tar`, and a MinGW cross-compiler (`i686-w64-mingw32-gcc`, `i686-pc-mingw32-gcc`, or `mingw32-gcc`). The script checks [zlib.net](https://zlib.net) for a newer zlib version and downloads it if available.
```

- [ ] **Step 2: Verify README renders correctly**

```bash
grep -A 10 '### Dependencies' README.md
```

Expected: shows the new text with no reference to manual copying or the raw gcc command.

---

### Task 4: Mark REGRESSIONS cleanups 3 and 4 as fixed

**Files:**
- Modify: `REGRESSIONS`

- [ ] **Step 1: Update cleanup 3**

Find:
```
3. No build system
   There is no Makefile. Compilation requires typing a long gcc command.
   A minimal Makefile with targets for the Windows cross-compile and
   (conditionally) a native Linux test build would reduce build friction.
```

Replace with:
```
3. No build system  [FIXED]
   Added build.sh. Detects MinGW cross-compiler variant, checks zlib.net
   for a newer zlib version (if network available), downloads and extracts
   zlib if needed, then runs the cross-compile command.
```

- [ ] **Step 2: Update cleanup 4**

Find:
```
4. Zlib not vendored
   The build depends on manually copying zlib source files into the repo
   directory before compiling. Either vendor the specific zlib version
   used (as a git submodule or tarball), or add a Makefile that downloads
   and extracts it automatically.
```

Replace with:
```
4. Zlib not vendored  [FIXED]
   build.sh fetches the current zlib release from zlib.net before
   compiling. Zlib source files are listed in .gitignore and are never
   committed to the repo.
```

- [ ] **Step 3: Verify REGRESSIONS looks right**

```bash
grep -A 5 '3\. No build' REGRESSIONS
grep -A 5 '4\. Zlib not' REGRESSIONS
```

Expected: both show the `[FIXED]` lines and the new descriptions.
