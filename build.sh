#!/usr/bin/env bash
#
# Windows-friendly build helper. Invoke it via:
#   bash build.sh              (from Command Prompt or PowerShell)
#   bash build.sh --clean      (remove previous binary first)
# The script keeps everything self-contained inside cheetah-db/.

# Or just execute "go run ."

set -euo pipefail

SCRIPT_DIR=$(
  cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
  pwd
)
cd "$SCRIPT_DIR"

OUTPUT_NAME="cheetah-server.exe"
GO_OS=${GOOS:-windows}
GO_ARCH=${GOARCH:-amd64}
CLEAN=0
VERBOSE=0

usage() {
  cat <<'EOF'
Usage: bash build.sh [options]

Options:
  --clean      Remove the previously built binary before compiling.
  --release    Strip symbols for a smaller binary (default).
  --debug      Keep symbols (disables -ldflags "-s -w").
  --verbose    Echo the go build command before executing it.
  -h, --help   Show this help text.

Environment overrides:
  GOOS, GOARCH, CGO_ENABLED, and other standard Go variables work as usual.
EOF
}

LD_FLAGS="-s -w"
while (($#)); do
  case "$1" in
    --clean)
      CLEAN=1
      ;;
    --release)
      LD_FLAGS="-s -w"
      ;;
    --debug)
      LD_FLAGS=""
      ;;
    --verbose)
      VERBOSE=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

if ! command -v go >/dev/null 2>&1; then
  echo "Error: Go toolchain not found in PATH. Install Go and retry." >&2
  exit 1
fi

if [[ $CLEAN -eq 1 ]]; then
  rm -f "$OUTPUT_NAME" "${OUTPUT_NAME%.exe}"
fi

BUILD_CMD=(env GOOS="$GO_OS" GOARCH="$GO_ARCH" go build)
if [[ -n "$LD_FLAGS" ]]; then
  BUILD_CMD+=(-ldflags "$LD_FLAGS")
fi
BUILD_CMD+=(-trimpath -o "$OUTPUT_NAME" .)

if [[ $VERBOSE -eq 1 ]]; then
  printf '>> %q ' "${BUILD_CMD[@]}"
  printf '\n'
fi

"${BUILD_CMD[@]}"

echo "cheetah-db build complete -> $SCRIPT_DIR/$OUTPUT_NAME"
