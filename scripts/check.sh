#!/usr/bin/env bash
# Build/test gate với output tối thiểu (tiết kiệm token): im lặng khi pass,
# chỉ in lỗi/warning/test fail khi có vấn đề.
#
# Dùng:  scripts/check.sh [fast|full]
#   fast (mặc định): fmt --check + clippy + test lib/integration (không doc-test, không example)
#   full           : fast + build examples + doc-test
set -uo pipefail
cd "$(dirname "$0")/.."

mode="${1:-fast}"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT
max_lines="${CHECK_MAX_LINES:-40}"

run() { # run <name> <cmd...>
  local name="$1"; shift
  if "$@" >"$log" 2>&1; then
    echo "ok   $name"
  else
    echo "FAIL $name"
    # Chỉ giữ dòng hữu ích: lỗi, warning, vị trí, test fail; bỏ Compiling/Checking/Running.
    grep -Ev '^\s*(Compiling|Checking|Fresh|Finished|Running|Downloaded|Downloading|Blocking|Locking|Updating|Adding)\b|^\s*$|^test .* \.\.\. ok$' "$log" \
      | head -n "$max_lines"
    echo "(log rút gọn ${max_lines} dòng; đặt CHECK_MAX_LINES để tăng)"
    exit 1
  fi
}

run fmt     cargo fmt --check
run clippy  cargo clippy --all-targets --quiet -- -D warnings
# Lib production không bật feature test-support (dev-dependency tự bật khi build tests).
run clippy-lib cargo clippy --lib --quiet -- -D warnings
run test    cargo test --lib --tests --quiet
if [ "$mode" = full ]; then
  run examples cargo build --examples --quiet
  run doctest  cargo test --doc --quiet
fi
echo "ALL OK ($mode)"
