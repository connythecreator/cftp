#!/usr/bin/env bash
# Replace the generated benchmark results in a Markdown file.
#
# Usage: bench/update-readme.sh README.md results-fragment.md
#
# The target must contain the markers
#   <!-- BENCH_RESULTS:START -->
#   <!-- BENCH_RESULTS:END -->
# and everything between them is replaced by the fragment.
set -euo pipefail

readme=${1:?usage: update-readme.sh README.md results-fragment.md}
block=${2:?usage: update-readme.sh README.md results-fragment.md}

if ! grep -q '<!-- BENCH_RESULTS:START -->' "$readme" \
    || ! grep -q '<!-- BENCH_RESULTS:END -->' "$readme"; then
    echo "update-readme: $readme is missing the BENCH_RESULTS markers" >&2
    exit 1
fi

awk -v block="$block" '
    BEGIN {
        while ((getline line < block) > 0) {
            replacement = replacement line "\n"
        }
    }
    /<!-- BENCH_RESULTS:START -->/ {
        print
        printf "%s", replacement
        skipping = 1
        next
    }
    /<!-- BENCH_RESULTS:END -->/ {
        skipping = 0
    }
    !skipping { print }
' "$readme" > "$readme.tmp"

mv "$readme.tmp" "$readme"
