#!/usr/bin/env bash
# Stage the user-facing documentation as an mdBook and render it.
#
# mdBook requires every chapter to live under the book's source directory, so
# pages that sit outside `docs/wiki` (the README, the benchmark guide, the
# architecture notes, the security policy) are copied into a staging directory
# and the links that point out of the wiki are rewritten to their local copies.
# The staging directory lives under `target/`, so nothing is duplicated in git.
#
# Usage: scripts/build-book.sh [OUTPUT_DIR]
# Requires `mdbook` on PATH (cargo install mdbook --locked).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
book="$root/target/book"
src="$book/src"
out=${1:-$root/book}

rm -rf "$book"
mkdir -p "$src"

cp "$root"/docs/wiki/*.md "$src"/
cp "$root"/docs/architecture.md "$root"/docs/resume.md "$root"/docs/packaging.md "$src"/
# mdBook renders a top-level README.md as the site index; the wiki Home page is
# the better landing page for new users, so the README is staged under a name
# that keeps it a normal chapter.
cp "$root/README.md" "$src/Overview.md"
cp "$root/CHANGELOG.md" "$src/CHANGELOG.md"
cp "$root/LICENSE" "$src/LICENSE"
cp "$root/bench/README.md" "$src/Benchmark-guide.md"
cp "$root/.github/SECURITY.md" "$src/SECURITY.md"
cp "$root/.github/CONTRIBUTING.md" "$src/CONTRIBUTING.md"
mkdir -p "$src/assets"
cp -r "$root"/assets/. "$src/assets"/

# Rewrite the links that escape docs/wiki for GitHub. The rewritten targets are
# the copies made above.
sed -i \
    -e 's|(\.\./architecture\.md)|(architecture.md)|g' \
    -e 's|(\.\./resume\.md)|(resume.md)|g' \
    -e 's|(\.\./packaging\.md)|(packaging.md)|g' \
    -e 's|(\.\./\.\./README\.md)|(Overview.md)|g' \
    -e 's|(\.\./\.\./bench/README\.md)|(Benchmark-guide.md)|g' \
    -e 's|(\.\./\.\./\.github/SECURITY\.md)|(SECURITY.md)|g' \
    -e 's|(\.\./\.github/SECURITY\.md)|(SECURITY.md)|g' \
    -e 's|(\.\./bench/README\.md)|(Benchmark-guide.md)|g' \
    -e 's|](docs/architecture\.md)|](Architecture.md)|g' \
    -e 's|](docs/packaging\.md)|](packaging.md)|g' \
    -e 's|](docs/wiki/Benchmarks\.md)|](Benchmarks.md)|g' \
    -e 's|](docs/wiki/Home\.md)|](Home.md)|g' \
    -e 's|](\.github/SECURITY\.md)|](SECURITY.md)|g' \
    -e 's|](\.github/CONTRIBUTING\.md)|](CONTRIBUTING.md)|g' \
    -e 's|src="\.\./\.\./assets/|src="assets/|g' \
    -e 's|(\.\./\.\./assets/|(assets/|g' \
    "$src"/*.md

chapter() {
    local file=$1
    local title=$2
    if [[ -f "$src/$file" ]]; then
        printf -- '- [%s](%s)\n' "$title" "$file"
    fi
}

{
    printf '# Summary\n\n'
    chapter Home.md "Introduction"
    printf '\n# Using cftp\n\n'
    chapter Usage.md "Usage"
    chapter Benchmarks.md "Benchmarks"
    chapter Troubleshooting.md "Troubleshooting"
    printf '\n# Understanding cftp\n\n'
    chapter Architecture.md "Architecture"
    chapter architecture.md "Design notes"
    chapter resume.md "Crash recovery design"
    chapter Protocol.md "Wire protocol"
    chapter Security.md "Security"
    printf '\n# Reference\n\n'
    chapter Overview.md "Overview"
    chapter Library.md "Library"
    chapter Benchmark-guide.md "Benchmark harness"
    chapter packaging.md "Packaging"
    chapter CHANGELOG.md "Changelog"
    chapter CONTRIBUTING.md "Contributing"
    chapter SECURITY.md "Security policy"
} > "$src/SUMMARY.md"

cat > "$book/book.toml" <<'EOF'
[book]
title = "cftp"
authors = ["cftp contributors"]
description = "QUIC-based file transfer with mutual TLS"
language = "en"
src = "src"

[output.html]
git-repository-url = "https://github.com/connythecreator/cftp"
EOF

rm -rf "$out"
mdbook build "$book" --dest-dir "$out"
echo "book: $out/index.html"
