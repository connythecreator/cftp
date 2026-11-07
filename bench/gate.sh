#!/usr/bin/env bash
# Build the release binaries, run the six-peer benchmark gate in Docker, and
# publish its results into README.md and docs/wiki/Benchmarks.md.
#
# The gate compares cftp with the security-equivalent peers (scp, SFTP, HTTPS).
# FTP and HTTP are measured and published as plaintext reference rows.
#
# Environment:
#   BENCH_ITERATIONS, BENCH_DATASETS, BENCH_LABEL, BENCH_SMALL_COUNT,
#   BENCH_SMALL_SIZE, BENCH_LARGE_SIZE, BENCH_HUGE_FILES, BENCH_HUGE_SIZE,
#   BENCH_MIXED_DIRS, BENCH_MIXED_PER_DIR,
#   BENCH_MIXED_SMALL_SIZE, BENCH_MIXED_LARGE_SIZE  forwarded to the container.
#   CFTP_BENCH_IMAGE        image tag (default cftp-bench-gate)
#   CFTP_BENCH_NO_PUBLISH   set to 1 to leave README/docs untouched
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

image=${CFTP_BENCH_IMAGE:-cftp-bench-gate}

cargo build --release --locked -p cftp-cli --bin cftp --example bench
docker build --tag "$image" bench/gate

mkdir -p bench/results
rm -f bench/results/latest.json bench/results/latest.md \
    bench/results/latest-fragment.md

docker_args=(
    run --rm --init
    --volume "$root/target/release/cftp:/bins/cftp:ro"
    --volume "$root/target/release/examples/bench:/bins/bench:ro"
    --volume "$root/bench/gate:/gate:ro"
    --volume "$root/bench/results:/out:rw"
    --env "HOST_UID=$(id -u)"
    --env "HOST_GID=$(id -g)"
)
for name in BENCH_ITERATIONS BENCH_DATASETS BENCH_LABEL BENCH_SMALL_COUNT \
    BENCH_SMALL_SIZE BENCH_LARGE_SIZE BENCH_HUGE_FILES BENCH_HUGE_SIZE \
    BENCH_MIXED_DIRS BENCH_MIXED_PER_DIR \
    BENCH_MIXED_SMALL_SIZE BENCH_MIXED_LARGE_SIZE; do
    if [[ -n "${!name:-}" ]]; then
        docker_args+=(--env "$name")
    fi
done
docker "${docker_args[@]}" "$image"

if [[ "${CFTP_BENCH_NO_PUBLISH:-0}" != "1" ]]; then
    bench/update-readme.sh README.md bench/results/latest-fragment.md
    cp bench/results/latest.md docs/wiki/Benchmarks.md
fi
