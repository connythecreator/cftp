#!/usr/bin/env bash
# Release-gate benchmark environment.
#
# Runs inside bench/gate/Dockerfile. Mounts expected by bench/gate.sh:
#   /bins/cftp, /bins/bench   release binaries (read-only)
#   /gate                     this directory (read-only)
#   /out                      results directory (read-write)
#
# The same container hosts every peer over loopback: sshd for scp and SFTP,
# vsftpd for FTP, and nginx for HTTP and HTTPS PUT, alongside a per-iteration
# `cftp serve` spawned by the harness. Keeping them in one network namespace
# means differences between the protocols are not hidden by a network
# bottleneck.
set -euo pipefail

# A controlled loopback environment is allowed to raise the kernel's socket
# buffer ceilings. The default rmem_max/wmem_max (208 KiB) is smaller than the
# bytes in flight at loopback speeds, so datagrams would be dropped by the
# kernel before any protocol could process them. Every tool benefits equally.
# Docker refuses these writes on some hosts; the run then uses whatever the
# container inherits.
sysctl -q -w net.core.rmem_max=134217728 || true
sysctl -q -w net.core.wmem_max=134217728 || true
sysctl -q -w net.core.rmem_default=134217728 || true
sysctl -q -w net.core.wmem_default=134217728 || true

ITERATIONS=${BENCH_ITERATIONS:-3}
# `--cleanup` below removes each upload once it has been measured, so the huge
# dataset costs one copy plus the generated source instead of one copy per
# peer and iteration, which is what makes it affordable on a CI runner.
DATASETS=${BENCH_DATASETS:-many-small,large,mixed,huge}
LABEL=${BENCH_LABEL:-Docker ubuntu:24.04, loopback}
SMALL_COUNT=${BENCH_SMALL_COUNT:-2000}
SMALL_SIZE=${BENCH_SMALL_SIZE:-4KiB}
LARGE_SIZE=${BENCH_LARGE_SIZE:-256MiB}
HUGE_FILES=${BENCH_HUGE_FILES:-1}
HUGE_SIZE=${BENCH_HUGE_SIZE:-1GiB}
MIXED_DIRS=${BENCH_MIXED_DIRS:-8}
MIXED_PER_DIR=${BENCH_MIXED_PER_DIR:-16}
MIXED_SMALL_SIZE=${BENCH_MIXED_SMALL_SIZE:-4KiB}
MIXED_LARGE_SIZE=${BENCH_MIXED_LARGE_SIZE:-32MiB}

wait_for() {
    local what=$1
    shift
    for _ in $(seq 1 200); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.1
    done
    echo "gate: ${what} did not become ready" >&2
    return 1
}

# --- users and directories -------------------------------------------------

useradd --create-home --shell /bin/bash bench
echo 'bench:bench' | chpasswd

install -d -o bench -g bench /srv/bench/scp /srv/bench/sftp
install -d -o bench -g bench /srv/bench/ftp
install -d -o www-data -g www-data /srv/bench/http /srv/bench/https /srv/bench/nginx-temp
install -d -o www-data -g www-data /var/log/nginx
install -d /var/run/vsftpd/empty /run/sshd

work=$(mktemp -d /tmp/cftp-gate.XXXXXX)

# The gate directory is bind-mounted read-only and owned by the host user.
# vsftpd refuses a config file it does not consider privileged, so stage the
# configs under /etc with root ownership and normal permissions.
install -m 644 /gate/sshd_config /etc/ssh/sshd_config.cftp-gate
install -m 644 /gate/vsftpd.conf /etc/vsftpd-gate.conf
install -m 644 /gate/nginx.conf /etc/nginx/nginx-gate.conf

# --- SSH -------------------------------------------------------------------

ssh-keygen -q -t ed25519 -N '' -f "$work/id_ed25519"
install -d -m 700 -o bench -g bench /home/bench/.ssh
install -m 600 -o bench -g bench "$work/id_ed25519.pub" /home/bench/.ssh/authorized_keys

install -d -m 700 /root/.ssh
install -m 600 "$work/id_ed25519" /root/.ssh/id_ed25519
ssh-keygen -A
# OpenSSH writes a bare host for the default port 22 and `[host]:port` for
# every other port. scp below connects to the default port, so pin the bare
# form; `[127.0.0.1]:22` would not match.
printf '127.0.0.1 %s\n' "$(cat /etc/ssh/ssh_host_ed25519_key.pub)" \
    > /root/.ssh/known_hosts
chmod 600 /root/.ssh/known_hosts

/usr/sbin/sshd -f /etc/ssh/sshd_config.cftp-gate
wait_for sshd ssh -o BatchMode=yes -o ConnectTimeout=1 -o StrictHostKeyChecking=yes \
    bench@127.0.0.1 true

# --- FTP -------------------------------------------------------------------

/usr/sbin/vsftpd /etc/vsftpd-gate.conf
wait_for vsftpd curl --silent --fail --max-time 2 --list-only \
    ftp://bench:bench@127.0.0.1/

# --- HTTP and HTTPS --------------------------------------------------------

# The HTTPS row must verify the peer, so the gate issues a local certificate
# for 127.0.0.1 and gives curl its public half as a trust anchor. The key
# never leaves this container.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -nodes -days 1 -subj /CN=127.0.0.1 -addext subjectAltName=IP:127.0.0.1 \
    -keyout /etc/ssl/cftp-gate.key -out /etc/ssl/cftp-gate.crt >/dev/null 2>&1
chmod 600 /etc/ssl/cftp-gate.key

nginx -c /etc/nginx/nginx-gate.conf -g 'daemon on;'
wait_for nginx curl --silent --output /dev/null --max-time 2 \
    http://127.0.0.1:8080/uploads/
wait_for nginx-tls curl --silent --output /dev/null --max-time 2 \
    --cacert /etc/ssl/cftp-gate.crt https://127.0.0.1:8443/uploads/

# --- harness ---------------------------------------------------------------

install -d /work/target/release/examples
install -m 0755 /bins/cftp /work/target/release/cftp
install -m 0755 /bins/bench /work/target/release/examples/bench

echo "gate: cftp $(/work/target/release/cftp --version)" >&2
echo "gate: $(nginx -v 2>&1), $(vsftpd -v 2>&1), $(ssh -V 2>&1)" >&2
echo "gate: ${ITERATIONS} iterations, datasets [${DATASETS}]" >&2

mkdir -p /out
/work/target/release/examples/bench \
    --gate \
    --iterations "$ITERATIONS" \
    --datasets "$DATASETS" \
    --small-count "$SMALL_COUNT" \
    --small-size "$SMALL_SIZE" \
    --large-size "$LARGE_SIZE" \
    --huge-files "$HUGE_FILES" \
    --huge-size "$HUGE_SIZE" \
    --mixed-dirs "$MIXED_DIRS" \
    --mixed-per-dir "$MIXED_PER_DIR" \
    --mixed-small-size "$MIXED_SMALL_SIZE" \
    --mixed-large-size "$MIXED_LARGE_SIZE" \
    --ssh-target bench@127.0.0.1 \
    --ssh-dest /srv/bench/scp \
    --sftp-target bench@127.0.0.1 \
    --sftp-dest /srv/bench/sftp \
    --ftp-url ftp://bench:bench@127.0.0.1/ \
    --http-put-url http://127.0.0.1:8080/uploads \
    --https-put-url https://127.0.0.1:8443/uploads \
    --http-put-root /srv/bench/http/uploads \
    --https-put-root /srv/bench/https/uploads \
    --ftp-root /srv/bench/ftp \
    --cleanup \
    --https-ca /etc/ssl/cftp-gate.crt \
    --json /out/latest.json \
    --markdown /out/latest.md \
    --readme-block /out/latest-fragment.md \
    --label "$LABEL"

if [[ -n "${HOST_UID:-}" && -n "${HOST_GID:-}" ]]; then
    for file in /out/latest.json /out/latest.md /out/latest-fragment.md; do
        [[ -e "$file" ]] && chown "$HOST_UID:$HOST_GID" "$file"
    done
fi
