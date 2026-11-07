FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        nginx \
        openssh-client \
        openssh-server \
        openssl \
        strace \
        vsftpd \
    && rm -rf /var/lib/apt/lists/*

# The gate config owns the HTTP server; the packaged default site is unused.
RUN rm -f /etc/nginx/sites-enabled/default

ENTRYPOINT ["/bin/bash", "/gate/run.sh"]
