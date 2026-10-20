SHELL := /bin/bash

VERSION := $(shell cargo pkgid -p cftp-cli | sed 's/.*[#@]//')
ARCH_DIR := packaging/arch
ARCH_TARBALL := $(ARCH_DIR)/cftp-$(VERSION).tar.gz
DEB := target/debian/cftp_$(VERSION)-1_amd64.deb

.PHONY: all assets release deb arch-tarball arch-docker arch-srcinfo verify-deb clean

all: deb

assets:
	cargo run --quiet -p cftp-cli --example gen-assets -- packaging/generated

release:
	cargo build --release --locked

deb: assets
	cargo deb -p cftp-cli --locked

arch-tarball:
	git archive --format=tar.gz --prefix=cftp-$(VERSION)/ -o $(ARCH_TARBALL) HEAD

arch-docker: arch-tarball
	docker run --rm -v "$(CURDIR)/$(ARCH_DIR):/pkg" -w /pkg archlinux:base-devel bash -lc '\
		pacman -Syu --noconfirm --needed rust >/dev/null && \
		useradd -m builder && \
		chown -R builder /pkg && \
		su builder -c "makepkg -f --noconfirm"'

arch-srcinfo:
	docker run --rm -v "$(CURDIR)/$(ARCH_DIR):/pkg" -w /pkg archlinux:base-devel bash -lc '\
		pacman -Syu --noconfirm >/dev/null && \
		useradd -m builder && \
		chown -R builder /pkg && \
		su builder -c "makepkg --printsrcinfo"' > $(ARCH_DIR)/.SRCINFO

verify-deb: deb
	dpkg-deb --info "$(DEB)"
	dpkg-deb --contents "$(DEB)"
	tmp=$$(mktemp -d); dpkg-deb -x "$(DEB)" "$$tmp"; "$$tmp/usr/bin/cftp" --version; rm -rf "$$tmp"

clean:
	rm -rf packaging/generated $(ARCH_DIR)/src $(ARCH_DIR)/pkg $(ARCH_TARBALL) $(ARCH_DIR)/*.pkg.tar.*
