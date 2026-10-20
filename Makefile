SHELL := /bin/bash

VERSION := $(shell cargo pkgid -p cftp-cli | sed 's/.*[#@]//')
ARCH_DIR := packaging/arch
ARCH_TARBALL := $(ARCH_DIR)/cftp-$(VERSION).tar.gz
DEB := target/debian/cftp_$(VERSION)-1_amd64.deb

.PHONY: all assets release fmt fmt-check clippy test doc check lint deny typos markdownlint actionlint ci check-version deb arch-tarball arch-docker arch-srcinfo verify-deb clean

all: deb

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --locked -- -D warnings

test:
	cargo test --workspace --locked

doc:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

check: fmt-check clippy test

lint: fmt-check clippy typos check-version

deny:
	cargo deny check

typos:
	typos

markdownlint:
	npx --yes markdownlint-cli2 "**/*.md"

actionlint:
	docker run --rm -v "$(CURDIR):/repo" -w /repo rhysd/actionlint:latest

ci: check doc deny typos check-version

check-version:
	@version=$$(cargo pkgid -p cftp-cli | sed 's/.*[#@]//'); \
	if ! grep -qx "pkgver=$$version" packaging/arch/PKGBUILD; then \
		echo "PKGBUILD pkgver does not match workspace version $$version" >&2; \
		exit 1; \
	fi

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
