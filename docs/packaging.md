# Packaging and releases

## Prerequisites

- Rust 1.88 or newer.
- `cargo-deb` for Debian packages:

  ```bash
  cargo install cargo-deb --version 3.8.0 --locked
  ```

- Docker for Arch builds on non-Arch hosts. The build uses
  `archlinux:base-devel` and `makepkg`.
- `git` for the Arch source tarball target.

## Local package builds

```bash
make assets
make deb
make verify-deb
```

`make deb` writes `target/debian/cftp_<version>-1_amd64.deb`. The package
contains the binary, man page, bash/zsh/fish completions, and the GPL copyright
file.

Arch builds:

```bash
make arch-tarball
make arch-docker
make arch-srcinfo
```

- `make arch-tarball` uses `git archive HEAD`, so commit packaging changes first.
- `make arch-docker` builds and tests the package in a container, then restores
  ownership of `packaging/arch` to the host user.
- `make arch-srcinfo` regenerates `packaging/arch/.SRCINFO`.

## Version bumps

1. Update the workspace version in `Cargo.toml`.
2. Update `pkgver` and `pkgrel` in `packaging/arch/PKGBUILD`.
3. Update `CHANGELOG.md`.
4. Run `make arch-srcinfo` and commit `.SRCINFO`.
5. Run `make check`, `make deb`, and `make arch-docker`.
6. Tag the release as `vX.Y.Z` and push the tag.
7. The release workflow builds the `.deb` and Arch package, generates
   `SHA256SUMS`, and publishes them to the GitHub Release.

Replace `sha256sums=('SKIP')` in `PKGBUILD` with real checksums (`updpkgsums`)
before publishing to the AUR.
