# Security policy

## Supported versions

Security fixes are provided for the latest `0.1.x` release. Older releases may
receive fixes at the maintainer's discretion.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's private vulnerability
reporting:

<https://github.com/connythecreator/cftp/security/advisories/new>

Do not open a public issue, discussion, or pull request for a security problem.
Include:

- A description of the issue and its impact.
- Steps to reproduce, including the cftp version and platform.
- Any relevant logs, configuration, or proof-of-concept code.
- Whether you would like to be credited in the advisory.

## Scope

Security-relevant areas include:

- Mutual TLS and certificate verification.
- Control-frame and data-stream parsing.
- Path validation, symlink handling, and writes under the receive root.
- Decompression limits and resource exhaustion.
- Dependency vulnerabilities reported through RustSec advisories.

## Response

Reports are reviewed on a best-effort basis. The maintainer will confirm receipt,
assess severity, and coordinate a fix and disclosure timeline with the reporter.
Please allow time for a release before public disclosure.
