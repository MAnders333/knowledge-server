# Security Policy

## Supported versions

Security fixes go into the latest release. Older releases are not patched; upgrade with `knowledge-server update`.

## Reporting a vulnerability

Do not open a public issue for a security problem. Report it privately through GitHub's private vulnerability reporting:

- Go to the repository's **Security** tab and choose **Report a vulnerability**, or open <https://github.com/MAnders333/knowledge-server/security/advisories/new>.

Include the affected version, what an attacker can do, and the steps to reproduce. Do not include real credentials or private data in the report; redact them first.

The maintainer replies in the advisory thread. A fix is published as a new release, and the advisory is published once users can upgrade.

## Verifying releases

Release binaries, `SHA256SUMS` and `install.sh` carry GitHub artifact attestations. See "Verifying a release" under "Security" in the README.
