# E10-T6-C2I — Vercel credential boundary

## Current state

- The operator confirmed on 2026-09-12 that `VERCEL_TOKEN` was added to the GitHub `Production` environment.
- Repository automation credentials cannot list or read environment secrets, so this record does not claim that the value is valid.
- GitHub environment secret values must never be copied into repository files, pull requests, issues, logs, or ordinary CI jobs.
- Vercel's GitHub integration remains the credential-free path for preview and main-branch deployments.

## Activation boundary

The existing `scripts/e10-t6c-vercel-activation.sh` is the only approved direct Vercel activation operator. It requires both explicit confirmation constants before checking Vercel authentication, pulls Production configuration into a temporary file, runs the fail-closed preflight, changes the single embedded-provider OAuth flag, and then performs the approved production deployment.

The manual `.github/workflows/e10-t6c-production-activation.yml` workflow is the only GitHub Actions entry point for this operator. It has no automatic trigger, accepts only a dispatch of `main`, requires two separate exact-value confirmations without defaults, serializes activation, and attaches its job to the GitHub `Production` environment. Permissions are limited to `contents: read`; checkout credentials are not persisted. `VERCEL_TOKEN` is injected only into the final operator step and no substitute credential transport is allowed.

## Stop gate

Adding the secret, preparing this workflow, opening its PR, passing CI, or merging it to `main` is readiness—not Production activation approval. After this task is complete, the user must provide a new and explicit Production dispatch approval before anyone dispatches the workflow. Until that separate decision, do not dispatch activation, change `SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED`, deploy with the Vercel CLI, or start provider consent. A merged workflow remains inert unless a human supplies both exact confirmations in a new manual dispatch.
