# E10-T6-C2I — Vercel credential boundary

## Current state

- The operator confirmed on 2026-09-12 that `VERCEL_TOKEN` was added to the GitHub `Production` environment.
- Repository automation credentials cannot list or read environment secrets, so this record does not claim that the value is valid.
- GitHub environment secret values must never be copied into repository files, pull requests, issues, logs, or ordinary CI jobs.
- Vercel's GitHub integration remains the credential-free path for preview and main-branch deployments.

## Activation boundary

The existing `scripts/e10-t6c-vercel-activation.sh` is the only approved direct Vercel activation operator. It requires both explicit confirmation constants before checking Vercel authentication, pulls Production configuration into a temporary file, runs the fail-closed preflight, changes the single embedded-provider OAuth flag, and then performs the approved production deployment.

A manual GitHub Actions workflow is still required to inject `${{ secrets.VERCEL_TOKEN }}` into that operator. The repository PAT available to the coordinator cannot create or update `.github/workflows/*` because it does not have GitHub's `workflow` scope. No substitute credential transport is allowed.

## Stop gate

Adding the secret is readiness, not production approval. Do not dispatch activation, change `SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED`, deploy with the Vercel CLI, or start provider consent until the separate production deployment decision is explicitly approved.
