# E10-T6-C2I — Vercel credential boundary

## Current state

- The operator confirmed on 2026-09-12 that `VERCEL_TOKEN` was added to the GitHub `Production` environment.
- Repository automation credentials cannot list or read environment secrets, so this record does not claim that the value is valid.
- GitHub environment secret values must never be copied into repository files, pull requests, issues, logs, or ordinary CI jobs.
- Vercel's GitHub integration remains the credential-free path for preview and main-branch deployments.

## Activation boundary

The existing `scripts/e10-t6c-vercel-activation.sh` is the only approved direct Vercel activation operator. It requires both explicit confirmation constants before checking Vercel authentication, changes the single embedded-provider OAuth flag, and then performs the approved production deployment. It never pulls sensitive Production values into the GitHub runner.

The manual `.github/workflows/e10-t6c-production-activation.yml` workflow is the only GitHub Actions entry point for this operator. It has no automatic trigger, accepts only a dispatch of `main`, requires two separate exact-value confirmations without defaults, serializes activation, and attaches its job to the GitHub `Production` environment. Permissions are limited to `contents: read` and `id-token: write`; checkout credentials are not persisted. Before the token-scoped operator, a dedicated client obtains a short-lived GitHub Actions OIDC token and calls the canonical Vercel runtime preflight. The endpoint verifies the exact repository, workflow, `main` ref, `workflow_dispatch` event, `Production` environment, audience, signature, and token lifetime before evaluating real runtime configuration. It returns only the existing redacted contract and never contacts a provider. `VERCEL_TOKEN` remains injected only into the final operator step.

Vercel CLI 59.16.0 does not download sensitive Production values through `env pull`; it writes `[SENSITIVE]` placeholders. Therefore a runner-local preflight cannot distinguish valid secrets from placeholders and is forbidden. The OIDC boundary follows GitHub's official cloud OIDC model: `id-token: write` only permits requesting a short-lived token and does not grant repository write access. See [GitHub OIDC reference](https://docs.github.com/en/actions/reference/security/oidc) and [OIDC token hardening](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-cloud-providers).

## Stop gate

Adding the secret, preparing this workflow, opening its PR, passing CI, or merging it to `main` is readiness—not Production activation approval. After this task is complete, the user must provide a new and explicit Production dispatch approval before anyone dispatches the workflow. Until that separate decision, do not dispatch activation, change `SHOPIFY_EMBEDDED_PROVIDER_OAUTH_ENABLED`, deploy with the Vercel CLI, or start provider consent. A merged workflow remains inert unless a human supplies both exact confirmations in a new manual dispatch.
