# GitHub Actions workflows

CI/CD for **aerospike-jdbc**: JFrog release bundles, optional GitHub draft releases, and **Maven Central via JFrog → `citrusleaf/artifact-publisher` webhook** (not from this repo’s workflows).

## Workflows overview

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **build** | Push to `main`; PR to `main` or `stage` | Build and test. Does not publish. |
| **push-to-stage** | Push to `stage`, version tags, or manual | Runs **build-release** only: build, sign, deploy to JFrog, **create release bundle**. **No** JF lifecycle promotion on push (avoids re-promoting an existing version such as `2.1.3` while Sonatype is still pending). |
| **build-release** | Called by **push-to-stage** | Maven build + shared-workflows sign/deploy + release bundle. Does **not** promote the bundle to DEV/TEST/STAGE. |
| **promote-release-bundle** | Manual (`workflow_dispatch`) | **`version`** + **`targets`**: promote **`aerospike-jdbc`** in JFrog — **DEV** only, **TEST** then **STAGE**, or **DEV → TEST → STAGE** (shared-workflows `promote-release-bundle`). |
| **promote** | Called by **promote-prod** only | JFrog **`build-promote`** to **`database-maven-dev-local`**, then **draft GitHub release** with artifacts. Does **not** call Maven Central. |
| **promote-prod** | Manual (`workflow_dispatch`) | Run **promote** with **`jf-build-id`** from **build-release** (optional path for dev-local + draft release). |
| **sonatype-approve-or-delete** | Manual (`workflow_dispatch`) | Optional Sonatype Central Portal API (GET/POST/DELETE). Not wired into **promote**; keep for manual ops. |

## Release flow (Path A)

1. **Build / bundle** — Push to **`stage`**, a **version tag**, or run **Java Build & Release**. Produces artifacts and a JFrog **release bundle** at the POM version. **Promotion is not automatic** (unlike **aerospike-client-java-reactive**, which runs **`promote-release-bundle` → DEV** inside **`create-release-bundle.yml`** after each bundle).

2. **JF lifecycle (manual)** — **Actions** → **Promote release bundle** → **`version`** (e.g. `2.1.3`) → **`targets`**: **DEV** (DEV only), **TEST_AND_STAGE** (default: TEST then STAGE), or **DEV_THEN_TEST_AND_STAGE** (all three in order). Uses the bundle already in JFrog; no **`jf-build-id`**.

3. **PREVIEW / PROD** — Product (or policy owner) promotes the bundle in the **JFrog UI** so the action is audited. **Maven Central** (and other upstreams) are triggered by the **JFrog webhook** into **`citrusleaf/artifact-publisher`**, not by a reusable workflow call from this repository.

4. **Optional: dev-local + draft GitHub release** — **Promote to Prod** (`promote-prod.yml`) still runs **`build-promote`** to **`database-maven-dev-local`** and creates a **draft** GitHub release from those artifacts. Use only if your team still wants that path; it is **orthogonal** to release-bundle promotion and does **not** publish to Maven Central.

### Build id vs bundle version

| Mechanism | Identifier | Use case |
|-----------|------------|----------|
| **`promote-release-bundle`** (workflow) | **`version`**, **`targets`** | Release-bundle lifecycle in JFrog: **DEV** / **TEST+STAGE** / **DEV+TEST+STAGE**. |
| **`promote-prod` → `promote.yml`** | **`jf-build-id`** | JFrog **build** promotion into **`database-maven-dev-local`** + draft GitHub release. |

Existing release bundles in JFrog are **unchanged** by these workflow edits. New runs only add automation and remove the in-repo Maven publish step.

## Required configuration

### Secrets (Settings → Secrets and variables → Actions)

| Secret | Used by | Description |
|--------|---------|-------------|
| `GPG_SECRET_KEY` | build-release | GPG private key (armored). |
| `GPG_PUBLIC_KEY` | build-release | GPG public key. |
| `GPG_PASS` | build-release | GPG passphrase. |
| `JFROG_OIDC_PROVIDER` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC provider for JFrog. |
| `JFROG_OIDC_AUDIENCE` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC audience for JFrog. |
| `AEROSPIKE_SA_CICD_USERNAME` | sonatype-approve-or-delete | Sonatype API user (manual workflow only). |
| `AEROSPIKE_SA_CICD_PASSWORD` | sonatype-approve-or-delete | Sonatype API token/password (manual workflow only). |

### Variables (Settings → Variables)

| Variable | Used by | Description |
|----------|---------|-------------|
| `BUILD_CONTAINER_DISTRO_VERSION` | build-release, promote, promote-release-bundle | Runner image. |
| `JFROG_PROJECT` | build-release, promote, promote-release-bundle | JFrog project key. |
| `JFROG_PLATFORM_URL` | promote, promote-release-bundle | JFrog platform URL. |
| `OIDC_PROVIDER_NAME` | build-release, promote, promote-release-bundle | OIDC provider (vars). |
| `OIDC_AUDIENCE` | build-release, promote, promote-release-bundle | OIDC audience (vars). |
| `SONATYPE_DOMAIN_NAME` | sonatype-approve-or-delete | Sonatype API base URL. |

## Composite actions

| Action | Role |
|--------|------|
| **get-version** | Snapshot vs release + version outputs. |
| **stage-release-artifacts** | Download promoted artifacts from JFrog for GitHub attach (**promote** passes `staging` + `github` only). |
| **publish-to-github** | Draft GitHub release + artifacts. |
| **publish-to-sonatype** | Not invoked by **promote**; optional for other/manual flows. |
| **publish-build-info-to-jfrog** | Build metadata to JFrog. |

## Dependencies

- **build-release** uses `aerospike/shared-workflows` at **`v3.5.0`** (`reusable_artifacts-cicd`, `reusable_create-release-bundle`, **`promote-release-bundle`** action).
- **Maven Central:** ensure **`(database, aerospike-jdbc)`** (or your project key) is on **`citrusleaf/artifact-publisher`** `publish-allow-list.json`, JFrog **webhook** to **`artifact-publisher`** is configured for **PROD** bundle promotion, and JFrog/OIDC permissions match org standards.
- **`main`** is **not** updated by these workflows; merge from **`stage`** per team process.
