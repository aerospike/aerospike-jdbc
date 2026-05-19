# GitHub Actions workflows

CI/CD for **aerospike-jdbc**: JFrog, Maven Central (via `citrusleaf/artifact-publisher`), and GitHub Releases.

## Workflows overview

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **build** | Push to `main`; PR to `main` or `stage` | Build and test. Does not publish. |
| **push-to-stage** | Push to `stage`, version tags, or manual | Runs **build-release**: build, sign, deploy to JFrog, create release bundle. |
| **build-release** | Called by **push-to-stage** | Maven build + shared-workflows sign/deploy + release bundle. |
| **promote** | Called by **promote-prod** only | JFrog `build-promote`, dispatch **artifact-publisher** (Maven Central), draft GitHub release. |
| **promote-prod** | Manual (`workflow_dispatch`) | Run **promote** with **jf-build-id** from **build-release**. |
| **sonatype-approve-or-delete** | Manual (`workflow_dispatch`) | Optional Sonatype Central Portal API (GET/POST/DELETE). **Not** used by **promote**; keep for manual ops or legacy. |

## How to run a release

### 1. Build and upload to JFrog

Push to **`stage`**, push a **version tag** (`v2.0.1`, `2.0.1`, `2.0.1-rc1`, …), or run **Java Build & Release** manually. That runs **push-to-stage** → **build-release**. Maven Central / GitHub release are **not** done here.

### 2. Build id for promotion

From the **Build and release** job, copy workflow output **`jf-build-id`** (JFrog build id, not the GitHub run id). From JFrog UI, use the first numeric id after the build name in the build URL.

### 3. Promote to prod

**Actions** → **Promote to Prod** → enter **`jf-build-id`** → run.

**promote** will: promote the build in JFrog to **`database-maven-dev-local`**, dispatch **`citrusleaf/artifact-publisher`** for Maven Central (see that repo’s run for progress), then create a **draft** GitHub release with artifacts.

### 4. After the run

- Watch **artifact-publisher** (and Maven Central sync) if needed.
- **Publish** the draft GitHub release when ready.
- **`main`** is **not** updated by these workflows; merge or fast-forward from **`stage`** per team process.

## Required configuration

### Secrets (Settings → Secrets and variables → Actions)

| Secret | Used by | Description |
|--------|---------|-------------|
| `GPG_SECRET_KEY` | build-release | GPG private key (armored). |
| `GPG_PUBLIC_KEY` | build-release | GPG public key. |
| `GPG_PASS` | build-release | GPG passphrase. |
| `CLIENT_BOT_PAT` | promote | PAT that can dispatch `citrusleaf/artifact-publisher`. |
| `JFROG_OIDC_PROVIDER` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC provider for JFrog. |
| `JFROG_OIDC_AUDIENCE` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC audience for JFrog. |
| `AEROSPIKE_SA_CICD_USERNAME` | sonatype-approve-or-delete | Sonatype API user (manual workflow only). |
| `AEROSPIKE_SA_CICD_PASSWORD` | sonatype-approve-or-delete | Sonatype API token/password (manual workflow only). |

### Variables (Settings → Variables)

| Variable | Used by | Description |
|----------|---------|-------------|
| `BUILD_CONTAINER_DISTRO_VERSION` | build-release, promote | Runner image. |
| `JFROG_PROJECT` | build-release, promote | JFrog project key. |
| `JFROG_PLATFORM_URL` | promote | JFrog platform URL. |
| `OIDC_PROVIDER_NAME` | build-release, promote | OIDC provider (vars). |
| `OIDC_AUDIENCE` | build-release, promote | OIDC audience (vars). |
| `ARTIFACT_PUBLISHER_DISPATCH_REF` | promote | Optional ref on `artifact-publisher` (default `main`). |
| `SONATYPE_DOMAIN_NAME` | sonatype-approve-or-delete | Sonatype API base URL. |

## Composite actions

| Action | Role |
|--------|------|
| **get-version** | Snapshot vs release + version outputs. |
| **stage-release-artifacts** | Download promoted artifacts from JFrog for GitHub attach (promote passes `staging` + `github` only). |
| **publish-to-github** | Draft GitHub release + artifacts. |
| **publish-to-sonatype** | Not invoked by **promote**; optional for other/manual flows. |
| **publish-build-info-to-jfrog** | Build metadata to JFrog. |

## Dependencies

- **build-release** uses `aerospike/shared-workflows` (reusable build/sign/deploy and release bundle).
- Tag patterns match **push-to-stage** (`v*`, semver, `*-rc*`).
