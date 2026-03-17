# GitHub Actions workflows

This directory contains the CI/CD workflows and composite actions used to build, stage, and publish **aerospike-jdbc** to JFrog, Maven Central (Sonatype), and GitHub Releases.

## Workflows overview

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **build** | Push/PR to `main` | Build and test on every commit. Does not publish. |
| **push-to-stage** | Push to `stage`, version tags, or manual | Runs **build-release**: build, sign, deploy to JFrog, create release bundle. |
| **build-release** | Called by `push-to-stage` only | Builds with Maven, signs artifacts, deploys to JFrog (requires shared-workflows). |
| **promote** | Called by **promote-prod** only | Promotes a JFrog build: publish to Sonatype (staging) and create a GitHub release (draft). |
| **promote-prod** | Manual (`workflow_dispatch`) | Promotes a specific build from stage to “prod”: run **promote** with a build number. |
| **sonatype-approve-or-delete** | Manual (`workflow_dispatch`) | Approve or discard a Sonatype staging deployment (final step before Maven Central). |

## How to run a release

### 1. Build and upload to JFrog (stage)

Either:

- **Push to `stage`**, or  
- **Push a version tag** (`v2.0.1`, `2.0.1`, `v2.0.1-rc1`, etc.), or  
- **Run “Java Build & Release”** from the Actions tab → **Run workflow**.

This runs **push-to-stage** → **build-release**: the project is built, artifacts are signed and deployed to JFrog. No Maven Central or GitHub release yet.

### 2. Note the build number

After the run finishes, open the workflow run. The **build number** is the run number (e.g. `42`) for that repository. You will need it for **Promote to Prod**.

### 3. Promote to “prod” (Sonatype staging + GitHub release)

1. Go to **Actions** → **Promote to Prod**.
2. Click **Run workflow**.
3. Enter the **build number** from step 2.
4. Run the workflow.

This runs **promote**: it promotes that build in JFrog, uploads to Sonatype (staging), and creates a **draft** GitHub release with the artifacts. The artifact is **not** on Maven Central until you approve it in Sonatype.

### 4. Approve (or discard) on Sonatype

1. In Sonatype (Central Portal), find the deployment created by the promote run and copy its **stage-release-id** (UUID).
2. Go to **Actions** → **Approve or Delete Sonatype Deployment**.
3. Click **Run workflow**.
4. Enter the **stage-release-id** and choose **POST** (approve) or **DELETE** (discard).
5. Run the workflow.

- **POST**: Publishes the staging deployment to Maven Central.  
- **DELETE**: Discards the staging deployment.

After **POST**, the release will sync to Maven Central. You can then publish the draft GitHub release if desired.

## Required configuration

### Secrets (Settings → Secrets and variables → Actions)

| Secret | Used by | Description |
|--------|---------|-------------|
| `GPG_SECRET_KEY` | build-release | GPG private key (armored) for signing artifacts. |
| `GPG_PUBLIC_KEY` | build-release | GPG public key. |
| `GPG_PASS` | build-release | Passphrase for the GPG key. |
| `AEROSPIKE_SA_CICD_USERNAME` | promote, sonatype-approve-or-delete | Sonatype/Central username. |
| `AEROSPIKE_SA_CICD_PASSWORD` | promote, sonatype-approve-or-delete | Sonatype/Central token or password. |
| `CLIENT_BOT_PAT` | promote | GitHub PAT for checkout and creating releases. |
| `JFROG_OIDC_PROVIDER` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC provider name for JFrog. |
| `JFROG_OIDC_AUDIENCE` | promote, stage-release-artifacts, publish-build-info-to-jfrog | OIDC audience for JFrog. |

### Variables (Settings → Variables)

| Variable | Used by | Description |
|----------|---------|-------------|
| `BUILD_CONTAINER_DISTRO_VERSION` | build-release, promote | Runner image (e.g. `ubuntu-latest`). |
| `JFROG_PROJECT` | build-release | JFrog project key. |
| `JFROG_PLATFORM_URL` | promote | JFrog platform URL (e.g. `https://aerospike.jfrog.io`). |
| `OIDC_PROVIDER_NAME` | build-release, promote | Same as the OIDC provider used in secrets. |
| `OIDC_AUDIENCE` | build-release, promote | Same as the OIDC audience used in secrets. |
| `SONATYPE_DOMAIN_NAME` | promote, sonatype-approve-or-delete | Sonatype API base (e.g. `https://central.sonatype.com`). |
| `VALIDATION_MAX_NUMBER_CHECKS` | promote | Max polling attempts for Sonatype validation (e.g. `30`). |

## Composite actions

Used by the workflows above; you normally don’t run them directly.

- **get-version** – Determines snapshot vs release and sets version output.
- **stage-release-artifacts** – Downloads promoted artifacts from JFrog into staging folders for Sonatype and GitHub.
- **publish-to-sonatype** – Uploads the staging bundle to Sonatype and waits for validation.
- **publish-to-github** – Creates a draft GitHub release and attaches the staged artifacts.
- **publish-build-info-to-jfrog** – Publishes build metadata (e.g. Sonatype staging id) to JFrog.

## Dependencies

- **build-release** uses `aerospike/shared-workflows` (reusable workflows for build, sign, deploy to JFrog). The repo must have access to that organization and the JFrog/OIDC setup must match.
- Version tags should match the format used in **push-to-stage** (e.g. `v2.0.1`, `2.0.1`, `v2.0.1-rc1`).
