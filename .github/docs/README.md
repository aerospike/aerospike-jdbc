# CI/CD - aerospike-jdbc

The publishing flow is JFrog release-bundle based, with a separate manual GitHub
Release path for the signed JDBC uber JAR.

Maven Central is not uploaded directly from this repository. JFrog promotion and
the organization artifact-publisher path handle Central publication.

## Release Flow

```text
push stage or version tag, or manual dispatch
  -> Trigger Creating Release Bundle
  -> create-release-bundle
  -> JFrog build artifacts + release bundle
  -> automatic DEV promotion for non-snapshot releases

manual: Promote release bundle to TEST and STAGE
manual: JFrog UI / org publishing path -> Maven Central
manual: Draft GitHub release -> versioned JFrog artifacts + signed uber JAR
```

## Workflows

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| **Build project** (`build.yml`) | Push to `main`; PR to `main`/`stage` | Maven build with Aerospike. No publish. |
| **Trigger Creating Release Bundle** (`release-creation-trigger.yml`) | Push to `stage`; version tags; manual | Calls `create-release-bundle.yml`. |
| **Build artifact and create release bundle** (`create-release-bundle.yml`) | Reusable workflow | Version detect -> build/sign/deploy -> JFrog release bundle -> DEV promotion. |
| **Promote release bundle to TEST and STAGE** | Manual | Promotes a non-snapshot release bundle version to TEST and STAGE. |
| **Draft GitHub release** | Manual | Downloads versioned JFrog artifacts, builds/signs the uber JAR, and creates a draft GitHub Release. No JFrog writes. |
| **Snyk scan** | Push/PR | Security scan and SARIF upload. |

Removed/superseded workflows: `push-to-stage.yml`, `build-release.yml`,
`promote-release-bundle.yml`, and `promote-to-dev.yml`.

## Version Detection

`get-version` follows the mapper-style release logic:

| Condition | Result |
|-----------|--------|
| Release or RC tag | Release build using the project version. |
| `pom.xml` version changed since the previous commit | Release build using the project version. |
| No release tag and no project version change | Snapshot build using `<project.version>-SNAPSHOT_<sha>`. |

## Manual Inputs

**Promote release bundle to TEST and STAGE**

| Input | Example |
|-------|---------|
| `version` | `2.1.7` |

**Draft GitHub release**

| Input | Example |
|-------|---------|
| `version` | `2.1.7` |
| `artifact-download-repository` | `database-maven-local` |

The GitHub Release workflow resolves source from the matching bare-version Git
tag (`<version>`, for example `2.1.7`), downloads Maven artifacts from JFrog
using the version path, builds and signs `uber-aerospike-jdbc-<version>.jar`,
and creates a draft GitHub Release.

## Secrets

| Secret | Used for |
|--------|----------|
| `GPG_SECRET_KEY`, `GPG_PUBLIC_KEY`, `GPG_PASS` | Release artifact signing and draft GitHub release uber-JAR signing. |
| `JFROG_OIDC_PROVIDER`, `JFROG_OIDC_AUDIENCE` | JFrog read access in the draft GitHub Release path. |

## Variables

| Variable | Used for |
|----------|----------|
| `BUILD_CONTAINER_DISTRO_VERSION` | Runner image. |
| `JFROG_PROJECT`, `JFROG_PLATFORM_URL` | JFrog release-bundle and promotion flows. |
| `OIDC_PROVIDER_NAME`, `OIDC_AUDIENCE` | JFrog OIDC for release-bundle and promotion flows. |

## Composite Actions

| Action | Role |
|--------|------|
| **get-version** | Mapper-style snapshot vs release version detection. |
| **stage-release-artifacts** | Read-only version-path JFrog artifact download plus checksum sidecars. |
| **build-sign-uber-jar** | Worktree build at the release source commit, GPG sign, copy to staging. |
| **publish-to-github** | Create a draft GitHub Release with staged files. |

## External Dependencies

- `aerospike/shared-workflows` v5.0.0 for reusable artifact CI/CD,
  release-bundle creation, and release-bundle promotion.
- JFrog release-bundle promotion and the organization artifact-publisher path for
  Maven Central publication.
