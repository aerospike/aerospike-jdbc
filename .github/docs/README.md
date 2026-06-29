# CI/CD — aerospike-jdbc

Four **independent** paths: build → JFrog, JF bundle promotion, Sonatype (JF UI), GitHub draft release.

Maven Central is **not** published from this repo — JFrog PROD promotion triggers `citrusleaf/artifact-publisher` via webhook.

## Release flow

```
push stage/main (or tag)
  → Java Build & Release → artifacts + JF release bundle

manual: Promote release bundle     → DEV / TEST / STAGE (JF only)
manual: JFrog UI                   → Sonatype / Maven Central
manual: Draft GitHub release       → draft release + JF artifacts + uber jar
```

Nothing auto-promotes on push (avoids re-promoting a version already in JFrog).

## Workflows

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| **build** | PR/push to `main`; PR to `main`/`stage` | `mvn` build + test. No publish. |
| **Java Build & Release** (`push-to-stage`) | Push to `stage` or `main`; version tags; manual | **build-release**: version detect → build/sign/deploy → JF release bundle. |
| **Promote release bundle** | Manual | JF bundle lifecycle: `version` + `targets` (DEV / TEST+STAGE / all). |
| **Draft GitHub release** | Manual | Read-only JF download → build/sign **uber jar** at source commit → draft GitHub release. **No JF writes.** |
| **sonatype-approve-or-delete** | Manual | Sonatype Central Portal API (ops only). |
| **snyk-scan** | Scheduled / manual | Security scan. |

Legacy (superseded by **Promote release bundle**): `promote-to-dev.yml`, `promote-to-test-and-stage.yml`.

### Version detection (`get-version`)

| Branch | pom change | Result |
|--------|------------|--------|
| **stage** | semver bump (e.g. `2.1.2` → `2.1.3`) | snapshot: `2.1.3-SNAPSHOT` |
| **stage** | RC bump (e.g. → `2.1.3-RC1`) | release: `2.1.3-RC1` |
| **main** | semver bump | release: `2.1.3` |
| either | no pom change / untagged push | snapshot with commit suffix |
| either | release/RC tag | release (pom version) |

### Manual inputs

**Promote release bundle**

| Input | Example |
|-------|---------|
| `version` | `2.1.3` (bundle in JFrog, not `jf-build-id`) |
| `targets` | `DEV`, `TEST_AND_STAGE`, `DEV_THEN_TEST_AND_STAGE` |

**Draft GitHub release**

| Input | Example |
|-------|---------|
| `build-number` | `jf-build-id` from Java Build & Release |
| `artifact-download-repository` | JF repo for `jf rt dl` (optional if `deployedRepository` is in build info) |

Source commit for the uber jar: `buildInfo.vcs.revision`, else `buildInfo.url` → GitHub Actions run → `head_sha`.

## Secrets

| Secret | Used for |
|--------|----------|
| `GPG_SECRET_KEY`, `GPG_PUBLIC_KEY`, `GPG_PASS` | build-release; draft uber jar signing |
| `JFROG_OIDC_PROVIDER`, `JFROG_OIDC_AUDIENCE` | JFrog read/write in CI |
| `AEROSPIKE_SA_CICD_USERNAME`, `AEROSPIKE_SA_CICD_PASSWORD` | sonatype-approve-or-delete only |

## Variables

| Variable | Used for |
|----------|----------|
| `BUILD_CONTAINER_DISTRO_VERSION` | Runner image |
| `JFROG_PROJECT`, `JFROG_PLATFORM_URL` | JFrog |
| `OIDC_PROVIDER_NAME`, `OIDC_AUDIENCE` | OIDC (also mirrored in secrets where actions require them) |
| `SONATYPE_DOMAIN_NAME` | sonatype-approve-or-delete |

## Composite actions

| Action | Role |
|--------|------|
| **get-version** | Branch-aware snapshot vs release version |
| **stage-release-artifacts** | Read-only JF download + checksum sidecars |
| **build-sign-uber-jar** | Worktree build at source commit, GPG sign, copy to staging |
| **publish-to-github** | Create draft GitHub release with staged files |

## External deps

- `aerospike/shared-workflows` **v3.5.0** — `reusable_artifacts-cicd`, `reusable_create-release-bundle`, `promote-release-bundle`
- Maven Central allow-list + JFrog webhook — org `artifact-publisher` setup
