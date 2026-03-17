# Alignment with shared-workflows CICD standard

This repo uses the [shared-workflows CICD standard](https://github.com/aerospike/shared-workflows/blob/main/.github/workflows/docs/CICD-standard.md) and the [Artifacts CI/CD orchestrator](https://github.com/aerospike/shared-workflows/blob/main/.github/workflows/artifacts-cicd/README.md).

## Approach

The orchestrator (`reusable_artifacts-cicd.yaml`) accepts a **build-script** and does not expose separate `setup-java` inputs. So we do **Java and Maven setup inside the build-script** (install Temurin 8 and Maven if needed, then run the Maven build and copy artifacts into `build/`). That matches the example pattern where the script can do anything (e.g. the example uses `make` and `package.sh`; we use in-script Java setup + Maven).

## Current workflow shape

- **extract-version** – Uses existing `get-version` action; outputs `version` and `is-snapshot`.
- **artifacts-cicd** – Single job calling `reusable_artifacts-cicd.yaml` with:
  - `version`, `gh-source-path`, `working-directory`, `gh-artifact-directory: build`, `jar-group-id: com.aerospike`
  - `build-env`: `VERSION` and `IS_SNAPSHOT` (from extract-version) for use in the script
  - `build-script`: install Java 8 (Temurin) and Maven if needed, start Aerospike, run Maven, copy artifacts to `build/`
  - OIDC and JFrog project/config
- **create-release-bundle** – Calls `reusable_create-release-bundle.yaml` with `jf-build-names: aerospike-jdbc:${{ needs.artifacts-cicd.outputs.jf-build-id }}` (orchestrator uses a timestamp as build id).

## Promote to Prod: build number

With the orchestrator, the JFrog build id is a **timestamp** (generated inside the orchestrator), not the GitHub Actions run number. When you run **Promote to Prod**, use the **jf-build-id** from the **Build and release** workflow run (exposed as the workflow output `jf-build-id`) as the build number to promote.

## Optional: pin to a tag

Shared-workflows is currently pinned by commit SHA. You can switch to a released tag (e.g. `v2.0.3`) and set `gh-workflows-ref` to the same value everywhere for easier upgrades.
