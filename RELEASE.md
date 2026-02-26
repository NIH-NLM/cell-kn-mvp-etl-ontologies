# Creating a Release

This document explains how to publish a new release of `cell-kn-mvp-etl-ontologies`
and what happens automatically when you do.

---

## Prerequisites

Before creating a release, confirm the following are in place:

- The `DISPATCH_TOKEN` repository secret is set.
  This is a GitHub Personal Access Token (PAT) with `repo` write scope on
  `cell-kn-mvp-etl-results`. It authorizes the ontologies repo to trigger
  the ETL pipeline in the results repo.
  ([Settings → Secrets and variables → Actions](../../settings/secrets/actions))

- The code on `main` is in a releasable state (tests pass, no pending work).

---

## How to Create a Release

1. **Go to the Releases page**
   In the GitHub UI: **Code → Releases → Draft a new release**

2. **Create a new tag**
   Click *Choose a tag*, type a new version tag, and select *Create new tag on publish*.
   Tags must follow the format `v{MAJOR}.{MINOR}.{PATCH}` — for example, `v0.3.1`.

3. **Target the `main` branch**
   Confirm the target branch is `main` (the default).

4. **Write release notes**
   Summarize what changed in this version. GitHub can auto-generate notes from
   merged pull requests using the *Generate release notes* button.

5. **Publish the release**
   Click **Publish release**. This triggers the automated workflow immediately.

---

## What Happens Automatically

Publishing a release runs the [Publish Release](.github/workflows/release.yml)
workflow, which has two sequential jobs.

### Job 1 — Publish artifacts to GitHub Packages

1. Sets the Maven version to match the release tag (e.g., tag `v0.3.1` → version `0.3.1`).
2. Builds and runs all tests (`mvn deploy` implies `mvn package`).
3. Publishes two artifacts to [GitHub Packages](../../packages):

   | Artifact | Description |
   |----------|-------------|
   | `cell-kn-mvp-etl-ontologies-{version}.jar` | Fat JAR with all Java dependencies bundled |
   | `cell-kn-mvp-etl-ontologies-{version}-bundle.zip` | ZIP containing the fat JAR + Python source files + `pyproject.toml` |

   The bundle ZIP is the primary artifact consumed by the results repo. It contains
   everything needed to run the ontology ETL pipeline:
   - The compiled Java application
   - The Python loader scripts (`OntologyParserLoader.py`, `ArangoDbUtilities.py`)
   - The Python dependency specification (`pyproject.toml`)

### Job 2 — Trigger the ETL pipeline in `cell-kn-mvp-etl-results`

Once the artifacts are published, the workflow sends a `repository_dispatch` event
to `cell-kn-mvp-etl-results` with the new version in the payload. This triggers the
combined ETL integration in the results repo even if the results code itself hasn't
changed, running:

- ArangoDB reset and OWL file download
- Ontology graph ETL (using the newly published ontologies JAR)
- Results and phenotype graph ETL
- ArangoDB archive creation
- S3 upload

---

## Versioning

Versions follow [Semantic Versioning](https://semver.org/): `MAJOR.MINOR.PATCH`.

| Change type | Example |
|-------------|---------|
| Bug fixes, small corrections | `v0.3.0` → `v0.3.1` |
| New ontologies added or existing ones updated | `v0.3.1` → `v0.4.0` |
| Breaking changes to the ETL output schema | `v0.4.0` → `v1.0.0` |

The version in `pom.xml` is a placeholder (`1.0`). The actual release version is set
by the CI workflow from the git tag at publish time and is never committed back to
the repository.

---

## Verifying a Release

After publishing, check the following:

1. **Workflow succeeded**
   Go to **Actions** and confirm the *Publish Release* workflow shows a green checkmark.
   Both jobs (`publish-release` and `dispatch-integration`) must pass.

2. **Artifacts are in GitHub Packages**
   Go to **Packages** (linked from the repo sidebar) and confirm both the `.jar` and
   `-bundle.zip` appear under the new version number.

3. **ETL was triggered in the results repo**
   In `cell-kn-mvp-etl-results`, go to **Actions** and look for an
   *ontologies-release* dispatch run that started shortly after your release.

---

## If Something Goes Wrong

**Job 1 fails (publish-release)**
The artifacts were not published. Fix the underlying issue (build error, test failure,
missing `GITHUB_TOKEN` permissions) and re-publish the release by editing it and
clicking *Update release* — this re-triggers the workflow.

**Job 2 fails (dispatch-integration)**
The artifacts were published but the ETL was not triggered. Check that `DISPATCH_TOKEN`
is set correctly in repository secrets and has `repo` write scope on
`cell-kn-mvp-etl-results`. You can re-trigger manually by dispatching from the
results repo's Actions tab directly.

**Wrong version tag**
If you published with an incorrect tag (e.g., `v0.3.` instead of `v0.3.1`), delete
the release and tag in GitHub, then create a new release with the correct tag.
