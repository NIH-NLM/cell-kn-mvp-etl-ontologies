# Creating a Release

This document explains how to publish a new release of `cell-kn-mvp-etl-ontologies`
and what happens automatically when you do.

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
workflow, which:

1. Sets the Maven version to match the release tag (e.g., tag `v0.3.1` → version `0.3.1`).
2. Builds and runs all tests.
3. Publishes two artifacts to [GitHub Packages](../../packages):

   | Artifact | Description |
   |----------|-------------|
   | `cell-kn-mvp-etl-ontologies-{version}.jar` | Fat JAR with all Java dependencies bundled |
   | `cell-kn-mvp-etl-ontologies-{version}-bundle.zip` | ZIP containing the fat JAR + Python source files + `pyproject.toml` |

4. Prints a link to `cell-kn-mvp-etl-results` in the workflow log so you can
   create the follow-up results release manually (see below).

---

## Next Step — Release the Results Repo

Once the workflow completes, create a release in `cell-kn-mvp-etl-results` to run
the combined ETL pipeline (ontology graph, results graph, ArangoDB archive, S3 upload).

**Before releasing `cell-kn-mvp-etl-results`**, check that the `ontologies.version`
property in its `pom.xml` matches the ontologies version you just published. Update it
if needed, then create the results release:

> https://github.com/NIH-NLM/cell-kn-mvp-etl-results/releases/new

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

2. **Artifacts are in GitHub Packages**
   Go to **Packages** (linked from the repo sidebar) and confirm both the `.jar` and
   `-bundle.zip` appear under the new version number.

---

## If Something Goes Wrong

**Workflow fails (build or test error)**
Fix the underlying issue and re-publish the release by editing it and clicking
*Update release* — this re-triggers the workflow.

**Wrong version tag**
If you published with an incorrect tag, delete the release and tag in GitHub, then
create a new release with the correct tag.
