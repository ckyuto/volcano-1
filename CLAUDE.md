# CLAUDE.md

Guidance for Claude (and humans) working in this repo.

## Releasing and Publishing Volcano Images

### Tag naming convention

`v<major>.<minor>.<patch>-volcano<upstream_version>` — e.g. `v1.0.4-volcano1.14.0`. The `-volcano*.*.*` suffix tracks the upstream Volcano version. See `.github/workflows/release.yaml` for the exact regex.

### Step 1 — Create the git tag (triggers the GitHub release)

Pushing a tag matching `v*.*.*-volcano*.*.*` automatically runs `.github/workflows/release.yaml` (goreleaser), which creates a GitHub release.

```bash
# From the commit you want to release (usually a merge commit on internal/kjp-volcano-<minor>)
git tag -a v1.0.4-volcano1.14.0 -m "v1.0.4-volcano1.14.0"
git push origin v1.0.4-volcano1.14.0
```

### Step 2 — Build and push the scheduler image

Build and push by hand. Checkout the tag first so the build is reproducible:

```bash
git checkout v1.0.4-volcano1.14.0

# Build
docker buildx build -t "volcano-oss/vc-scheduler:v1.0.4-volcano1.14.0" . \
  --output=type=docker \
  -f installer/dockerfile/scheduler/Dockerfile \
  --platform="linux/amd64" --no-cache

# Tag for the LinkedIn registry
docker tag volcano-oss/vc-scheduler:v1.0.4-volcano1.14.0 \
  lnkdin.cr/temp/volcano-oss/vc-scheduler:v1.0.4-volcano1.14.0

# Push
docker push lnkdin.cr/temp/volcano-oss/vc-scheduler:v1.0.4-volcano1.14.0
```

### Step 3 — Build and push controller-manager and webhook-manager (when they also need bumping)

The controller and admission webhook live in a separate workflow. They are versioned independently from the scheduler — only bump them when their code has actually changed.

```bash
# Controller
docker buildx build -t "volcano-oss/vc-controller-manager:<tag>" . \
  --output=type=docker \
  -f installer/dockerfile/controller-manager/Dockerfile \
  --platform="linux/amd64" --no-cache
docker tag volcano-oss/vc-controller-manager:<tag> lnkdin.cr/temp/volcano-oss/vc-controller-manager:<tag>
docker push lnkdin.cr/temp/volcano-oss/vc-controller-manager:<tag>

# Admission webhook
docker buildx build -t "volcano-oss/vc-webhook-manager:<tag>" . \
  --output=type=docker \
  -f installer/dockerfile/webhook-manager/Dockerfile \
  --platform="linux/amd64" --no-cache
docker tag volcano-oss/vc-webhook-manager:<tag> lnkdin.cr/temp/volcano-oss/vc-webhook-manager:<tag>
docker push lnkdin.cr/temp/volcano-oss/vc-webhook-manager:<tag>
```

### Step 4 — Bump the image reference in uqm-volcano

After the image is pushed, update the `FROM` line in `uqm-volcano/<minor>/vc-scheduler.Dockerfile` (and the controller/webhook Dockerfiles if those were also bumped) to reference the new tag, and open a PR.

### Notes

- `--no-cache` is important: scheduler builds pull Go modules fresh, and stale caches have silently shipped old code before.
- The in-repo `vc-scheduler.Dockerfile` uses the image name `volcano-oss/vc-scheduler`, but the `Makefile`'s `make images` target uses `volcano/vc-scheduler`. When publishing manually, follow the workflow YAML and use `volcano-oss/`.
