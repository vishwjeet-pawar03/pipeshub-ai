# SB-4 & SB-5 — sandbox hardening + tar/symlink safety: QA report

PR: https://github.com/pipeshub-ai/pipeshub-ai/pull/3810 (stacked on #3803; commit `b26fe9c2d`).

Branch `fix/sb-4-5-sandbox-container-hardening`, stacked on `fix/sb-3-refuse-local-sandbox`
(carries SB-1 + SB-3). Live stack: compose project `vishwjeet`, image `pipeshub-ai:sb45`,
default `docker` sandbox mode, Azure `gpt-5.6-luna` + `text-embedding-3-small`.
Plan: [TEST-PLAN.md](TEST-PLAN.md).

## Verdict

**Both issues fixed; no regression found.** SB-5 (the real one) is closed in full and
verified live: a sandbox can no longer plant a symlink that makes the query host copy its
own files back in. SB-4's privilege/disk surface is hardened on every deployment shape
(incl. Helm DinD); the one invasive item (`read_only` rootfs) is deferred to a DinD-tested
follow-up because it would otherwise risk the deployment mode I can't test here.

## Results

| ID | What | Result | Evidence |
| --- | --- | --- | --- |
| U00 | sandbox/agent unit suites on the fix | 700 passed, 26 skipped; only the 5 pre-existing `[local]` contract failures (same on `main`) | `evidence/U00-unit-tests.txt` |
| U00b | full `tests/unit` fix vs branch point (SB-3) | see `evidence/U00b-full-suite-compare.txt` | `screenshots/U00b.png` |
| S1 | **SB-5 repro**: shipped `_extract_container_dir` + a symlink output tar | **LEAK**: host symlink created, next read returns `TOP-SECRET-HOST-FILE-CONTENTS` / `/etc/hostname`; fixed extractor skips it | `evidence/S1-sb5-repro.txt`, `screenshots/S1.png` |
| S2/S3 | SB-5 fix unit tests (symlink dropped, safe file kept, symlinked artifact refused) | 5 passed | `evidence/S2-S3-new-unit-tests.txt` |
| S4 | SB-5 live: `run_code` writes a normal artifact | delivered unchanged (no regression) | `evidence/S5-live-symlink.txt` |
| S5 | SB-5 live: `run_code` makes a symlink in `/output`, then artifacts are read | symlink created **inside** the sandbox but **skipped on the host** (`Skipping non-regular tar member …`); only `report.txt` returned, body = `legit artifact body` | `evidence/S5-live-symlink.txt`, `screenshots/S5-symlink-output-expanded.png` |
| H1 | SB-4 live: the actual `run_code` container's HostConfig + in-container checks | `User=sandbox`, `CapDrop=[ALL]`, `no-new-privileges`, `PidsLimit=256`, `NetworkMode=none`, `fsize=512MB`; in-container uid 1000, cannot write `/etc` | `evidence/H1-in-container.txt`, `evidence/H1-hostconfig-summary.txt`, `screenshots/H1.png` |
| H4 | SB-4 Helm: opt-in `sandbox.runtimeClassName` + NetworkPolicy | absent by default; rendered when set | `evidence/H4-helm-runtimeclass-netpol.txt`, `screenshots/H4.png` |
| H5 | SB-4 Helm: `check_chart.sh` | all variants valid | `evidence/H5-helm-check-chart.txt`, `screenshots/H5.png` |

## What changed

**SB-5 (`coding/docker.py`, `app/sandbox/docker_executor.py`, `base_executor.py`,
`artifact_upload.py`):**
- `_extract_container_dir` (both executors): extract **regular files only** (skip
  symlinks/hardlinks/devices) and pass `filter="data"`. This is the primary fix — the
  booby-trapped symlink never lands on the host.
- Host-side readers defence-in-depth: `_collect_working_dir_inputs`,
  `_promote_src_artifacts`, `_list_output_artifacts`, `list_files`,
  `base_executor.collect_artifacts` skip symlinks; file reads use a new
  `_read_file_nofollow` (`O_NOFOLLOW`); `artifact_upload._read_file_bytes` refuses a
  symlinked artifact outright.

**SB-4 (`coding/egress_firewall.py` `CONTAINER_HARDENING`, shared by every
container-create path; Helm):**
- `user="sandbox"` pinned (the firewalled setup path still overrides to root, then
  setpriv-drops) — an image/daemon default can't silently run as root.
- `fsize` ulimit 512 MB — bounds a single-file disk-fill; the dropped-privilege program
  can't raise it.
- Helm: opt-in `sandbox.runtimeClassName` (gVisor/Kata) to contain the privileged DinD
  sidecar; the deny-all-style NetworkPolicy already exists (`networkPolicy.enabled`).
- Already shipped by SB-1 and re-verified live: `cap_drop=ALL`, `no-new-privileges`,
  `pids_limit`.

## Deferred (tracked, not in this PR)

- **`read_only` rootfs** + **total-volume disk quota.** Both need writable Docker *volume*
  mounts for `/src`,`/output`,`/deps` (tmpfs breaks `put_archive` on a read-only rootfs,
  verified). The sandbox deliberately avoids mounts because host bind-mounts break under
  Helm Docker-in-Docker; daemon-managed anonymous volumes are likely DinD-safe but cannot
  be validated on this (non-DinD) host. These are defence-in-depth on top of protections
  that already exist (`cap_drop=ALL` etc.), so they wait for a DinD smoke test rather than
  risk the one deployment mode we can't see here.
- **gVisor/Kata by default** — can't be mandated (would break clusters without the
  RuntimeClass); shipped as the opt-in `runtimeClassName` above.

## Upgrade impact

No config changes required; defaults unchanged. The sandbox now pins the container user to
`sandbox` and sets a 512 MB per-file limit — both match how the stock `pipeshub/sandbox`
image already runs, so existing deployments are unaffected. A custom `SANDBOX_DOCKER_IMAGE`
must contain a `sandbox` user (the egress firewall already assumed this). No data
migration.
