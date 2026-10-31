# QA engine specification

This is the contract every implementation change must keep. Section numbers are
referenced from tests and reviews.

## 1. Invariants

| # | Invariant | Enforced by |
| --- | --- | --- |
| I1 | Every floor test is in the final plan, whatever the model returns | `validate.merge`; `test_validate.test_floor_survives_a_model_that_ignores_it` |
| I2 | A model error, timeout, non-JSON or schema-invalid reply means floor only, and the report says so | `planner.run`, `validate.merge`; `test_model_failure_means_floor_only`, `test_malformed_output_is_a_failure` |
| I3 | A model addition must be a catalog id of this run's edition, not quarantined, not planned | `validate.merge`; `test_unknown_ids_are_dropped_and_listed`, `test_overlay_id_in_a_base_run_is_dropped` |
| I4 | The budget trims model additions by ascending confidence, never the floor | `validate.merge`; `test_budget_trims_low_confidence_additions_not_the_floor` |
| I5 | Every planned test ends with an outcome; `not_run` always carries a reason | `run.run`; `test_missing_secret_is_not_run_not_failed`, `test_stack_tests_without_a_stack_are_not_run` |
| I6 | Teardown runs on success, failure, timeout and SIGTERM | `run.run` `finally`, `stack.Stack.stop` |
| I7 | A PR cannot weaken its own selection: rules come from the target branch, and an engine or rules change runs everything | `qa-pipeline.yml` "Pick the engine" (the target branch's engine and rules, checked out into `qa-engine/`); `always.toml` `[[full_run]]` |
| I10 | The committed catalog matches the tests | `catalog check` in `main.yml` |
| I11 | Model text in PR comments has no mentions, markup or links, and is at most 200 characters per reason | `validate.clean_text`; `test_model_text_cannot_inject_markup_or_mentions` |
| I12 | `/qa` arguments match fixed patterns; permission comes from the GitHub API; fork code runs with secrets only through an admin's `--trusted` | `command.parse`, `qa-command.yml`; `test_untrusted_arguments_are_rejected` |

## 2. Pipeline and contracts

```text
collect ──changeset.json──▶ impact ──plan-input.json──▶ plan ──plan-llm.json──▶ validate ──plan.json──▶ run ──results*.json──▶ report
             changeset.diff        (+ catalog rows)          (model adapter)                                   (+ junit, logs)
```

JSON Schemas live in `scripts/qa/schema/`. `test_contracts.py` checks every
producer against them, and `validate` checks model output against
`plan-llm.v1.json`.

| File | Schema | Key fields |
| --- | --- | --- |
| `tests.jsonl` rows | `catalog-row.v1.json` | `id` = `<prefix>:<path relative to the suite cwd>[::Class::test]`; prefixes `py`, `it`, `node`, `fe`, `e2e`, `scripts`, `suite`; `edition`; `tc`, `title`, `covers`, `needs`, `markers`; `status` active / planned / quarantined; `ordered` |
| `changeset.json` | `changeset.v1.json` | base, head, first-parent commits, files with status / edition / kind, zero-context hunks |
| `plan-input.json` | `plan-input.v1.json` | `floor.items[id].why` (up to 3 reasons) and `grep`; `full_suites`; `stack`; `matrix`; `deferred`; `unmapped`; `missing`; `candidates` (up to 300); `catalogs`; `budget` |
| `plan-llm.json` | `plan-llm.v1.json` | `add[{id, reason, confidence}]`, `add_matrix`, `missing`, `risk`; `_planner` status added by the adapter runner |
| `plan.json` | `plan.v1.json` | `tests[{id, source floor\|model, why, grep}]`; `planner`; `stack{needed, graph_db[], broker, compose_profiles[]}`; `dropped`, `budget_trimmed` |
| `results*.json` | `results.v1.json` | `tests[{id, outcome, seconds, details}]`, `invocations[{label, cmd, exit, log}]`, `fatal`, `scope` |

## 3. Selection algorithm (the floor)

For a change set and profile, the floor is the union of:

1. **Full runs.** The `full` profile, more than `max_files` changed files, a
   `[[full_run]]` path, or an edition switch file. A switch file covers the unit
   suites of both editions under the same `cwd`.
2. **Changed tests.** Every catalog row in a changed file.
3. **Rules.** Each `[[rule]]` whose `paths` match adds its `tests` globs,
   integration `markers`, `it_all`, `suites`, `matrix`, `graph_db` and
   `broker`. `names` expands one rule per name with `{name}` substituted.
4. **Covers.** Rows whose `covers:` globs match a changed file.
5. **Mirrors.**
   - `src/x/y.py` maps to `tests/unit/x/test_y.py`, and `src/x/y.ts` to
     `tests/x/y.test.ts`. The suite whose edition matches the file wins.
   - With no mirror test, tests in the mirror directory run as a safety net.
     The file stays unmapped and becomes a missing unit test, unless it only
     declares things (types, constants, `__init__`, `index`).
6. **Import graphs.**
   - Python: an `ast` walk over `backend/python/app`, plus test files. It
     counts `mock.patch("app....")` strings as imports.
   - TypeScript: relative imports and the frontend's `@/` aliases.
   - Tests within `import_depth` hops of a changed module are selected:
     fast 1, standard 2, conservative 4.
7. **Always.** Each `[[always]]` selector whose `when` matches; e2e smoke is
   passed as `--grep @smoke`.
8. **History.** Tests failing on `main` in the last three runs.

After the union:

- A profile with `integration = false` moves stack-needing integration tests
  to `deferred`, which is listed but not run.
- Branch overrides (`oss-sync-*` gives full, `release/*` conservative) only
  raise the profile.
- Gaps: missing mirrors, new connectors with no `integration-tests/connectors/<name>/`,
  and new `*.routes.ts` files.

## 4. Edition model

- **Overlay.** `scripts/qa` is the engine. A repository can add a rule overlay in
  `scripts/<name>/qa`: suites, impact rules, always rules, quarantine, planner
  notes, and a `qa_hooks.py` that classifies paths into editions. Overlay lists
  extend the shared ones and overlay tables update them. Overlays are
  discovered as `scripts/<name>/qa` directories with a `rules/` dir; the engine
  names none.
- **Edition of a path.** The overlay's `edition(path)` hook names the edition; a
  switch file is `switch`; everything else is the base edition.
- **Suite ownership.** When suites tie on a file, the one whose edition
  matches the file's wins.
- **Scope.** `--edition` limits which catalog rows the model sees; rows of an
  edition outside the run are dropped before the model sees them.
- **Catalogs.** `integration-tests/catalog/` holds the base rows; an overlay
  keeps its own rows in a separate catalog directory.

## 5. Runner

- **Families.**

  | Family | Runs | Result granularity |
  | --- | --- | --- |
  | `pytest` (backend) | `python -m pytest <files or suite root>` | per test file |
  | `it-pytest` (`integration-tests/`) | `python -m pytest <node ids>` | per test |
  | `mocha` | a generated `--config` with an explicit `spec` list, xunit reporter | per file, falling back to the invocation |
  | `vitest` | `vitest run <files>` | per file, falling back to the invocation |
  | `playwright` | `playwright test <files> [--grep]` | per file, falling back to the invocation |
  | `npm-script` | `npm run <script>` | invocation |
  | `unittest` | `python -m unittest discover -s scripts` | invocation |

  For `it-pytest`, a selected test in an ordered lifecycle module
  (`pytest.mark.order`) runs the whole module.
- **Stages.**
  1. Resolve.
  2. Preflight. A missing toolchain makes the test `not_run`; a missing `env:`
     need makes it `not_run` unless `PIPESHUB_REQUIRE_CONNECTOR_SECRETS`; a
     managed stack also checks docker, disk and ports.
  3. Execute the unit invocations.
  4. Per graph DB: compose up (managed), health, seed
     (`integration-tests/stack/seed.py`, idempotent), the stack invocations,
     logs, then teardown.
  5. Retry: failed pytest ids are retried once; a pass on retry is `flaky`.
- **Gate.**
  - `fail` / `error` on a test that is not quarantined blocks.
  - `flaky`, `skip` and `not_run` are reported and do not block. `--strict`
    makes `not_run` block.
  - With no results (shadow mode), the verdict is `plan-only` and passes.

## 6. Culprit search

`bisect --range A..B --tests FILE`:

1. Re-run the failing ids at the head twice. If they now pass, the result is
   `flaky`.
2. Narrow to first-parent commits whose own floor (conservative profile)
   includes a failing id. If none qualify, use the whole range.
3. Bisect with probes in detached worktrees, at most `--max-steps` probes.
4. The result is `exact` with a culprit, or `candidates`.

## 7. Not yet built (tracked in the implementation plan)

- **`/qa draft-tests`** (P5): a write-enabled planner whose drafts are kept
  only if they compile, pass 3 runs and raise diff coverage (TestGen-LLM
  filters).
- **Diff coverage per changed line** (P4): `changeset.hunks` is already
  recorded for it.
- **Nightly miss attribution as a scheduled job.** `history.misses` and
  `culprit.search` are the building blocks. Storing history (orphan branch or
  S3) is an open decision.
- **Shadow comparison** against the existing workflows' JUnit artifacts
  ("would have missed").
