# Answer-quality evals

Two kinds of check live here.

**Structural, free, on every pull request.** `test_golden_assertions.py` and
`test_live_harness_reporting.py` run in CI (`unit-test.yml`) with no model, no
key and no network. They check the harness itself and the shape of each case.

**Live, scheduled, costs money.** `answer-quality-evals.yml` runs the golden
cases against a real model: the **nightly** set every night at 02:00 UTC, the
**full** set on Sunday at 03:00 UTC, and either on demand. This is what notices
that a prompt or model change made the agent behave worse.

## What a live run measures, and what it does not

Each case gives the agent a question and a set of tools, then asks: which tool
did it reach for first, did it call `final_answer` more than once, did it try to
write something without asking, did it claim high confidence when a source was
missing?

The tools are stubs that return fixed text (`live_runner.py`). The cases are
about the agent's *choices*, so a run needs a real model but no stack, no data
and no seeding. That keeps a nightly run to a handful of model calls, and keeps
"did behaviour change?" from depending on whether a corpus indexed correctly.

It follows that a passing run says nothing about retrieval quality, citation
correctness, or answers over real customer-shaped data. Those are covered by the
integration and browser tests, which run against the full stack.

## Running one yourself

```bash
cd backend/python
TEST_OPENAI_API_KEY=sk-... python -m tests.evals.run_scheduled_evals \
    --set nightly \
    --output reports/evals/nightly.json \
    --summary reports/evals/summary.md \
    --baseline tests/evals/baselines/nightly.json
```

`--set full` runs everything. `--model` and `EVAL_PROVIDER`/`EVAL_MODEL` pick a
different model; `--fail-on-regression` turns a fall in pass rate into a
non-zero exit.

The run exits non-zero when it measured nothing: no key, no model, no cases, or
every case skipped. A run that tested nothing must not look like a run that
passed — that bug existed here once already.

## Sets

`case_sets.py` decides which cases run nightly. The nightly set is the cheap
tripwire — wrong tool choice, and writing without being asked. Everything runs
weekly.

Adding a case to `GOLDEN_CASES` puts it in the full set automatically; add its
id to `NIGHTLY_CASE_IDS` if it belongs in the nightly set too.

**There are only four cases today.** That is enough to catch a gross regression
and not enough to call this coverage. Good next cases, in rough order of value:

- an answer that must cite the document it came from, and must not cite one it
  did not use;
- a question whose answer is in a document the asker may not see — the answer
  must not leak it;
- a question the corpus cannot answer — "I don't know" beats a confident guess;
- a follow-up that depends on the previous turn, to catch context loss.

## Costs

`cost.py` prices a run from the tokens the agent runtime counted, using a table
of published prices with the date they were read (`PRICES_AS_OF`). An unlisted
model is reported as "not known" rather than free, and a run that used no tokens
is not priced at zero.

The job summary shows what the run cost and what that is per month on its own
schedule.

## Baselines

`baselines/nightly.json` and `baselines/full.json` hold the run a new run is
compared against. Both are placeholders until the first real run: with the AI
account out of credit when this landed, there were no numbers to commit.

To adopt a run as the baseline, download its `answer-quality-<run id>` artifact
and copy its JSON over the matching baseline file. Only runs of the same case
set, provider and model are compared; anything else is shown without a verdict,
the same rule `integration-tests/perf/compare.py` uses for the performance
benchmarks.
