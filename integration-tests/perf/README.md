# Performance benchmarks

A repeatable measurement of how fast PipesHub indexes documents and how fast it
answers questions, and a check that tells you when either gets noticeably worse.

| File | What it is |
| --- | --- |
| `corpus.py` | Builds the synthetic documents. Same seed, same files. |
| `stack.py` | Shared plumbing: log in, create a knowledge base, upload the corpus, wait for the indexer. |
| `bench_indexing.py` | Uploads the corpus into a fresh knowledge base and times the indexing. |
| `bench_query.py` | Seeds a knowledge base, then times searches and chat turns under load. |
| `compare.py` | Judges a result against a committed baseline. Reports only. |
| `baselines/<label>.json` | Committed results, one per environment and benchmark. |
| `../../.github/workflows/perf-indexing.yml` | Runs the indexing benchmark every week on the CI stack. |
| `../../.github/workflows/perf-query.yml` | Runs the query benchmark every week on the CI stack. |

It lives in `integration-tests/` rather than `loadtest/` because it drives the
same API the integration tests do. It reuses their knowledge-base client
(`helper/clients/kb_client.py`), their login and OAuth-client bootstrap, and
their AI-model seeding, and CI already installs their dependencies. `loadtest/`
is a separate diagnostic toolkit for a person comparing two builds by hand:
flame graphs, container probes and per-phase timing. This measures the numbers
a schedule can watch; that one explains them.

## What the indexing benchmark measures

The corpus is a mix of `txt`, `md`, `html` and `csv` files, plus `docx`, `xlsx`
and text-only `pdf` files. Three quarters are under 20 KB, a fifth are 20 to 100
KB, and one in twenty is between 100 KB and 5 MB. File and folder names and the
text include accented letters, Cyrillic, Arabic, Hebrew, CJK and emoji. The
files sit in a nested folder tree. The default is 500 files from seed 1337,
about 38 MB.

The benchmark:

1. creates a knowledge base, recreates the folder tree and uploads every file
   through `POST /api/v1/knowledgeBase/{kb}/upload`, four at a time;
2. polls the knowledge base's record list every 2 seconds until every record
   reaches a final indexing status, or an hour passes;
3. deletes the knowledge base and any AI models it added.

| Measure | Meaning |
| --- | --- |
| Wall time | From the first upload to the last record finishing. |
| Throughput | Records that reached `COMPLETED`, per minute of wall time. |
| Time to indexed p50/p95/p99 | For each completed record, from its upload returning to the first poll that saw it `COMPLETED`. Accurate to about one poll interval. |
| Failures | Upload errors, records that ended `FAILED`, `EMPTY` or another non-success final status, records still unfinished at the timeout, and records the listing never showed. If only never-shown records are left five minutes after their upload (`--not-listed-grace`), the run stops early and says so, instead of waiting out the hour. The result lists up to 50 failures with the indexer's reason. |
| Peak memory | The highest `docker stats` reading for the app container, and the highest RSS of the indexing process tree inside it (read with `docker top`, so the image needs no `ps`). The app container runs all seven services, so the second figure is the one about indexing. Both need `--container`. |

Every file carries a per-run line of text, so each run uploads new bytes. The
indexer skips files whose MD5 it has already indexed, and a rerun against the
same stack would otherwise measure that shortcut.

## Running it locally

You need a running stack you can throw away. Use your own compose project name
and port so you do not touch anyone else's containers:

```bash
cd deployment/docker-compose
cp env.template perf.env        # then set passwords; COMPOSE_PROFILES=graph-neo4j; APP_PORT=3710
docker compose -p pipeshub-perf --env-file perf.env up -d
```

Create an org and skip onboarding as in the "Create organization" and "Skip
onboarding" steps of `.github/workflows/perf-indexing.yml`. Then, from
`integration-tests/` with its virtualenv active:

```bash
export PIPESHUB_BASE_URL=http://localhost:3710
export PIPESHUB_TEST_USER_EMAIL=... PIPESHUB_TEST_USER_PASSWORD=...
python perf/bench_indexing.py --docs 100 --label my-laptop \
  --ai-models existing --container pipeshub-perf-pipeshub-ai-1 \
  --summary reports/perf/summary.md
python perf/compare.py --baseline perf/baselines/my-laptop.json --current reports/perf/indexing.json
docker compose -p pipeshub-perf down -v   # when finished
```

The query benchmark takes the same arguments, plus the shape of the load:

```bash
python perf/bench_query.py --docs 40 --users 2 --duration 60 --label my-laptop-query \
  --ai-models existing --container pipeshub-perf-pipeshub-ai-1 \
  --summary reports/perf/summary.md
python perf/compare.py --baseline perf/baselines/my-laptop-query.json --current reports/perf/query.json
```

Start small locally: 40 files still have to be indexed before the first
question, and chat turns on a laptop's own model take far longer than the
provider models CI uses.

Indexing needs an LLM as well as an embedding model. With no LLM configured,
every record ends `FAILED` with the reason "Failed to process document:
'llm'". With no embedding model configured, the stack's built-in CPU model is
used, which works but is slow.

- `--ai-models seed` (the CI default) adds the test OpenAI LLM and embedding
  models for the run and removes them afterwards. It needs `TEST_OPENAI_API_KEY`.
- `--ai-models existing` uses whatever the org already has. Without a key, a
  local Ollama model works. Add it as the org's default LLM in the AI models
  settings, with endpoint `http://host.docker.internal:11434`.

The result records the LLM and embedding model that were used. `compare.py`
treats runs with different models as not comparable.

Spreadsheets (`csv`, `xlsx`) are the slowest kind by far, because the indexer
sends table rows through the LLM, up to `MAX_TABLE_ROWS_FOR_LLM` per file. On
a laptop CPU model, one spreadsheet can take a quarter of an hour. Pass
`--kinds txt,md,html,docx,pdf` to leave them out of a local run. The kinds are
recorded and compared like the other settings.

To look at a corpus without a stack: `python perf/corpus.py --docs 50 --out /tmp/corpus`.

## What the query benchmark measures

The same corpus, smaller: 120 files by default, seeded and indexed before
anything is timed. Then a fixed profile — four simulated users for five
minutes, each repeating the same cycle of operations one second apart:

| Operation | What it does |
| --- | --- |
| `search` | `POST /api/v1/search` for one of eight phrases, no filter. Twice per cycle. |
| `search_filtered` | The same, filtered to the seeded knowledge base. Once per cycle. |
| `chat` | `POST /api/v1/conversations/stream`, read to the terminal frame. Once per cycle. |

Each user starts at its own offset in the question list, so four users are not
asking the same thing at the same moment. The questions are built from the
corpus's own vocabulary, so a search has something to find; a question nothing
can answer would measure the empty-result path and look fast.

| Measure | Meaning |
| --- | --- |
| Latency p50/p95/p99 | Per operation, for the requests that succeeded. A chat turn is timed from the request to the terminal `RUN_FINISHED` frame. |
| Chat first answer frame | How long a turn takes to start answering: the first frame carrying answer text. Not the first frame of the stream — the gateway flushes one as soon as the conversation row exists, before the query service has been asked anything, so timing that would miss every change in retrieval and prompt assembly. |
| Throughput | Operations completed per minute of the load window. |
| Errors | Refused requests, streams that ended without an answer, and `RUN_ERROR` frames, grouped by reason. |
| Found something | The share of successful requests that came back with anything: a search with hits, an answer citing at least one record. A run where searches find nothing is measuring the empty-result path, however fast it looks. |
| Peak memory | The highest `docker stats` reading for the app container during the load. Needs `--container`. |

The question set, the mix, the number of users, the duration and the think time
all go into the result, and `compare.py` refuses to judge two runs that differ
in any of them.

Seeding needs the corpus indexed, so a query run costs an indexing run first.
If the indexer does not finish in `--index-timeout` (30 minutes by default), or
fewer documents are indexed than `--require-indexed` asks for (all of them by
default), the benchmark stops before asking anything and says how many were
indexed and what the rest ended as. A run that gets past that but is still short
of its baseline's corpus is reported without a verdict. Questions asked over a half-seeded
knowledge base come back empty, which is faster and counts as a success
everywhere, so an unfinished seed would otherwise read as the best run yet.

## The comparison and its thresholds

`compare.py` puts the result next to the baseline with the same label and marks
a measure as a regression when it moves past its threshold:

| Measure | Flags when | Why there |
| --- | --- | --- |
| Throughput | falls more than 20% | A whole-run rate averages out per-record jitter, so it is the steadiest number. Shared CI runners still vary by something like 10% between identical runs. 20% sits above that noise and below the size of the regressions worth catching, such as a lost worker, a stage that became serial, or an extra round trip per record. |
| Time to indexed p95 | rises more than 30% | The 95th percentile is decided by the slowest 25 of 500 records, mostly the large files, and it is only measured to within one poll. It moves more than throughput, so it gets more room. |
| Time to indexed p50 | rises more than 30% | Same reasoning as p95. |
| Wall time | rises more than 25% | Mostly tracks throughput. It shows the change in minutes a person would notice. |
| Peak indexing memory | rises more than 25% | Memory is steady from run to run, but the sampler reads it only every 5 seconds, so short spikes can be missed. |
| Failed or unfinished records | rises at all | The baseline should have none. A new failure is a correctness problem, not noise. |

For a query run it checks these instead:

| Measure | Flags when | Why there |
| --- | --- | --- |
| Search p50 and p95, filtered search p95 | rise more than 30% | Searches are short, so a shared runner's jitter is a large share of one. 30% sits above that and below a regression worth catching, such as a lost index or an extra round trip per query. |
| Chat turn p50 and p95 | rise more than 30% | Most of a turn is the model provider's own latency, which varies from week to week whatever the code does. |
| Chat first answer frame p95 | rises more than 30% | Retrieval and prompt assembly happen before the first answer frame, so this moves when our code slows down rather than the provider. |
| Throughput (operations/min) | falls more than 20% | A whole-run rate averages out per-request jitter, so it is the steadiest number here too. |
| Searches that found a hit, filtered searches that found a hit, answers that cited a document | fall at all | An empty result is fast and counts as a success, so a run that stopped finding anything improves every latency measure above. The filtered searches get their own row because a knowledge-base filter that stopped matching would be hidden by the unfiltered ones still finding plenty. Any fall here is worth a look. |
| Failed searches and chat turns | rise at all | The baseline should have none. |

The check does not fail the workflow. It writes its verdict into the job
summary and exits 0. The thresholds above are reasoned, not yet measured. Once
four or five weekly runs exist, look at how much they actually vary, adjust the
numbers, and consider passing `--fail-on-regression`. If the baseline and the
result differ in label, graph DB, broker, AI models, corpus size, seed or file
kinds, `compare.py` says so and ignores the verdicts.

## Updating a baseline, on purpose

A baseline changes only when the change is understood. Refreshing one to make
a warning go away hides the very thing this exists to catch.

1. Pick a run on `main` whose numbers you trust: a scheduled run, or a
   `workflow_dispatch` of **Indexing Performance**. Two runs that agree are
   better than one.
2. Download its `perf-indexing-<run id>` artifact and copy `indexing.json` to
   `integration-tests/perf/baselines/ci-neo4j-4cpu.json`. The file name is the
   `--label` the workflow passes.
3. Commit it on its own, and say why in the message. For example: "new
   baseline: the parser pool change makes indexing 30% faster".

Changing `--docs`, `--seed`, the corpus mix in `corpus.py` or the runner size
changes what is being measured, so it needs a new baseline in the same PR. For
the query benchmark the same is true of `--users`, `--duration`,
`--think-time` and the question set in `bench_query.py`: change any of them and
the next run will say it is not comparable until its baseline is refreshed.

The query baseline works the same way: take `query.json` from a trusted
scheduled run of **Query Performance** and copy it to
`integration-tests/perf/baselines/ci-query-neo4j-4cpu.json`.

`baselines/ci-neo4j-4cpu.json` is a placeholder for now. It holds a note
instead of numbers, so the workflow reports each run and says there is nothing
to compare with yet. Replace it with the first trusted scheduled run, as above.
`baselines/ci-query-neo4j-4cpu.json` is a placeholder too, for the same
reason: a query benchmark needs its corpus indexed first, and a laptop's CPU
model leaves that unfinished. A 50-file run on a developer laptop was tried
first and did not give a usable baseline. The machine was busy with other stacks, and the laptop's LLM ran on
its CPU. After an hour, 22 of the 47 uploaded files were indexed and the rest
were still in progress.
