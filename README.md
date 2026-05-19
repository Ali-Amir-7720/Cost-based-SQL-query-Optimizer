# qopt — Cost-Based SQL Query Optimizer

**Advanced Database Management Systems — Project 02 (Phases 1–3 complete)**

**Seraiki Stallions (Group 29)**  
M. Fahad Pasha (BSCS24147) · M Ali Amir (BSCS24137) · M Ali (BSCS24073)

## Overview

`qopt` is a cost-based SQL query optimizer over CSV-backed tables. It implements the full pipeline from the course manual:

`SQL → Parser → Rewriter → Cost Model → Selinger DP → Executor`

| Phase | Deliverable |
|-------|-------------|
| **1** | Parser, catalog, materialized executor, naive plans, interactive shell |
| **2** | Four rewrite rules, cost model, `EXPLAIN` with estimates |
| **3** | Selinger join-order DP, 4 optimizer modes, benchmark Q1–Q5 |

## Build (Windows)

Requires **g++** (MinGW-w64) on PATH:

```bash
mingw32-make
mingw32-make tests
mingw32-make bench
```

On Linux/WSL use `make` instead of `mingw32-make`.

## Run interactively

```bash
./qopt --data benchmark/benchdata
```

Commands: `SELECT …`, `EXPLAIN SELECT …`, `\bench SELECT …`, `\stats`, `LOAD <dir>`, `\quit`

## Benchmark

- Generator: `benchmark/gen_data.cpp`
- Batch runner: `benchmark/run_bench_batch.cpp` (invoked by `make bench`)
- Results: `benchmark/benchmark_results.txt`, summary: `benchmark/speedup_summary.md`

## Design document

See **`design.md`** (export to **`design.pdf`** for submission).

## Project layout

| Path | Purpose |
|------|---------|
| `src/` | Parser, catalog, rewriter, cost model, join_order, executor, main |
| `tests/` | Unit + e2e tests |
| `benchmark/` | Data generator, bench driver, results |
| `manual (2).pdf` | Course specification |

## GitHub progress

See **`GITHUB_PUSH.md`** for ordered commits to push phase-by-phase.
