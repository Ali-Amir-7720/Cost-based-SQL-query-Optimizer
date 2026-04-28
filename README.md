# Cost-Based SQL Query Optimizer

**Group 29**  
M. Fahad Pasha (BSCS24147)  
M Ali Amir (BSCS24137)  
M Ali (BSCS24073)

## Phase 1 Scope
This repo contains the phase 1 version of our SQL optimizer. We support parsing, catalog loading, naive logical planning, materialized execution, and the basic rewrite/cost/join-order pipeline used by the later phases.

## What It Runs
- `SELECT` queries over CSV-backed tables
- `WHERE` filters
- joins across multiple tables
- `GROUP BY` with a single aggregate
- `LIMIT`
- `EXPLAIN` and benchmark-style comparisons

## Build And Test
From a Windows shell with `mingw32-make` available:

```bash
mingw32-make tests
```

That builds the project and runs the full test suite. To clean generated binaries and objects:

```bash
mingw32-make clean
```

## Data
Sample CSV files and cached catalog data live under `benchmark/benchdata/`.
