# qopt — Cost-Based SQL Query Optimizer
**Advanced Database Management Systems — Project 02**

## Overview

`qopt` is a SQL query optimizer demonstrating the central algorithms every real DBMS uses:

| Component | Implementation |
|---|---|
| **Parser** | Hand-written recursive-descent (~350 lines, no parser-generator) |
| **Catalog** | Single-pass CSV statistics, hand-written JSON cache |
| **Rule Rewriter** | Constant folding, predicate pushdown, projection pushdown, join-input swap |
| **Cost Model** | System R cardinality formulas (equijoin, range selectivity, NDV) |
| **Join-Order Search** | Selinger 1979 DP over bitmask subsets — O(n²·2ⁿ) |
| **Executor** | Materialised model: Scan, Filter, Project, HashJoin, CrossProduct, GroupBy, Limit |

## Build

```bash
# Linux / WSL / MinGW (requires g++ ≥ 9 with C++17)
make

# Windows (MSVC not tested — use MinGW or WSL)
```

## Generate benchmark data

```bash
make bench          # builds gen_data, generates ~2.5M rows, runs benchmark
# OR manually:
./benchmark/gen_data benchmark/benchdata
```

## Run

```bash
./qopt --data benchmark/benchdata
```

## Interactive Commands

```
qopt> SELECT <sql>          run query — shows naive vs optimized plan + speedup
qopt> EXPLAIN SELECT <sql>  show plan tree without executing
qopt> \bench SELECT <sql>   benchmark across 4 optimizer modes
qopt> \stats                session statistics (queries, avg speedup, plan time)
qopt> LOAD <dir>            reload catalog from another data directory
qopt> \quit                 exit
```

## Supported SQL Subset

```sql
SELECT expr [AS alias] [, ...]  |  SELECT *
FROM table [, table ...]
[WHERE pred AND pred ...]
[GROUP BY column]
[LIMIT n]

expr    ::= column | table.column | aggregate(expr) | expr * expr | literal
aggregate ::= SUM | COUNT | AVG | MIN | MAX
pred    ::= column op literal  |  column op column
op      ::= = | != | < | <= | > | >=
```

## Run tests

```bash
make tests
```

## Benchmark Queries

| Q | Tables | Key Filters | Expected Win |
|---|---|---|---|
| Q1 | 2 | `country='PK'` | ≥100× (rules win) |
| Q2 | 3 | `country='PK'`, `year=2024` | ≥100× (rules + DP) |
| Q3 | 4 | `country='PK'`, `category='Electronics'` | Very large (DP critical) |
| Q4 | 2 + GROUP BY | `year=2024` | Rules win (pushdown) |
| Q5 | 3 adversarial | `year=2024`, `total>4000` | Full optimizer wins |

## Architecture

```
SQL String → Parser → Rewriter (fixed-point) → Cost Model → Selinger DP → Executor
```

No external libraries. No OR support. No OUTER JOIN. No subqueries. No transactions.

## Limitations

- No histogram-based selectivity (base project; equi-depth histograms are a bonus)
- Left-deep join trees only (bushy trees are a bonus)
- No index support; all joins use hash join or cross product
- No pipelining; fully materialised at each operator

## Files

```
src/
  plan.h/cpp        — Value, Schema, Expr, Pred, PlanNode types
  catalog.h/cpp     — CSV loader, column stats, catalog.json cache
  parser.h/cpp      — Recursive-descent SQL parser
  executor.h/cpp    — 7 materialised operators
  rewriter.h/cpp    — 4 rewrite rules + fixed-point loop
  cost_model.h/cpp  — System R cardinality + cost formulas
  join_order.h/cpp  — Selinger DP, extract_join_info, apply_join_ordering
  main.cpp          — REPL entry point
benchmark/
  gen_data.cpp      — Deterministic data generator (seed=42424242)
  run_bench.sh      — Benchmark runner (5 queries × 4 modes)
tests/
  test_parser.cpp
  test_rewriter.cpp
  test_cost.cpp
  test_join_order.cpp
  test_e2e.cpp
```
