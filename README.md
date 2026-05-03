```markdown
# qopt — Cost-Based SQL Query Optimizer
**Advanced Database Management Systems — Project 02**
**Seraiki Stallions (Group 29):** M. Fahad Pasha (BSCS24147) · M Ali Amir (BSCS24137) · M Ali (BSCS24073)

---

## Overview

`qopt` implements the core algorithms of a real DBMS query optimizer — no external libraries.

| Component | Implementation |
|---|---|
| **Parser** | Hand-written recursive-descent (~350 lines) |
| **Catalog** | Single-pass CSV stats, JSON cache |
| **Rule Rewriter** | Constant folding, predicate/projection pushdown, join-input swap |
| **Cost Model** | System R cardinality formulas (equijoin, range, NDV) |
| **Join-Order Search** | Selinger 1979 DP over bitmask subsets — O(n²·2ⁿ) |
| **Executor** | Materialised: Scan, Filter, Project, HashJoin, CrossProduct, GroupBy, Limit |

---

## Build & Test

```bash
make                # Linux / WSL / MinGW (requires g++ ≥ 9, C++17)
mingw32-make        # Windows
make tests          # Run full test suite
make clean          # Remove binaries and objects
make bench          # Generate ~2.5M rows and run benchmark
```

## Run

```bash
./qopt --data benchmark/benchdata
```

---

## Interactive Commands

| Command | Description |
|---|---|
| `SELECT <sql>` | Run query — shows naive vs. optimized plan + speedup |
| `EXPLAIN SELECT <sql>` | Show plan tree without executing |
| `\bench SELECT <sql>` | Benchmark across 4 optimizer modes |
| `\stats` | Session statistics (queries, avg speedup, plan time) |
| `LOAD <dir>` | Reload catalog from another data directory |
| `\quit` | Exit |

---

## Supported SQL Subset

```sql
SELECT expr [AS alias] [, ...]  |  SELECT *
FROM table [, table ...]
[WHERE pred AND pred ...]
[GROUP BY column]
[LIMIT n]

expr      ::= column | table.column | aggregate(expr) | expr * expr | literal
aggregate ::= SUM | COUNT | AVG | MIN | MAX
pred      ::= column op literal  |  column op column
op        ::= = | != | < | <= | > | >=
```

---

## Architecture

```
SQL String → Parser → Rewriter (fixed-point) → Cost Model → Selinger DP → Executor
```

---

## Benchmark Queries

| Q | Tables | Key Filters | Expected Win |
|---|---|---|---|
| Q1 | 2 | `country='PK'` | ≥100× (rules) |
| Q2 | 3 | `country='PK'`, `year=2024` | ≥100× (rules + DP) |
| Q3 | 4 | `country='PK'`, `category='Electronics'` | Very large (DP critical) |
| Q4 | 2 + GROUP BY | `year=2024` | Rules (pushdown) |
| Q5 | 3 adversarial | `year=2024`, `total>4000` | Full optimizer |

---

## File Structure

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
  test_parser.cpp   test_rewriter.cpp   test_cost.cpp
  test_join_order.cpp   test_e2e.cpp
```

---

## Known Limitations

- No histogram-based selectivity (equi-depth histograms are a bonus)
- Left-deep join trees only (bushy trees are a bonus)
- No index support — all joins use hash join or cross product
- Fully materialised execution; no pipelining
- No OR, OUTER JOIN, subqueries, or transactions
```
