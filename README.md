# qopt — Cost-Based SQL Query Optimizer
**Advanced Database Management Systems — Project 02 (Phase 1)**

**Seraiki Stallions (Group 29)**
* M. Fahad Pasha (BSCS24147) * M Ali Amir (BSCS24137) * M Ali (BSCS24073)

## Overview
`qopt` is a SQL query optimizer demonstrating the central algorithms used in real-world DBMS. This Phase 1 release supports parsing, catalog loading, naive logical planning, materialized execution, and a basic rewrite/cost/join-order pipeline.

**Architecture Pipeline:**
`SQL String -> Parser -> Rewriter (fixed-point) -> Cost Model -> Selinger DP -> Executor`

| Component | Implementation |
|---|---|
| **Parser** | Hand-written recursive-descent (~350 lines, no parser-generator) |
| **Catalog** | Single-pass CSV statistics, hand-written JSON cache |
| **Rule Rewriter** | Constant folding, predicate pushdown, projection pushdown, join-input swap |
| **Cost Model** | System R cardinality formulas (equijoin, range selectivity, NDV) |
| **Join-Order Search** | Selinger 1979 DP over bitmask subsets — O(n²·2ⁿ) |
| **Executor** | Materialized model: Scan, Filter, Project, HashJoin, CrossProduct, GroupBy, Limit |

## Supported SQL Subset
The optimizer supports querying CSV-backed tables, `WHERE` filtering, multi-table joins, single-aggregate `GROUP BY`, and `LIMIT` clauses.
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

## Build and Test
*(Note: Windows users should run `mingw32-make`, while Linux/WSL users can run standard `make`. Requires g++ ≥ 9 with C++17).*

Build the project and run the full test suite:
```bash
make tests
```

Clean generated binaries and objects:
```bash
make clean
```

## Generate Data and Run
Sample CSV files and cached catalog data live under `benchmark/benchdata/`. 

Generate ~2.5M rows and run the benchmark automatically:
```bash
make bench
```

Alternatively, generate data manually and start the interactive optimizer:
```bash
./benchmark/gen_data benchmark/benchdata
./qopt --data benchmark/benchdata
```

## Interactive Commands
Once `qopt` is running, use the following REPL commands:
```text
qopt> SELECT <sql>          run query — shows naive vs optimized plan + speedup
qopt> EXPLAIN SELECT <sql>  show plan tree without executing
qopt> \bench SELECT <sql>   benchmark across 4 optimizer modes
qopt> \stats                session statistics (queries, avg speedup, plan time)
qopt> LOAD <dir>            reload catalog from another data directory
qopt> \quit                 exit
```

## File Structure
* `src/`: Core optimizer components (`parser`, `catalog`, `rewriter`, `cost_model`, `join_order`, `executor`, `plan`).
* `benchmark/`: Deterministic data generator and benchmark shell runner.
* `tests/`: Unit and end-to-end correctness tests.
```
