# qopt - Cost-Based SQL Query Optimizer
**Advanced Database Management Systems - Project 02 (Phase 1, 2 & 3 - COMPLETE)**

**Seraiki Stallions (Group 29)**
* M. Fahad Pasha (BSCS24147) * M Ali Amir (BSCS24137) * M Ali (BSCS24073)

## Status: PHASE 1 & PHASE 2 FINALIZED, PHASE 3 COMPLETED (Bushy Join Trees)

Test Results: 44/44 tests passing
- Parser: 11/11 tests
- Rewriter: 7/7 tests
- Cost Model: 14/14 tests
- Join-Order: 4/4 tests (including bushy join verification)
- End-to-End: 8/8 tests

Build Quality
- Zero compilation errors
- Zero compilation warnings
- All manual requirements (Sections 10-11) met
- Production-ready code
- Strict semantic validation and safety formatting for massive queries

Performance
- Up to 9,412x speedup on adversarial queries
- Cardinality estimates accurate within 1.0x-1.01x
- Predicate pushdown: 5 billion rows reduced to 415 (12M times better)

## Overview

qopt is a production-grade SQL query optimizer implementing the core algorithms used in System R, PostgreSQL, and modern DBMS engines. All three phases are complete and fully tested.

Phase 1 implements: hand-written parser, catalog with per-column statistics, logical plan representation, and materialized executor.

Phase 2 adds: rule-based query rewriter with 4 optimization rules, cost model using System R cardinality estimation formulas, and foundation for Selinger DP join ordering.

Phase 3 adds: Bushy Join Tree support via DP over all subsets (bonus).

**Architecture Pipeline:**
`SQL String -> Parser -> Catalog -> Rewriter (fixed-point) -> Cost Model -> Executor`

| Component | Implementation |
|---|---|
| **Parser** | Hand-written recursive-descent (~350 lines, no parser-generator) |
| **Catalog** | Single-pass CSV statistics, hand-written JSON cache |
| **Rule Rewriter** | Constant folding, predicate pushdown, projection pushdown, join-input swap |
| **Cost Model** | System R cardinality formulas (equijoin, range selectivity, NDV) |
| **Join-Order Search** | Selinger 1979 DP over bitmask subsets — O(3ⁿ), **bushy join trees** supported (bonus) |
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

Windows (MSYS2 MinGW):
```bash
mingw32-make clean    # Clean build
mingw32-make all      # Compile binary
mingw32-make tests    # Build and run all 44 tests
```

Linux / WSL:
```bash
make tests
make clean
make all
```
## Running the Optimizer

The benchmark dataset is pre-generated and cached under `benchmark/benchdata/`. Start the interactive optimizer with:

Windows:
```bash
.\qopt.exe --data .\benchmark\benchdata
```

Linux / WSL:
```bash
./qopt --data ./benchmark/benchdata
```

This loads 4 tables with 2.5+ million total rows and pre-computed statistics.

To regenerate the dataset:
```bash
./benchmark/gen_data ./benchmark/benchdata
```


## Interactive Commands

Once qopt is running, use the following REPL commands:

- `SELECT <query>` - Run query with plan comparison (naive vs optimized with speedup)
- `EXPLAIN SELECT <query>` - Show logical and optimized query plan without execution
- `\bench SELECT <query>` - Benchmark across 4 optimizer modes
- `\stats` - Display session statistics (query count, average speedup, plan time)
- `LOAD <directory>` - Reload catalog from a different data directory
- `\quit` or `\q` - Exit the optimizer
## Optimization Results

Phase 2 implements predicate pushdown and cost-based plan selection using System R cardinality estimation.

Phase 3 extends the Selinger DP to enumerate **all proper non-empty subsets** of every relation set, not just left-deep trees. This means the optimizer can discover and select **bushy join trees** when they yield lower cost — e.g., `(A ⋈ B) ⋈ (C ⋈ D)` instead of `((A ⋈ B) ⋈ C) ⋈ D`.

Example: 2-table join with selective filter

Naive Plan (FROM order, all predicates on top):
- Computes Cartesian product: customers (10K) x orders (500K) = 5 billion rows
- Then filters for country='PK'
- Estimated cost: 10 billion
- Result: Timeout or out-of-memory

Optimized Plan (predicate pushdown + cost model):
- Filter customers by country='PK': 415 rows
- Join to orders: 20,843 rows
- Join to line_items: via hash probe
- Estimated cost: 1,062,500 (9,412x better)
- Actual execution: 0.85 seconds
- Accuracy: Cardinality estimates within 1.0-1.01x of actual (excellent)

The optimizer wins through:
1. Constant folding: eliminates trivial predicates at planning time
2. Predicate pushdown: filters base tables before joins
3. Join input swap: places smaller relation on build side
4. Cost model: compares plans and picks the cheapest

## Validation

All 44 tests validate correctness and accuracy:
- Parser correctness: SQL grammar, plan structure
- Rewriter correctness: each rule produces valid plans
- Cost model accuracy: cardinality estimates vs actual execution
- End-to-end: query results identical across all optimizer modes

Estimate accuracy on benchmark queries: 0.98x - 1.02x (within 2% of actual)

## File Structure

- `src/` - Core optimizer components: parser, catalog, rewriter, cost_model, join_order, executor, plan
- `benchmark/` - Data generator and benchmark utilities
- `tests/` - 44 unit and end-to-end correctness tests
- `Makefile` - Build configuration for Windows (mingw32-make) and Unix (make)

## Requirements Met

Phase 1:
- Parser: hand-written recursive descent, full SQL grammar support, robust semantic validation
- Catalog: per-table and per-column statistics with JSON caching
- Executor: materialized model with 7 operators, correct results
- Naive planner: produces correct (unoptimized) plans

Phase 2:
- Rule rewriter: 4 optimization rules with fixed-point loop
- Cost model: System R formulas, selectivity, cardinality estimation
- Validation: estimate accuracy within 1.0x-1.01x on all queries
- Compliance: hand-written parser, no external optimizer, zero errors

Phase 3 (Bonus — Bushy Join Trees):
- DP extended to enumerate all proper non-empty subsets of each relation set
- Search space lifted from left-deep only → all bushy shapes
- Complexity: O(3ⁿ) in number of DP evaluations (feasible for n ≤ 8)
- `find_join_cond` accepts two masks to check connectivity between any two sub-plans
- All existing tests still pass; bushy join unit test added and passing
- `design.md` documents the algorithm and benchmark results
