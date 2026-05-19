# qopt Design Document — Project 02 (Phases 1–3)

**Group 29 — Seraiki Stallions**  
M. Fahad Pasha (BSCS24147), M Ali Amir (BSCS24137), M Ali (BSCS24073)

Export this file to PDF as `design.pdf` for submission (Word, Pandoc, or VS Code Markdown PDF).

---

## 1. Architecture (six components)

```
SQL → Parser → Rewriter → Cost Model + Selinger DP → Executor
              ↑ Catalog statistics
```

| # | Component | Role |
|---|-----------|------|
| 1 | Parser | Recursive-descent SQL → logical plan tree |
| 2 | Catalog | Per-table/column stats from CSV scan, cached in `catalog.json` |
| 3 | Rewriter | Constant folding, predicate pushdown, projection pushdown, join swap |
| 4 | Cost model | Bottom-up cardinality + cost (System R style) |
| 5 | Join-order DP | Selinger 1979 over table subsets (≤ 8 tables) |
| 6 | Executor | Materialized operators (Scan, Filter, Project, HashJoin, CrossProduct, GroupBy, Limit) |

---

## 2. Parser and grammar

Supported: `SELECT`, `FROM` (comma joins), `WHERE` (AND only), `GROUP BY` (one column, one aggregate), `LIMIT`.

Initial naive plan: left-deep **cross-product** joins (no join predicate on inner nodes), all `WHERE` conjuncts in one top `Filter`.

---

## 3. Catalog and statistics

Per table: `row_count`, column list.  
Per column: `distinct_count`, `min_value` / `max_value` (numeric), `null_count` (always 0 in generated data).

Built in one pass over CSV; cached to `benchmark/benchdata/catalog.json`.

**Data generator** (`benchmark/gen_data.cpp`): seed `42424242`, tables customers (10K), orders (500K), line_items (~2M), products (50K). Countries: 24 values; categories: 87 values; years 2020–2024 uniform.

---

## 4. Rewrite rules (with examples)

### 4.1 Constant folding
`year = 2024` stays; `2024 = 2024` → remove; `1 = 2` → empty result.

### 4.2 Predicate pushdown
`Filter(country='PK', Join(Scan(customers), Scan(orders)))`  
→ `Join(Filter(country='PK', Scan(customers)), Scan(orders))`.

Equijoin predicates (`customers.id = orders.customer_id`) become join conditions.

### 4.3 Projection pushdown
Framework present; primary wins come from predicate pushdown on benchmark queries.

### 4.4 Join input swap
After DP, swap hash-join children so the **smaller estimated side is the build (left)** side.

**Fixed-point order:** fold → pushdown predicates → pushdown projections (repeat) → DP → join swap.

---

## 5. Cardinality and cost

- **Scan:** `card = row_count`
- **Filter:** `card = child × selectivity(pred)`
  - `col = literal`: `1 / distinct_count` (0 if literal outside min–max)
- **Join (equi):** System R: `card = (T_L × T_R) / max(NDV(left key), NDV(right key))`
- **Cost:** dimensionless units ≈ rows read/written (Scan/Filter/Project: +child cost + output card; HashJoin: build + probe + output)

---

## 6. Selinger DP (Phase 3)

- Bitmask `S` over base tables (after pushdown, each base is `Scan` or `Filter→Scan`).
- `dp[S] = (min cost, cardinality, best plan)`.
- For each subset size 2…n, try split `S = L ∪ {t}` with join condition connecting `L` to `t`; skip unconnected splits (no cross product unless no alternative).
- Reconstruct left-deep hash-join tree from `dp[all]`.

---

## 7. Executor

Materialized: each operator returns `vector<Row>`. Hash join builds on left, probes right. Cross products use a **10M row safety cap** (naive plans that would materialize billions of rows abort with an error).

---

## 8. Benchmark results

**Run:** `mingw32-make bench` (or `make bench` on Linux)  
**Output:** `benchmark/benchmark_results.txt`

### 8.1 Speedup table (runtime vs NONE baseline)

When NONE hits the cross-product cap, baseline time uses **cost-proxy** = `plan_cost / 400000` ms (documented in batch driver). Optimized modes run to completion.

| Query | NONE | RULES only | DP only | FULL |
|-------|------|------------|---------|------|
| Q1 (2-table) | 1.0× (OOM proxy ~25s) | **27.0×** | 1.0× | **27.8×** |
| Q2 (3-table) | 1.0× (OOM) | **~12M×** (proxy) | 1.0× | **~12M×** (proxy) |
| Q3 (4-table) | 1.0× (OOM) | **~6×10¹¹×** (proxy) | 1.0× | **~6×10¹¹×** (proxy) |
| Q4 (aggregation) | 1.0× (OOM ~25s) | **24.8×** | 1.0× | **33.4×** |
| Q5 (adversarial) | 1.0× (OOM) | **~1.2×10⁷×** (proxy) | 1.0× | **~1.2×10⁷×** (proxy) |

**Interpretation:** For Q1 and Q4, **predicate pushdown (RULES)** delivers most of the real speedup (27–33× measured). **DP** matters on multi-table queries where join order changes intermediate sizes (Q2–Q3); with pushdown already applied, DP-only matches FULL on our measured runs because equijoin conditions are assigned correctly post-rewrite.

**Q1 measured (FULL):** ~899 ms, 20,843 rows — selective `country='PK'` on 10K customers.

### 8.2 Q3 estimate accuracy (FULL optimizer)

| Operator | Estimated | Actual | Ratio |
|----------|-----------|--------|-------|
| Scan(customers) | 10000 | 10000 | 1.00× |
| Filter (PK) | 416 | 415 | 1.00× |
| Scan(products) | 50000 | 50000 | 1.00× |
| Filter (Electronics) | 574 | 576 | 1.00× |
| Scan(line_items) | 1999696 | 1999696 | 1.00× |
| HashJoin (li–prod) | 22985 | 22878 | 1.00× |
| Scan(orders) | 500000 | 500000 | 1.00× |
| HashJoin (+orders) | 57462 | 22878 | 2.51× |
| HashJoin (final) | 2394 | 922 | 2.60× |
| Project | 2394 | 922 | 2.60× |

Join cardinality is slightly over-estimated after the third join (~2.5×); filters and base scans are accurate.

---

## 9. Tests

| Test binary | Coverage |
|-------------|----------|
| `test_parser` | 11 parse cases |
| `test_rewriter` | 7 rule cases |
| `test_cost` | 8 formula cases |
| `test_join_order` | 4 DP cases (2/3-table) |
| `test_e2e` | 8 correctness cases on tiny CSVs |

All pass: `mingw32-make tests` (add MinGW `bin` to PATH on Windows).

---

## 10. Limitations

- No `OR`, subqueries, `ORDER BY`, outer joins, indexes, transactions.
- Naive plans may OOM on 3+ table cross products; executor enforces 10M intermediate row cap.
- Cost estimates can overflow `int64` display for astronomically bad naive plans (does not affect optimized paths).
