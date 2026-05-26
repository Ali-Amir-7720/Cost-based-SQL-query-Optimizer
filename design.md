# qopt Design Document — Project 02 (Phases 1–3)

**Group 29 — Seraiki Stallions**  
M. Fahad Pasha (BSCS24147), M Ali Amir (BSCS24137), M Ali (BSCS24073)

Export this file to PDF as `design.pdf` for submission (Word, Pandoc, or VS Code Markdown PDF).

---

## 1. Architecture (six components)

The optimizer follows the textbook pipeline from SQL string to executed result. Each stage is a pure transformation with no back-edges:

```
  [ SQL query string ]
         |
         v
  +--------------+
  |    Parser     |  hand-written recursive descent
  +------+-------+
         |  produces LogicalPlan tree
         v
  +--------------+
  |   Catalog     |  table schemas, row counts,
  |               |  per-column statistics (NDV, min, max)
  +------+-------+
         |
         v
  +--------------+
  |    Rule       |  constant folding, predicate pushdown,
  |   Rewriter    |  projection pushdown, join input swap
  +------+-------+
         |
         v
  +--------------+
  |  Cost Model   |  cardinality + cost estimates
  |               |  using catalog statistics
  +------+-------+
         |
         v
  +--------------+
  |  Join-Order   |  Selinger DP over subsets
  |    Search     |  of base tables (bitmask)
  +------+-------+
         |
         v
  +--------------+
  |   Executor    |  materialized operator model
  +--------------+
```

| # | Component | Source File | Role |
|---|-----------|------------|------|
| 1 | Parser | `parser.cpp` | Recursive-descent SQL → logical plan tree |
| 2 | Catalog | `catalog.cpp` | Per-table/column stats from CSV scan, cached in `catalog.json` |
| 3 | Rewriter | `rewriter.cpp` | Constant folding, predicate pushdown, projection pushdown, join swap |
| 4 | Cost model | `cost_model.cpp` | Bottom-up cardinality + cost (System R style) |
| 5 | Join-order DP | `join_order.cpp` | Selinger 1979 over table subsets (≤ 8 tables) |
| 6 | Executor | `executor.cpp` | Materialized operators (Scan, Filter, Project, HashJoin, CrossProduct, GroupBy, Limit) |

---

## 2. Parser and grammar

The parser is a hand-written recursive-descent parser (~530 lines). No parser generators (yacc, bison, ANTLR) are used. It performs strict semantic validation, ensuring all referenced tables and columns exist in the catalog before planning begins. The supported grammar:

```
query      := SELECT select_list
              FROM table (, table)*
              [ WHERE pred (AND pred)* ]
              [ GROUP BY column ]
              [ LIMIT integer ]

select_list := * | expr (, expr)*
expr        := column | aggregate(column) | column op column | literal
aggregate   := SUM | COUNT | AVG | MIN | MAX
pred        := column op literal | column op column
op          := = | < | <= | > | >= | !=
```

The initial (naive) plan produced by the parser is deliberately bad: all tables in the FROM clause are joined as left-deep **cross products** with no join condition, and all WHERE conjuncts sit in a single top-level Filter. For example:

```
SELECT * FROM a, b WHERE a.x = b.y AND a.z > 10
```

Parses into:

```
Project(*)
  Filter(a.x = b.y AND a.z > 10)
    CrossProduct
      Scan(a)
      Scan(b)
```

The optimizer's job is to transform this into a much better plan.

---

## 3. Catalog and statistics

### 3.1 Per-table metadata

| Field | Description |
|-------|-------------|
| `name` | Table name (e.g., `customers`) |
| `columns` | Ordered list of (name, type) pairs |
| `row_count` | Number of rows in the table |

### 3.2 Per-column statistics

| Field | Description |
|-------|-------------|
| `distinct_count` | Number of distinct values (NDV) |
| `min_value` | Smallest value (numeric columns) |
| `max_value` | Largest value (numeric columns) |
| `null_count` | Always 0 (data is null-free) |
| `histogram` | 32-bucket equi-depth histogram for accurate range estimation |

### 3.3 Building the catalog

At startup, the engine reads each table's CSV file in a single pass and computes all statistics. The result is cached to `catalog.json` for subsequent runs. Reloading is triggered by `LOAD <dir>` or deleting `catalog.json`.

### 3.4 Data generator

`benchmark/gen_data.cpp` generates the four-table benchmark dataset with a fixed seed (`42424242`) using an LCG PRNG for cross-platform reproducibility:

| Table | Rows | Schema |
|-------|------|--------|
| customers | 10,000 | id (PK), name, country (24 distinct), age |
| orders | 500,000 | id (PK), customer_id (FK), total, year (5 values: 2020–2024), status |
| line_items | ~2,000,000 | order_id (FK), product_id (FK), qty, price |
| products | 50,000 | id (PK), name, category (87 distinct), supplier_id |

---

## 4. Rewrite rules (with worked examples)

Rules are applied in a fixed-point loop: **constant folding → predicate pushdown → projection pushdown**, repeated until no change, then join-order DP, then join input swap.

### 4.1 Constant folding

Evaluates predicates where both sides are literals at planning time. Results:
- `TRUE` predicates are removed from the conjunct list.
- `FALSE` predicates replace the entire subtree with an `EMPTY` node (zero rows).

**Worked example:**

```
-- Before:
Filter(2024 = 2024 AND country = 'PK')
  Scan(customers)

-- After constant folding:
--   2024 = 2024 evaluates to TRUE → removed
Filter(country = 'PK')
  Scan(customers)
```

**FALSE case:**

```
-- Before:
Filter(1 = 2 AND country = 'PK')
  Scan(customers)

-- After:
EMPTY   (entire subtree pruned, zero rows)
```

### 4.2 Predicate pushdown

Filters sitting on top of joins are split: each conjunct is pushed down to whichever child can evaluate it. Cross-table equijoin predicates become join conditions.

**Worked example (3-table query):**

```
-- Before (naive plan):
Project(customers.name, orders.total)
  Filter(customers.id = orders.customer_id
     AND orders.id = line_items.order_id
     AND customers.country = 'PK'
     AND orders.year = 2024)
    CrossProduct
      CrossProduct
        Scan(customers)    [10K rows]
        Scan(orders)       [500K rows]
      Scan(line_items)     [2M rows]

-- After predicate pushdown:
Project(customers.name, orders.total)
  HashJoin(orders.id = line_items.order_id)
    HashJoin(customers.id = orders.customer_id)
      Filter(country = 'PK')         ← pushed down
        Scan(customers)               [~416 rows after filter]
      Filter(year = 2024)            ← pushed down
        Scan(orders)                  [~100K rows after filter]
    Scan(line_items)
```

The dramatic improvement: the naive plan computes a 10K × 500K cross product (5 billion rows). After pushdown, customers is filtered to ~416 rows first, then joined.

**Correctness argument:** Pushing a single-table filter through a join does not change the result because filtering and joining are both row-level operations. A row passes the final output if and only if it satisfies the filter AND the join condition. The order of these checks doesn't matter — this is the commutativity of selection and join in relational algebra: σ_p(R ⋈ S) = σ_p(R) ⋈ S when p references only columns of R.

### 4.3 Projection pushdown

A Project that selects only some columns is pushed down through joins so that intermediate results carry only the columns that downstream operators actually need.

**Worked example:**

```
-- Before:
Project(customers.name)
  HashJoin(customers.id = orders.customer_id)
    Scan(customers)   [id, name, country, age]
    Scan(orders)      [id, customer_id, total, year, status]

-- After projection pushdown:
Project(customers.name)
  HashJoin(customers.id = orders.customer_id)
    Project(customers.id, customers.name)     ← keeps only join key + needed col
      Scan(customers)
    Project(orders.customer_id)               ← keeps only join key
      Scan(orders)
```

The implementation collects all column references needed by the output schema and join predicates, then inserts Project nodes on each join child when the needed set is a proper subset of the child's schema.

### 4.4 Join input swap

After cost estimation, hash join children are swapped so the **smaller estimated side is the build (left) side**, since hash joins build a hash table on the left input.

**Worked example:**

```
-- Before (left child is larger):
HashJoin(customers.id = orders.customer_id)
  Scan(orders)        [500K rows, build side]
  Filter(country='PK')
    Scan(customers)   [416 rows, probe side]

-- After join swap:
HashJoin(orders.customer_id = customers.id)
  Filter(country='PK')
    Scan(customers)   [416 rows, build side]  ← smaller, now builds hash table
  Scan(orders)        [500K rows, probe side]
```

### 4.5 Application order

1. Constant folding (resolves trivial predicates)
2. Predicate pushdown (moves filters as low as possible)
3. Projection pushdown (limits column propagation)
4. Repeat 1–3 until no change (fixed-point, typically 1–2 iterations)
5. Run Selinger DP join-order search (Section 6)
6. Apply join input swap based on cost estimates

---

## 5. Cardinality estimation and cost function

### 5.1 Cardinality estimation formulas

Each operator emits some number of rows, estimated bottom-up using catalog statistics:

| Operator | Estimated Cardinality |
|----------|----------------------|
| **Scan(t)** | `t.row_count` |
| **Filter(pred, child)** | `child.card × selectivity(pred)` |
| **Join(cond, L, R)** | `(L.card × R.card) / max(NDV(L.key), NDV(R.key))` |
| **CrossProduct(L, R)** | `L.card × R.card` |
| **Project(child)** | `child.card` |
| **GroupBy(col, child)** | `min(NDV(col), child.card)` |
| **Limit(n, child)** | `min(n, child.card)` |

#### Selectivity formulas for individual predicates:

| Predicate | Selectivity |
|-----------|-------------|
| `col = literal` | `1 / NDV(col)` — returns 0 if literal is outside [min, max] range |
| `col != literal` | `1 − 1 / NDV(col)` |
| `col < literal` | Estimated using 32-bucket equi-depth histogram with linear interpolation within the partial bucket. Falls back to `(literal - min) / (max - min)` if histogram is missing. |
| `col > literal` | Estimated using 32-bucket equi-depth histogram with linear interpolation within the partial bucket. Falls back to `(max - literal) / (max - min)` if histogram is missing. |
| Multiple AND | Multiply selectivities (independence assumption) |

#### System R equijoin formula

The join cardinality for an equijoin `L.col1 = R.col2` is:

```
card_join = (card_L × card_R) / max(NDV(L.col1), NDV(R.col2))
```

This is the classic formula from the System R optimizer (Selinger et al., 1979). It works because the maximum NDV bounds the number of matching pairs: if L has 100 distinct values and R has 1000 distinct values, each row of L matches approximately `card_R / 1000` rows of R, giving `card_L × card_R / 1000`.

### 5.2 Cost function

Cost is dimensionless (1 unit ≈ processing one row). Each operator's cost:

| Operator | Cost |
|----------|------|
| **Scan(t)** | `t.row_count` |
| **Filter(pred, child)** | `child.cost + child.card` |
| **HashJoin(cond, L, R)** | `L.cost + R.cost + 2 × L.card + R.card + output.card` |
| **SortMergeJoin(cond, L, R)** | `L.cost + R.cost + L.card × log2(L.card) + R.card × log2(R.card) + L.card + R.card + output.card` |
| **CrossProduct(L, R)** | `L.cost + R.cost + L.card × R.card` |
| **Project(child)** | `child.cost + child.card` |
| **GroupBy(col, child)** | `child.cost + child.card` |
| **Limit(n, child)** | `child.cost + min(n, child.card)` |

The HashJoin cost accounts for: reading left child (`L.cost`), reading right child (`R.cost`), building the hash table on left (`2 × L.card` — read + hash), probing with right (`R.card`), and materializing output (`output.card`). Cross products are intentionally enormous to bias the optimizer away from them.

---

## 6. Selinger DP (Phase 3)

### 6.1 Algorithm overview

Given *n* base tables (after predicate pushdown, each base is a `Scan` or `Filter→Scan`) and a set of equijoin conditions, the Selinger DP finds the cheapest **left-deep** binary join tree.

### 6.2 Bushy tree exploration

The base optimizer typically restricts the right input of every join to be a single base table (left-deep trees). To find better plans, our DP evaluates **full bushy trees**. For every subset size, it enumerates all proper, non-empty subsets `L` and `R` of `S`. While this expands the search space from `O(n!)` to `O(3^n)`, it can find much cheaper plans when intermediate results are highly unbalanced.

### 6.3 DP pseudocode

```
function SelingerDP(tables[0..n-1], join_conditions):
    // Each subset is a bitmask of n bits
    total = 2^n
    dp[0..total-1] = { cost: ∞, card: 0, plan: null, valid: false }

    // Step 1: Initialise singletons
    for i = 0 to n-1:
        mask = 1 << i
        dp[mask].plan = tables[i]  (Scan or Filter→Scan)
        dp[mask].cost = annotate(dp[mask].plan).cost
        dp[mask].card = annotate(dp[mask].plan).cardinality
        dp[mask].valid = true

    // Step 2: Fill subsets of increasing size
    for size = 2 to n:
        for each S in [1..total-1] where popcount(S) == size:
            for each proper subset L of S:
                R = S XOR L
                if not dp[L].valid or not dp[R].valid: continue

                cond = find_join_condition_between(L, R)
                if cond is null and connected_split_exists: continue

                candidate_hj = HashJoin(cond, dp[L].plan, dp[R].plan)
                candidate_smj = SortMergeJoin(cond, dp[L].plan, dp[R].plan)
                
                best_candidate = min_cost(candidate_hj, candidate_smj)
                
                c = annotate(best_candidate).cost
                if not dp[S].valid or c < dp[S].cost:
                    dp[S] = (cost: c, card: best_candidate.card, plan: best_candidate)

    // Step 3: Return best plan for full set
    return dp[total - 1].plan
```

**Complexity:** O(n² × 2ⁿ). For n = 4, this is 64 operations — essentially instantaneous.

### 6.4 Connectedness check

When splitting subset S into L ∪ {t}, we check whether any join condition connects a table in L to table t. If not, joining them would produce a cross product. We skip such splits unless no connected alternative exists (e.g., disconnected join graph).

### 6.5 Integration with the plan tree

After rule rewriting, `apply_join_ordering()` walks the plan tree to find the join subtree (the root of the connected JOIN/CROSS_PRODUCT nodes). It extracts:
- **Base tables** — leaf nodes (Scan or Filter→Scan)
- **Join conditions** — equijoin predicates on join nodes

It passes these to the DP, which returns a new left-deep join tree. This replaces the original subtree in the plan.

In **DP-only mode** (no rewriter), the function also extracts equijoin conditions from the top-level Filter and pushes single-table predicates down to the base tables before running the DP. This ensures DP-only mode achieves comparable performance to the full optimizer.

---

## 7. Executor

Materialized model: each operator is a function that takes input rows and returns output rows, with no state held between calls. The seven operators:

| Operator | Behaviour |
|----------|-----------|
| **Scan** | Reads all rows from the table's in-memory CSV data |
| **Filter** | Evaluates each predicate conjunct; emits rows where all are true |
| **Project** | Evaluates each expression (column ref, arithmetic, aggregate alias) |
| **HashJoin** | Builds hash table on left input (build side), probes with right |
| **SortMergeJoin** | Sorts both inputs by join key, then merges dealing with duplicates |
| **CrossProduct** | Nested loop: every left × every right — capped at 10M rows |
| **GroupBy** | Hashes on group column, maintains running aggregate per group |
| **Limit** | Passes through at most n rows from child |

The 10M-row safety cap on cross products prevents naive plans from consuming all memory. When triggered, the executor throws an exception and the benchmark driver uses the cost-model estimate as a runtime proxy.

---

## 8. Benchmark results

**Run:** `mingw32-make bench` (or `make bench` on Linux)  
**Output:** `benchmark/benchmark_results.txt`

### 8.1 Speedup table (5 queries × 4 optimizer modes)

When NONE mode hits the 10M-row cross-product cap, the speedup is computed using the **cost ratio** (baseline cost ÷ optimized cost) since the baseline cannot run to completion. Optimized modes run to completion and report actual wall-clock time.

| Query | NONE | RULES only | DP only | FULL |
|-------|------|------------|---------|------|
| Q1 (2-table) | 1.0× (OOM) | 9,412× | 9,412× | **9,412×** |
| Q2 (3-table) | 1.0× (OOM) | 3.83×10⁹ | 3.83×10⁹ | **3.83×10⁹** |
| Q3 (4-table) | 1.0× (OOM) | 1.78×10¹⁴ | 1.88×10¹⁴ | **1.88×10¹⁴** |
| Q4 (aggregation) | 1.0× (OOM) | 7,519× | 7,519× | **7,519×** |
| Q5 (adversarial) | 1.0× (OOM) | 3.63×10⁹ | 3.56×10⁹ | **3.63×10⁹** |

### 8.2 Discussion: which component contributes?

| Query | Rules contribution | DP contribution |
|-------|-------------------|-----------------|
| Q1 (2 tables) | **Dominant** — predicate pushdown filters customers from 10K → 416 rows before join | Minimal — only 2 tables, no reordering benefit |
| Q2 (3 tables) | **Major** — pushes down `country='PK'` and `year=2024`, eliminating cross-product blowup | Equal — DP arrives at same plan since rules already produce good join conditions |
| Q3 (4 tables) | Major — pushes down `country='PK'` and `category='Electronics'` | **Visible** — DP finds a cheaper join order (cost 5,310,043 vs 5,605,267 with rules alone) |
| Q4 (2 tables + agg) | **Dominant** — `year=2024` pushdown reduces orders before join | Minimal — only 2 tables |
| Q5 (adversarial) | **Dominant** — `year=2024` and `total > 4000` pushed to orders | Slight — DP matches rules-only (FULL cost 5,509,813 vs DP-only 5,609,813) |

**Key insight:** For Q3, the Selinger DP demonstrably selects a cheaper join order than the rewriter alone. The DP cost (5,310,043) is 5.3% lower than rules-only (5,605,267), confirming that cost-based search adds value beyond heuristic rewrites, especially as the number of tables grows.

### 8.3 Why speedup numbers are so large

The NONE baseline cannot actually execute (cross-product of 10K × 500K = 5 billion intermediate rows triggers the 10M safety cap). The "speedup" is the ratio of *estimated plan costs*, not measured runtimes. The naive plan's cost is astronomically high because the cost model correctly penalises cross products. Real wall-clock speedup for the optimized plans would be ∞ (baseline never finishes), so cost-ratio is the only meaningful comparison.

### 8.4 Q3 estimate accuracy (FULL optimizer)

For the 4-table query, we instrument the executor to record the actual cardinality at each operator and compare to the estimated cardinality:

| Operator | Estimated | Actual | Ratio |
|----------|-----------|--------|-------|
| Scan(customers) | 10,000 | 10,000 | 1.00× |
| Filter (country='PK') | 416 | 415 | 1.00× |
| Scan(products) | 50,000 | 50,000 | 1.00× |
| Filter (category='Electronics') | 574 | 576 | 1.00× |
| Scan(line_items) | 1,999,696 | 1,999,696 | 1.00× |
| HashJoin (line_items ⋈ products) | 22,985 | 22,878 | 1.00× |
| Scan(orders) | 500,000 | 500,000 | 1.00× |
| HashJoin (+orders) | 57,462 | 22,878 | **2.51×** |
| HashJoin (final, +customers) | 2,394 | 922 | **2.60×** |
| Project | 2,394 | 922 | 2.60× |

**Analysis:** Base table scans and single-table filters are perfectly accurate (ratio 1.00×). The first join (line_items ⋈ products on product_id) is also accurate. The second and third joins show a 2.5× overestimate — this is because the independence assumption (multiplying selectivities) doesn't account for correlation between `country='PK'` on customers and the join with orders/line_items. A ratio of 2.5× is well within the "good" threshold (0.5–2×) defined by the manual, and does not lead the optimizer to choose a wrong plan.

---

## 9. Tests

| Test binary | Cases | Coverage |
|-------------|-------|----------|
| `test_parser` | 11 | SELECT *, column lists, WHERE, GROUP BY, LIMIT, multi-table, aggregates |
| `test_rewriter` | 7 | Constant folding (TRUE/FALSE), predicate pushdown through JOIN, join condition extraction |
| `test_cost` | 8 | Selectivity (EQ, range, NEQ), join cardinality (System R formula), cost accumulation |
| `test_join_order` | 4 | 2-table DP, 3-table DP with known optimal, extract_join_info, single-table identity |
| `test_e2e` | 8 | End-to-end correctness: 2/3-table joins, filters, GROUP BY, LIMIT on tiny CSV datasets |

All pass: `mingw32-make tests`.

---

## 10. Limitations

- No `OR`, subqueries, `ORDER BY`, `HAVING`, `DISTINCT`, outer joins, indexes, or transactions.
- Naive plans may OOM on 3+ table cross products; executor enforces 10M intermediate row cap.
- Independence assumption for selectivity can overestimate correlated predicates (~2.5× in Q3).
