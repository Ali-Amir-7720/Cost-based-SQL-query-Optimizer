# GitHub push guide — show progress phase-by-phase

Repository: `https://github.com/Ali-Amir-7720/Cost-based-SQL-query-Optimizer`

Push **one commit at a time** so your teacher sees incremental history. Run from the project root in **Git Bash** or PowerShell.

**Prerequisite:** MinGW on PATH for local tests (`mingw32-make tests`).

---

## Commit 1 — Phase 1 core (parser, catalog, plan, executor)

```bash
git add src/plan.cpp src/plan.h src/catalog.cpp src/catalog.h
git add src/parser.cpp src/parser.h src/executor.cpp src/executor.h
git add src/main.cpp Makefile .gitignore
git commit -m "Phase 1: parser, catalog, executor, and naive plan pipeline"
git push origin main
```

---

## Commit 2 — Phase 2 rewriter

```bash
git add src/rewriter.cpp src/rewriter.h
git commit -m "Phase 2: rule rewriter (fold, pushdown, join swap)"
git push origin main
```

---

## Commit 3 — Phase 2 cost model

```bash
git add src/cost_model.cpp src/cost_model.h tests/test_cost.cpp
git commit -m "Phase 2: System R cost model and cardinality tests"
git push origin main
```

---

## Commit 4 — Phase 3 Selinger join-order DP

```bash
git add src/join_order.cpp src/join_order.h tests/test_join_order.cpp
git commit -m "Phase 3: Selinger DP join-order search"
git push origin main
```

---

## Commit 5 — Phase 3 optimizer integration (4 modes)

```bash
git add src/main.cpp
git commit -m "Phase 3: integrate rules, DP, and four optimizer modes in qopt shell"
git push origin main
```

---

## Commit 6 — Tests

```bash
git add tests/test_parser.cpp tests/test_rewriter.cpp tests/test_e2e.cpp
git commit -m "Tests: parser, rewriter, join-order, and end-to-end correctness"
git push origin main
```

---

## Commit 7 — Benchmark data generator and sample data

```bash
git add benchmark/gen_data.cpp benchmark/benchdata/*.csv benchmark/benchdata/catalog.json
git commit -m "Benchmark: deterministic CSV generator and benchdata schema"
git push origin main
```

---

## Commit 8 — Benchmark driver and results

```bash
git add benchmark/run_bench.sh benchmark/run_bench.ps1
git add benchmark/run_bench_batch.cpp Makefile
git add benchmark/benchmark_results.txt benchmark/speedup_summary.md
git commit -m "Phase 3: benchmark suite (Q1-Q5) and recorded results"
git push origin main
```

---

## Commit 9 — Documentation

```bash
git add README.md design.md manual\ \(2\).pdf
git commit -m "Docs: README, design document, and course manual"
git push origin main
```

---

## After pushing

1. Open GitHub → **Commits** — teacher sees each phase in order.
2. Export `design.md` → **`design.pdf`** for the zip submission.
3. Zip as `Group29_Project02_Optimizer.zip` (source + `design.pdf` + `benchmark/` + `tests/`).

**Do not commit:** `*.exe`, `*.o`, `manual_extract.txt` (already in `.gitignore`).
