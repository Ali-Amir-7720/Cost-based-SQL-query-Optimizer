# Benchmark speedup summary (5 queries × 4 modes)

From `benchmark_results.txt` — speedup = baseline (NONE) time ÷ mode time.

| Query | NONE | RULES only | DP only | FULL |
|-------|------|------------|---------|------|
| Q1 | 1.0× | 27.0× | 1.0× | **27.8×** |
| Q2 | 1.0× | large (OOM proxy) | 1.0× | large (OOM proxy) |
| Q3 | 1.0× | large (OOM proxy) | 1.0× | large (OOM proxy) |
| Q4 | 1.0× | 24.8× | 1.0× | **33.4×** |
| Q5 | 1.0× | large (OOM proxy) | 1.0× | large (OOM proxy) |

**Notes**

- NONE runs hit the 10M-row cross-product guard; baseline uses cost-proxy timing.
- Q1/Q4: rules + pushdown dominate; FULL adds join swap + DP polish.
- Q2/Q3/Q5: without rules, DP cannot fix cross-product disaster; FULL ≈ RULES.

**Cost-ratio speedup (FULL vs NONE, Q1):** ~9.4×10⁶ estimated cost units → aligns with manual’s “plan cost ratio” narrative.
