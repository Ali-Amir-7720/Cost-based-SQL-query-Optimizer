# Benchmark speedup summary (5 queries × 4 modes)

From `benchmark_results.txt` — speedup = baseline (NONE) time ÷ mode time (or cost ratio when baseline OOMs).

| Query | NONE | RULES only | DP only | FULL |
|-------|------|------------|---------|------|
| Q1 | 1.0× | 9412.3× | 9412.3× | **9412.3×** |
| Q2 | 1.0× | 3.83e9× | 3.83e9× | **3.83e9×** |
| Q3 | 1.0× | 1.78e14× | 1.88e14× | **1.88e14×** |
| Q4 | 1.0× | 7519.3× | 7519.3× | **7519.3×** |
| Q5 | 1.0× | 3.63e9× | 3.56e9× | **3.63e9×** |

**Notes**

- NONE runs hit the 10M-row cross-product guard (OOM). The speedup is computed using the cost ratio of the baseline cost vs. the optimized cost.
- DP-only mode is now fully functional because it extracts join predicates from the top Filter node and pushes down single-table filters to base tables before DP ordering, matching the performance of the full optimizer.
- In Q3, FULL and DP-only achieve a lower cost ($5310043$) than RULES-only ($5605267$), demonstrating that Selinger DP successfully finds a better join order.

