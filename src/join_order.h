#pragma once
#include "plan.h"
#include "catalog.h"
#include "cost_model.h"
#include <vector>
#include <string>
#include <limits>

// ============================================================
//  Join-order search — Selinger 1979 dynamic programming
//
//  Input:  set of base tables (with pushed-down filters), join
//          conditions between them.
//  Output: cheapest left-deep join tree joining all tables.
//
//  State:  dp[S] = cheapest plan joining exactly the tables
//          encoded in bitmask S.
//
//  n ≤ MAX_TABLES (≤ 8 for bonus); base project uses n ≤ 4.
// ============================================================

constexpr int MAX_TABLES = 8;

// A single base table after predicate pushdown:
//   plan = Filter(pushed_preds, Scan(table))  (or just Scan if no preds)
struct BaseTable {
    std::string              name;
    std::unique_ptr<PlanNode> plan;   // Scan or Filter(Scan)
};

// A join condition between two table sets
struct JoinCond {
    std::unique_ptr<Pred> pred;
    // Which table's columns are on each side (for bitmap lookup)
    std::string left_table;
    std::string right_table;
};

class JoinOrderDP {
public:
    JoinOrderDP(const CostModel& cm, const Catalog& cat)
        : cm_(cm), cat_(cat) {}

    // Given base tables (post-pushdown) and inter-table join conditions,
    // return the optimal left-deep join tree.
    std::unique_ptr<PlanNode> find_best_order(
        std::vector<BaseTable>&   tables,
        std::vector<JoinCond>&    conds);

private:
    const CostModel& cm_;
    const Catalog&   cat_;

    struct DPEntry {
        double                    cost        = std::numeric_limits<double>::infinity();
        double                    cardinality = 0.0;
        std::unique_ptr<PlanNode> plan;
        bool                      valid       = false;
    };

    // Count set bits in a bitmask
    static int popcount(int mask) {
        int c = 0;
        for (int m = mask; m; m &= m-1) c++;
        return c;
    }

    // Find a join condition connecting any table in left_mask to table t
    const Pred* find_join_cond(int left_mask, int t_idx,
                               const std::vector<BaseTable>&  tables,
                               const std::vector<JoinCond>&   conds) const;

    // Build a HashJoin node for the plan(left_mask) ⋈ base_table[t_idx]
    std::unique_ptr<PlanNode> make_join(
        std::unique_ptr<PlanNode> left_plan,
        int                       t_idx,
        const Pred*               cond,
        const std::vector<BaseTable>& tables) const;
};

// ============================================================
//  Free functions (used by the optimizer pipeline in main.cpp)
// ============================================================

// Recursively extract base tables and join conditions from a join subtree.
void extract_join_info(const PlanNode*         node,
                       std::vector<BaseTable>& tables,
                       std::vector<JoinCond>&  conds);

// Walk a plan tree; when a join-root subtree is found replace it with
// the optimal left-deep order produced by Selinger DP.
std::unique_ptr<PlanNode> apply_join_ordering(
    std::unique_ptr<PlanNode> plan,
    const CostModel&          cm,
    const Catalog&            cat);
