#include "join_order.h"
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <iostream>

// ============================================================
//  extract_join_info — recursively collect base tables + join conds
// ============================================================
static bool is_base_table_node(const PlanNode* node) {
    if (!node) return false;
    if (node->kind == PlanKind::SCAN) return true;
    if (node->kind == PlanKind::FILTER && node->left &&
        (node->left->kind == PlanKind::SCAN)) return true;
    return false;
}

void extract_join_info(const PlanNode*         node,
                       std::vector<BaseTable>& tables,
                       std::vector<JoinCond>&  conds)
{
    if (!node) return;

    if (is_base_table_node(node)) {
        BaseTable bt;
        bt.name = collect_tables(node).empty() ? "" : collect_tables(node)[0];
        bt.plan = clone_plan(node);
        tables.push_back(std::move(bt));
        return;
    }

    if (node->kind == PlanKind::JOIN || node->kind == PlanKind::CROSS_PRODUCT) {
        extract_join_info(node->left.get(),  tables, conds);
        extract_join_info(node->right.get(), tables, conds);

        if (node->join_pred &&
            node->join_pred->kind == PredKind::EXPR_OP_EXPR &&
            node->join_pred->lhs &&
            node->join_pred->rhs &&
            node->join_pred->lhs->kind == ExprKind::COL_REF &&
            node->join_pred->rhs->kind == ExprKind::COL_REF)
        {
            JoinCond jc;
            jc.pred        = clone_pred(node->join_pred.get());
            jc.left_table  = node->join_pred->lhs->tbl;
            jc.right_table = node->join_pred->rhs->tbl;
            conds.push_back(std::move(jc));
        }
        return;
    }

    // Unexpected node type — recurse to find nested joins
    extract_join_info(node->left.get(),  tables, conds);
    extract_join_info(node->right.get(), tables, conds);
}

// ============================================================
//  JoinOrderDP::find_join_cond
//  Find a join condition connecting any table in left_mask to table t
// ============================================================
const Pred* JoinOrderDP::find_join_cond(
    int                            left_mask,
    int                            t_idx,
    const std::vector<BaseTable>&  tables,
    const std::vector<JoinCond>&   conds) const
{
    if (t_idx >= (int)tables.size()) return nullptr;
    const std::string& t_name = tables[t_idx].name;

    for (auto& jc : conds) {
        // Does one side of the condition reference t?
        bool t_is_right = (jc.right_table == t_name);
        bool t_is_left  = (jc.left_table  == t_name);
        if (!t_is_right && !t_is_left) continue;

        // The other side must belong to a table in left_mask
        const std::string& other = t_is_right ? jc.left_table : jc.right_table;
        for (int i = 0; i < (int)tables.size() && i < MAX_TABLES; i++) {
            if ((left_mask & (1 << i)) && tables[i].name == other)
                return jc.pred.get();
        }
    }
    return nullptr;
}

// ============================================================
//  JoinOrderDP::make_join
//  Build a HashJoin(left_plan, base_table[t_idx]) node
// ============================================================
std::unique_ptr<PlanNode> JoinOrderDP::make_join(
    std::unique_ptr<PlanNode>        left_plan,
    int                              t_idx,
    const Pred*                      cond,
    const std::vector<BaseTable>&    tables) const
{
    auto join      = std::make_unique<PlanNode>();
    join->kind     = PlanKind::JOIN;
    join->right    = clone_plan(tables[t_idx].plan.get());
    join->left     = std::move(left_plan);
    if (cond) join->join_pred = clone_pred(cond);

    // Re-build schema
    join->schema = join->left->schema;
    for (auto& c : join->right->schema) join->schema.push_back(c);

    return join;
}

// ============================================================
//  JoinOrderDP::find_best_order  — Selinger DP (main algorithm)
// ============================================================
std::unique_ptr<PlanNode> JoinOrderDP::find_best_order(
    std::vector<BaseTable>& tables,
    std::vector<JoinCond>&  conds)
{
    int n = (int)tables.size();
    if (n == 0) return nullptr;
    if (n == 1) return clone_plan(tables[0].plan.get());
    if (n > MAX_TABLES) {
        std::cerr << "[join_order] WARNING: " << n << " tables > MAX_TABLES=" << MAX_TABLES << ", truncating\n";
        n = MAX_TABLES;
    }

    int total = 1 << n;
    std::vector<DPEntry> dp(total);

    // ── Step 1: Initialise singleton subsets ──────────────────
    for (int i = 0; i < n; i++) {
        int mask = 1 << i;
        dp[mask].plan = clone_plan(tables[i].plan.get());
        cm_.annotate(dp[mask].plan.get());
        dp[mask].cost        = dp[mask].plan->cost;
        dp[mask].cardinality = dp[mask].plan->cardinality;
        dp[mask].valid       = true;
    }

    // ── Step 2: Fill subsets of size 2 .. n ──────────────────
    for (int size = 2; size <= n; size++) {
        for (int S = 1; S < total; S++) {
            if (popcount(S) != size) continue;

            // Try each table t ∈ S as the right (probe) side
            for (int t = 0; t < n; t++) {
                if (!(S & (1 << t))) continue;   // t not in S
                int L = S ^ (1 << t);             // left subset
                if (!dp[L].valid) continue;

                // Find join condition between L and t
                const Pred* jcond = find_join_cond(L, t, tables, conds);

                // Skip if no join condition (cross product) — unless forced
                // (only force when no other option remains)
                if (!jcond) {
                    // See if any other t' in S has a condition with L
                    bool found_alt = false;
                    for (int t2 = 0; t2 < n && !found_alt; t2++) {
                        if (t2 == t || !(S & (1 << t2))) continue;
                        if (find_join_cond(L ^ (1 << t2), t2, tables, conds)) found_alt = true;
                        // Also check t2's left condition
                        if (find_join_cond(L, t2, tables, conds)) found_alt = true;
                    }
                    if (found_alt) continue;  // better split exists → skip cross product
                }

                auto candidate = make_join(clone_plan(dp[L].plan.get()), t, jcond, tables);
                cm_.annotate(candidate.get());

                double c = candidate->cost;
                if (!dp[S].valid || c < dp[S].cost) {
                    dp[S].cost        = c;
                    dp[S].cardinality = candidate->cardinality;
                    dp[S].plan        = std::move(candidate);
                    dp[S].valid       = true;
                }
            }
        }
    }

    // ── Step 3: Return the best plan for the full set ─────────
    int full = total - 1;
    if (dp[full].valid) return std::move(dp[full].plan);

    // Fallback: join in given order (should not normally reach here)
    std::cerr << "[join_order] WARNING: DP did not find a connected plan, using given order\n";
    auto result = clone_plan(tables[0].plan.get());
    for (int i = 1; i < n; i++) {
        const Pred* jcond = find_join_cond((1 << i) - 1, i, tables, conds);
        result = make_join(std::move(result), i, jcond, tables);
    }
    cm_.annotate(result.get());
    return result;
}

// ============================================================
//  apply_join_ordering — walk plan, replace join subtrees with DP
// ============================================================
static bool is_join_kind(PlanKind k) {
    return k == PlanKind::JOIN || k == PlanKind::CROSS_PRODUCT;
}

std::unique_ptr<PlanNode> apply_join_ordering(
    std::unique_ptr<PlanNode> plan,
    const CostModel&          cm,
    const Catalog&            cat)
{
    if (!plan) return nullptr;

    // If this node is a join root, extract the whole subtree and run DP
    if (is_join_kind(plan->kind)) {
        std::vector<BaseTable> tables;
        std::vector<JoinCond>  conds;
        extract_join_info(plan.get(), tables, conds);
        if ((int)tables.size() >= 2) {
            JoinOrderDP dp(cm, cat);
            return dp.find_best_order(tables, conds);
        }
        return plan;
    }

    // Otherwise recurse — pass non-join wrappers (Project, Filter, GroupBy, Limit)
    if (plan->left)  plan->left  = apply_join_ordering(std::move(plan->left),  cm, cat);
    if (plan->right) plan->right = apply_join_ordering(std::move(plan->right), cm, cat);
    return plan;
}
