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
//  JoinOrderDP::find_join_cond_masks
//  Find a join condition whose sides span left_mask and right_mask
// ============================================================
const Pred* JoinOrderDP::find_join_cond_masks(
    int                            left_mask,
    int                            right_mask,
    const std::vector<BaseTable>&  tables,
    const std::vector<JoinCond>&   conds) const
{
    for (auto& jc : conds) {
        int l_idx = -1, r_idx = -1;
        for (int i = 0; i < (int)tables.size() && i < MAX_TABLES; i++) {
            if (tables[i].name == jc.left_table)  l_idx = i;
            if (tables[i].name == jc.right_table) r_idx = i;
        }
        if (l_idx < 0 || r_idx < 0) continue;

        bool l_in_left  = (left_mask  & (1 << l_idx)) != 0;
        bool r_in_right = (right_mask & (1 << r_idx)) != 0;
        bool l_in_right = (right_mask & (1 << l_idx)) != 0;
        bool r_in_left  = (left_mask  & (1 << r_idx)) != 0;

        if ((l_in_left && r_in_right) || (l_in_right && r_in_left))
            return jc.pred.get();
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
    const std::vector<BaseTable>&    tables,
    bool                             is_smj) const
{
    auto join      = std::make_unique<PlanNode>();
    join->kind     = is_smj ? PlanKind::SORT_MERGE_JOIN : PlanKind::JOIN;
    join->right    = clone_plan(tables[t_idx].plan.get());
    join->left     = std::move(left_plan);
    if (cond) join->join_pred = clone_pred(cond);

    // Re-build schema
    join->schema = join->left->schema;
    for (auto& c : join->right->schema) join->schema.push_back(c);

    return join;
}

// ============================================================
//  JoinOrderDP::make_join_bushy
//  Build a HashJoin(left_plan, right_plan) for two arbitrary sub-plans
// ============================================================
std::unique_ptr<PlanNode> JoinOrderDP::make_join_bushy(
    std::unique_ptr<PlanNode> left_plan,
    std::unique_ptr<PlanNode> right_plan,
    const Pred*               cond,
    bool                      is_smj) const
{
    auto join      = std::make_unique<PlanNode>();
    join->kind     = is_smj ? PlanKind::SORT_MERGE_JOIN : PlanKind::JOIN;
    join->left     = std::move(left_plan);
    join->right    = std::move(right_plan);
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
    // BONUS (Phase 3): enumerate ALL proper non-empty subsets L of S,
    // setting R = S \ L.  This expands the search space from left-deep
    // trees to full bushy trees in O(3^n) time.
    for (int size = 2; size <= n; size++) {
        for (int S = 1; S < total; S++) {
            if (popcount(S) != size) continue;

            // Check if any split of S has a join condition (to avoid
            // forcing cross products when a connected split exists)
            bool any_connected = false;
            for (int L = (S - 1) & S; L > 0 && !any_connected; L = (L - 1) & S) {
                int R = S ^ L;
                if (!dp[L].valid || !dp[R].valid) continue;
                if (find_join_cond_masks(L, R, tables, conds)) any_connected = true;
            }

            // Enumerate ALL proper non-empty subsets L of S
            for (int L = (S - 1) & S; L > 0; L = (L - 1) & S) {
                int R = S ^ L;

                // Both halves must already have optimal plans
                if (!dp[L].valid || !dp[R].valid) continue;

                // Avoid evaluating the same (L,R) pair twice as (R,L)
                // We keep both orderings so the cost model can pick
                // which side to use as build (hash) vs probe.

                const Pred* jcond = find_join_cond_masks(L, R, tables, conds);

                // Skip cross products when a join-connected split exists
                if (!jcond && any_connected) continue;

                // Try HashJoin
                auto cand_hj = make_join_bushy(
                    clone_plan(dp[L].plan.get()),
                    clone_plan(dp[R].plan.get()),
                    jcond, false);
                cm_.annotate(cand_hj.get());

                // Try SortMergeJoin
                auto cand_smj = make_join_bushy(
                    clone_plan(dp[L].plan.get()),
                    clone_plan(dp[R].plan.get()),
                    jcond, true);
                cm_.annotate(cand_smj.get());

                auto& candidate = (cand_smj->cost < cand_hj->cost) ? cand_smj : cand_hj;

                double c = candidate->cost;
                if (!dp[S].valid || c < dp[S].cost) {
                    dp[S].cost        = c;
                    dp[S].cardinality = candidate->cardinality;
                    dp[S].plan        = clone_plan(candidate.get());
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
        result = make_join(std::move(result), i, jcond, tables, false);
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

// Helper: check if a predicate is a cross-table equijoin (col = col, different tables)
static bool is_cross_table_equijoin(const Pred* p) {
    if (!p || p->kind != PredKind::EXPR_OP_EXPR) return false;
    if (p->op != Op::EQ) return false;
    if (!p->lhs || !p->rhs) return false;
    if (p->lhs->kind != ExprKind::COL_REF || p->rhs->kind != ExprKind::COL_REF) return false;
    return p->lhs->tbl != p->rhs->tbl;
}

// Helper: check if a predicate references only one table
static std::string single_table_ref(const Pred* p) {
    if (!p) return "";
    auto tbls = p->referenced_tables();
    if (tbls.size() == 1) return tbls[0];
    return "";
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

    // ── NEW: Filter sitting on top of a join/cross-product tree ────
    // In DP-only mode (no rewriter), all WHERE predicates stay in a
    // top-level Filter above bare cross-products.  Extract equijoin
    // conditions from the Filter so the DP can use them, and push
    // single-table predicates down to the matching base tables.
    if (plan->kind == PlanKind::FILTER && plan->left && is_join_kind(plan->left->kind)) {
        std::vector<BaseTable> tables;
        std::vector<JoinCond>  conds;
        extract_join_info(plan->left.get(), tables, conds);

        if ((int)tables.size() >= 2) {
            // Classify each predicate in the Filter
            std::vector<std::unique_ptr<Pred>> remaining;

            for (auto& pred : plan->preds) {
                if (is_cross_table_equijoin(pred.get())) {
                    // Cross-table equijoin → join condition for DP
                    JoinCond jc;
                    jc.left_table  = pred->lhs->tbl;
                    jc.right_table = pred->rhs->tbl;
                    jc.pred        = clone_pred(pred.get());
                    conds.push_back(std::move(jc));
                } else {
                    std::string tbl = single_table_ref(pred.get());
                    if (!tbl.empty()) {
                        // Single-table predicate → push down to the base table
                        bool pushed = false;
                        for (auto& bt : tables) {
                            if (bt.name == tbl) {
                                auto filt   = std::make_unique<PlanNode>();
                                filt->kind  = PlanKind::FILTER;
                                filt->left  = std::move(bt.plan);
                                filt->preds.push_back(clone_pred(pred.get()));
                                filt->schema = filt->left->schema;
                                bt.plan = std::move(filt);
                                pushed  = true;
                                break;
                            }
                        }
                        if (!pushed) remaining.push_back(clone_pred(pred.get()));
                    } else {
                        remaining.push_back(clone_pred(pred.get()));
                    }
                }
            }

            JoinOrderDP dp(cm, cat);
            auto result = dp.find_best_order(tables, conds);

            // Re-wrap with any remaining predicates that could not be classified
            if (!remaining.empty() && result) {
                auto filt   = std::make_unique<PlanNode>();
                filt->kind  = PlanKind::FILTER;
                filt->schema = result->schema;
                filt->left  = std::move(result);
                filt->preds = std::move(remaining);
                return filt;
            }
            return result;
        }
    }

    // Otherwise recurse — pass non-join wrappers (Project, Filter, GroupBy, Limit)
    if (plan->left)  plan->left  = apply_join_ordering(std::move(plan->left),  cm, cat);
    if (plan->right) plan->right = apply_join_ordering(std::move(plan->right), cm, cat);
    return plan;
}
