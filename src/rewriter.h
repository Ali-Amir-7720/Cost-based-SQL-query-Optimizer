#pragma once
#include "plan.h"
#include "catalog.h"

// ============================================================
//  Rule-based rewriter
//
//  Apply order (fixed-point loop):
//    1. Constant folding
//    2. Predicate pushdown
//    3. Projection pushdown
//  Repeat until no change.
//  Then separately apply join-input swap after cost estimation.
// ============================================================
class Rewriter {
public:
    explicit Rewriter(const Catalog& cat) : cat_(cat) {}

    // Run rules 1–3 to a fixed point, then return the resulting plan.
    std::unique_ptr<PlanNode> rewrite(std::unique_ptr<PlanNode> plan);

    // Rule 4: swap join inputs so the smaller side is the build (left) side.
    // Call after cost annotation.
    std::unique_ptr<PlanNode> apply_join_swap(std::unique_ptr<PlanNode> plan);

    // Expose individual rules for unit testing
    std::unique_ptr<PlanNode> constant_fold     (std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> predicate_pushdown(std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> projection_pushdown(std::unique_ptr<PlanNode> plan);

private:
    const Catalog& cat_;

    // ---- constant folding helpers ----
    bool  try_fold_pred(Pred* p) const;
    Value eval_const_expr(const Expr* e) const; // returns null_val if not foldable

    // ---- pushdown helpers ----
    // Push predicates through a Join node sitting inside a Filter
    std::unique_ptr<PlanNode> push_preds_through_join(
        std::unique_ptr<PlanNode> filter,
        std::unique_ptr<PlanNode> join);

    // ---- projection helpers ----
    // Collect all column refs needed throughout the subtree
    void collect_needed(const PlanNode* node,
                        std::vector<std::pair<std::string,std::string>>& needed) const;
    void collect_expr_cols(const Expr* e,
                           std::vector<std::pair<std::string,std::string>>& needed) const;
    void collect_pred_cols(const Pred* p,
                           std::vector<std::pair<std::string,std::string>>& needed) const;

    // Re-attach schemas after rewriting
    void reattach_schemas(PlanNode* node) const;
    Schema make_join_schema(const PlanNode* left, const PlanNode* right) const;
};
