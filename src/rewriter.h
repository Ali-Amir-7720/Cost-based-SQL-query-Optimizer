#pragma once
#include "plan.h"
#include "catalog.h"

class Rewriter {
public:
    explicit Rewriter(const Catalog& cat) : cat_(cat) {}
    std::unique_ptr<PlanNode> rewrite(std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> apply_join_swap(std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> constant_fold     (std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> predicate_pushdown(std::unique_ptr<PlanNode> plan);
    std::unique_ptr<PlanNode> projection_pushdown(std::unique_ptr<PlanNode> plan);

private:
    const Catalog& cat_;

    bool  try_fold_pred(Pred* p) const;
    Value eval_const_expr(const Expr* e) const;
    std::unique_ptr<PlanNode> push_preds_through_join(
        std::unique_ptr<PlanNode> filter,
        std::unique_ptr<PlanNode> join);

    void collect_needed(const PlanNode* node,
                        std::vector<std::pair<std::string,std::string>>& needed) const;
    void collect_expr_cols(const Expr* e,
                           std::vector<std::pair<std::string,std::string>>& needed) const;
    void collect_pred_cols(const Pred* p,
                           std::vector<std::pair<std::string,std::string>>& needed) const;

    //reattach schemas
    void reattach_schemas(PlanNode* node) const;
    Schema make_join_schema(const PlanNode* left, const PlanNode* right) const;
};
