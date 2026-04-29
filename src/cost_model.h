#pragma once
#include "plan.h"
#include "catalog.h"

// ============================================================
//  Cost model
//
//  annotate(node, cat) walks the plan tree bottom-up,
//  filling in node->cardinality and node->cost for every node.
//
//  Cardinality formulas (System R / textbook):
//    Scan(t)          → t.row_count
//    Filter(pred, c)  → c.card * selectivity(pred)
//    Join(col1=col2)  → L.card * R.card / max(NDV(col1), NDV(col2))
//    CrossProduct     → L.card * R.card
//    Project          → c.card
//    GroupBy          → distinct_count(group_col)
//    Limit(n)         → min(n, c.card)
//
//  Cost formulas:
//    Scan             → row_count
//    Filter           → c.cost + c.card
//    HashJoin         → L.cost + R.cost + 2*L.card + R.card + out.card
//    CrossProduct     → L.cost + R.cost + L.card * R.card
//    Project/Groupby/Limit → c.cost + c.card
// ============================================================
class CostModel {
public:
    explicit CostModel(const Catalog& cat) : cat_(cat) {}

    // Annotate the entire plan tree bottom-up
    void annotate(PlanNode* node) const;

    // Estimate selectivity of a single predicate over its column
    double selectivity(const Pred* p) const;

    // Look up NDV (number of distinct values) for a column expression
    int64_t ndv(const Expr* col_expr) const;

private:
    const Catalog& cat_;

    void annotate_scan         (PlanNode* node) const;
    void annotate_filter       (PlanNode* node) const;
    void annotate_join         (PlanNode* node) const;
    void annotate_cross_product(PlanNode* node) const;
    void annotate_project      (PlanNode* node) const;
    void annotate_groupby      (PlanNode* node) const;
    void annotate_limit        (PlanNode* node) const;

    double selectivity_one(const Pred* p) const;
};
