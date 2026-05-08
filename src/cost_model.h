#pragma once
#include "plan.h"
#include "catalog.h"

//cost model
class CostModel {
public:
    explicit CostModel(const Catalog& cat) : cat_(cat) {}

    //annotate plan tree
    void annotate(PlanNode* node) const;

    //selectivity estimation
    double selectivity(const Pred* p) const;

    //ndv lookup
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
