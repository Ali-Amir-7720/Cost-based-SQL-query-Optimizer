#include "cost_model.h"
#include <algorithm>
#include <cmath>
#include <cassert>

// ============================================================
//  NDV helper — number of distinct values for a column expr
// ============================================================
int64_t CostModel::ndv(const Expr* e) const {
    if (!e || e->kind != ExprKind::COL_REF) return 1;
    const ColStats* cs = cat_.get_col(e->tbl, e->col);
    if (!cs) return 1;
    return std::max((int64_t)1, cs->distinct_count);
}

// ============================================================
//  Selectivity of a single predicate
// ============================================================
double CostModel::selectivity_one(const Pred* p) const {
    if (!p) return 1.0;
    if (p->kind == PredKind::ALWAYS_TRUE)  return 1.0;
    if (p->kind == PredKind::ALWAYS_FALSE) return 0.0;
    if (!p->lhs || !p->rhs) return 1.0;

    // Identify column side and value side
    const Expr* col_e = nullptr;
    const Expr* val_e = nullptr;
    bool col_is_lhs   = false;

    if (p->lhs->kind == ExprKind::COL_REF) { col_e = p->lhs.get(); val_e = p->rhs.get(); col_is_lhs = true; }
    else if (p->rhs->kind == ExprKind::COL_REF) { col_e = p->rhs.get(); val_e = p->lhs.get(); col_is_lhs = false; }

    // col op col  → join selectivity default (handled higher up)
    if (!col_e && p->lhs->kind == ExprKind::COL_REF && p->rhs->kind == ExprKind::COL_REF)
        return 1.0 / std::max(ndv(p->lhs.get()), ndv(p->rhs.get()));

    if (!col_e) return 0.5; // expression pred — default

    const ColStats* cs  = cat_.get_col(col_e->tbl, col_e->col);
    if (!cs) return 0.5;
    int64_t ndv_val = std::max((int64_t)1, cs->distinct_count);

    // Get numeric literal value (may not be available if val side is an expr)
    double lit_num  = 0.0;
    bool   have_lit = (val_e && val_e->kind == ExprKind::LITERAL);
    if (have_lit) lit_num = val_e->lit.as_numeric();

    // Adjust operator if column is on RHS
    Op op = p->op;
    if (!col_is_lhs) {
        switch (op) {
            case Op::LT: op = Op::GT; break;  case Op::LE: op = Op::GE; break;
            case Op::GT: op = Op::LT; break;  case Op::GE: op = Op::LE; break;
            default: break;
        }
    }

    switch (op) {
        case Op::EQ: {
            if (have_lit && (cs->type == ValType::INT || cs->type == ValType::DOUBLE)) {
                if (lit_num < cs->min_val - 1e-9 || lit_num > cs->max_val + 1e-9)
                    return 0.0; // literal outside range → zero selectivity
            }
            // String equality: treat as 1/NDV
            return 1.0 / (double)ndv_val;
        }
        case Op::NEQ:
            return 1.0 - 1.0 / (double)ndv_val;

        case Op::LT:
        case Op::LE: {
            if (!have_lit || cs->type == ValType::TEXT) return 1.0 / 3.0;
            double range = cs->max_val - cs->min_val;
            if (range <= 0.0) return 0.5;
            return std::max(0.0, std::min(1.0, (lit_num - cs->min_val) / range));
        }
        case Op::GT:
        case Op::GE: {
            if (!have_lit || cs->type == ValType::TEXT) return 1.0 / 3.0;
            double range = cs->max_val - cs->min_val;
            if (range <= 0.0) return 0.5;
            return std::max(0.0, std::min(1.0, (cs->max_val - lit_num) / range));
        }
        default:
            return 0.5;
    }
}

// Selectivity of a full predicate (multiple conjuncts → multiply)
double CostModel::selectivity(const Pred* p) const {
    return selectivity_one(p);
}

// Selectivity of a vector of conjuncts (AND → product)
static double combined_sel(const std::vector<std::unique_ptr<Pred>>& preds,
                           const CostModel& cm) {
    double sel = 1.0;
    for (auto& p : preds) sel *= cm.selectivity(p.get());
    return sel;
}

// ============================================================
//  Per-operator annotation helpers
// ============================================================
void CostModel::annotate_scan(PlanNode* node) const {
    const TableMeta* tm = cat_.get_table(node->table_name);
    double rc = tm ? (double)tm->row_count : 1000.0;
    node->cardinality = rc;
    node->cost        = rc;   // cost of reading every row
}

void CostModel::annotate_filter(PlanNode* node) const {
    double in_card = node->left ? node->left->cardinality : 1.0;
    double in_cost = node->left ? node->left->cost        : 0.0;
    double sel = 1.0;
    for (auto& p : node->preds) sel *= selectivity_one(p.get());
    sel = std::max(sel, 1e-6);
    node->cardinality = std::max(1.0, in_card * sel);
    node->cost        = in_cost + in_card;   // read every input row
}

void CostModel::annotate_join(PlanNode* node) const {
    if (!node->left || !node->right) return;
    double lcard = node->left->cardinality;
    double rcard = node->right->cardinality;
    double lcost = node->left->cost;
    double rcost = node->right->cost;

    double out_card;
    double cost;

    if (!node->join_pred) {
        // Cross product — enormous cost to bias optimizer away
        out_card = lcard * rcard;
        cost     = lcost + rcost + out_card;
    } else {
        // System R equijoin formula:
        // out = L.card * R.card / max(NDV(left.col), NDV(right.col))
        const Pred* jp = node->join_pred.get();
        int64_t ndv_l = ndv(jp->lhs.get());
        int64_t ndv_r = ndv(jp->rhs.get());
        int64_t max_ndv = std::max((int64_t)1, std::max(ndv_l, ndv_r));

        out_card = std::max(1.0, (lcard * rcard) / (double)max_ndv);
        // HashJoin cost: build left, probe right, materialise output
        cost = lcost + rcost + 2.0 * lcard + rcard + out_card;
    }

    node->cardinality = out_card;
    node->cost        = cost;
}

void CostModel::annotate_cross_product(PlanNode* node) const {
    double lcard = node->left  ? node->left->cardinality  : 1.0;
    double rcard = node->right ? node->right->cardinality : 1.0;
    double lcost = node->left  ? node->left->cost         : 0.0;
    double rcost = node->right ? node->right->cost        : 0.0;
    node->cardinality = lcard * rcard;
    node->cost        = lcost + rcost + node->cardinality;
}

void CostModel::annotate_project(PlanNode* node) const {
    double in_card = node->left ? node->left->cardinality : 0.0;
    double in_cost = node->left ? node->left->cost        : 0.0;
    node->cardinality = in_card;
    node->cost        = in_cost + in_card;
}

void CostModel::annotate_groupby(PlanNode* node) const {
    double in_card = node->left ? node->left->cardinality : 0.0;
    double in_cost = node->left ? node->left->cost        : 0.0;
    // Output: one row per distinct group value
    int64_t ndv_est = 1;
    if (!node->gb_col.empty()) {
        const ColStats* cs = cat_.get_col(node->gb_table, node->gb_col);
        if (cs) ndv_est = std::max((int64_t)1, cs->distinct_count);
        else    ndv_est = std::max((int64_t)1, (int64_t)(in_card / 10));
    }
    node->cardinality = std::min((double)ndv_est, in_card);
    node->cost        = in_cost + in_card;
}

void CostModel::annotate_limit(PlanNode* node) const {
    double in_card = node->left ? node->left->cardinality : 0.0;
    double in_cost = node->left ? node->left->cost        : 0.0;
    node->cardinality = std::min((double)node->limit_n, in_card);
    node->cost        = in_cost + node->cardinality;
}

// ============================================================
//  Dispatch  — bottom-up walk
// ============================================================
void CostModel::annotate(PlanNode* node) const {
    if (!node) return;
    annotate(node->left.get());
    annotate(node->right.get());
    switch (node->kind) {
        case PlanKind::SCAN:          annotate_scan(node);          break;
        case PlanKind::FILTER:        annotate_filter(node);        break;
        case PlanKind::JOIN:          annotate_join(node);          break;
        case PlanKind::CROSS_PRODUCT: annotate_cross_product(node); break;
        case PlanKind::PROJECT:       annotate_project(node);       break;
        case PlanKind::GROUPBY:       annotate_groupby(node);       break;
        case PlanKind::LIMIT:         annotate_limit(node);         break;
        case PlanKind::EMPTY:
            node->cardinality = 0;
            node->cost        = 0;
            break;
    }
}
