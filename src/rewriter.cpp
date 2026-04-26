#include "rewriter.h"
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <iostream>

// ============================================================
//  Schema re-attachment (needed after structural changes)
// ============================================================
Schema Rewriter::make_join_schema(const PlanNode* left, const PlanNode* right) const {
    Schema s = left ? left->schema : Schema{};
    if (right) for (auto& c : right->schema) s.push_back(c);
    return s;
}

void Rewriter::reattach_schemas(PlanNode* node) const {
    if (!node) return;
    reattach_schemas(node->left.get());
    reattach_schemas(node->right.get());
    switch (node->kind) {
        case PlanKind::SCAN:
            node->schema = cat_.make_schema(node->table_name);
            break;
        case PlanKind::FILTER:
            if (node->left) node->schema = node->left->schema;
            break;
        case PlanKind::JOIN:
        case PlanKind::CROSS_PRODUCT:
            node->schema = make_join_schema(node->left.get(), node->right.get());
            break;
        case PlanKind::PROJECT: {
            bool star = (!node->proj_exprs.empty() &&
                         node->proj_exprs[0]->kind == ExprKind::COL_REF &&
                         node->proj_exprs[0]->col  == "*");
            if (star && node->left) { node->schema = node->left->schema; break; }
            // Rebuild schema from expressions
            Schema s;
            for (auto& ex : node->proj_exprs) {
                SchemaCol sc;
                if (ex->kind == ExprKind::COL_REF) {
                    sc.table = ex->tbl; sc.name = ex->col;
                    if (node->left) {
                        int idx = find_col(node->left->schema, ex->tbl, ex->col);
                        if (idx >= 0) sc.type = node->left->schema[idx].type;
                    }
                } else if (ex->kind == ExprKind::AGGREGATE) {
                    sc.name = agg_str(ex->agg); sc.type = ValType::DOUBLE;
                } else { sc.name = ex->to_string(); sc.type = ValType::DOUBLE; }
                if (!ex->alias.empty()) sc.alias = ex->alias;
                s.push_back(sc);
            }
            node->schema = s;
            break;
        }
        case PlanKind::GROUPBY: {
            Schema s;
            if (node->left) {
                int idx = find_col(node->left->schema, node->gb_table, node->gb_col);
                if (idx >= 0) s.push_back(node->left->schema[idx]);
            }
            SchemaCol ac; ac.name = agg_str(node->gb_agg); ac.type = ValType::DOUBLE;
            s.push_back(ac);
            node->schema = s;
            break;
        }
        case PlanKind::LIMIT:
        case PlanKind::EMPTY:
            if (node->left) node->schema = node->left->schema;
            break;
    }
}

// ============================================================
//  Rule 1 — Constant Folding
// ============================================================
Value Rewriter::eval_const_expr(const Expr* e) const {
    if (!e) return Value::null_val();
    if (e->kind == ExprKind::LITERAL) return e->lit;
    if (e->kind == ExprKind::COL_REF) return Value::null_val(); // not foldable
    if (e->kind == ExprKind::BINARY_OP) {
        Value lv = eval_const_expr(e->lhs.get());
        Value rv = eval_const_expr(e->rhs.get());
        if (lv.is_null() || rv.is_null()) return Value::null_val();
        double ld = lv.as_numeric(), rd = rv.as_numeric();
        switch (e->op) {
            case Op::MUL: return Value::from_double(ld * rd);
            case Op::ADD: return Value::from_double(ld + rd);
            case Op::SUB: return Value::from_double(ld - rd);
            case Op::DIV: return (rd == 0) ? Value::null_val() : Value::from_double(ld / rd);
            case Op::EQ:  return Value::from_bool(value_cmp(lv,rv)==0);
            case Op::NEQ: return Value::from_bool(value_cmp(lv,rv)!=0);
            case Op::LT:  return Value::from_bool(value_cmp(lv,rv)< 0);
            case Op::LE:  return Value::from_bool(value_cmp(lv,rv)<=0);
            case Op::GT:  return Value::from_bool(value_cmp(lv,rv)> 0);
            case Op::GE:  return Value::from_bool(value_cmp(lv,rv)>=0);
            default:      return Value::null_val();
        }
    }
    return Value::null_val();
}

bool Rewriter::try_fold_pred(Pred* p) const {
    if (p->kind != PredKind::EXPR_OP_EXPR) return false;
    if (!p->lhs || !p->rhs) return false;
    if (!p->lhs->is_literal() || !p->rhs->is_literal()) return false;
    // Both sides are pure literals → fold
    Value lv = eval_const_expr(p->lhs.get());
    Value rv = eval_const_expr(p->rhs.get());
    if (lv.is_null() || rv.is_null()) return false;
    int cmp = value_cmp(lv, rv);
    bool result;
    switch (p->op) {
        case Op::EQ:  result = (cmp==0); break;
        case Op::NEQ: result = (cmp!=0); break;
        case Op::LT:  result = (cmp< 0); break;
        case Op::LE:  result = (cmp<=0); break;
        case Op::GT:  result = (cmp> 0); break;
        case Op::GE:  result = (cmp>=0); break;
        default: return false;
    }
    p->kind = result ? PredKind::ALWAYS_TRUE : PredKind::ALWAYS_FALSE;
    return true;
}

std::unique_ptr<PlanNode> Rewriter::constant_fold(std::unique_ptr<PlanNode> node) {
    if (!node) return nullptr;
    node->left  = constant_fold(std::move(node->left));
    node->right = constant_fold(std::move(node->right));

    if (node->kind != PlanKind::FILTER) return node;

    bool any_false = false;
    for (auto& p : node->preds) {
        try_fold_pred(p.get());
        if (p->kind == PredKind::ALWAYS_FALSE) { any_false = true; break; }
    }
    if (any_false) {
        // Replace entire subtree with EMPTY
        auto empty = std::make_unique<PlanNode>();
        empty->kind = PlanKind::EMPTY;
        return empty;
    }
    // Remove ALWAYS_TRUE predicates
    node->preds.erase(
        std::remove_if(node->preds.begin(), node->preds.end(),
            [](const std::unique_ptr<Pred>& p){ return p->kind == PredKind::ALWAYS_TRUE; }),
        node->preds.end());
    // If no predicates remain, return child directly
    if (node->preds.empty()) return std::move(node->left);
    return node;
}

// ============================================================
//  Rule 2 — Predicate Pushdown
// ============================================================
std::unique_ptr<PlanNode> Rewriter::push_preds_through_join(
    std::unique_ptr<PlanNode> filter,
    std::unique_ptr<PlanNode> join)
{
    auto left_tables  = collect_tables(join->left.get());
    auto right_tables = collect_tables(join->right.get());

    std::vector<std::unique_ptr<Pred>> remaining;
    std::vector<std::unique_ptr<Pred>> for_left;
    std::vector<std::unique_ptr<Pred>> for_right;

    for (auto& pred : filter->preds) {
        auto refs  = pred->referenced_tables();
        bool l_ref = false, r_ref = false;
        for (auto& t : refs) {
            if (std::find(left_tables.begin(),  left_tables.end(),  t) != left_tables.end()) l_ref = true;
            if (std::find(right_tables.begin(), right_tables.end(), t) != right_tables.end()) r_ref = true;
        }
        if (l_ref && r_ref) {
            // Cross-table predicate → becomes join condition (equijoin)
            if (!join->join_pred && pred->op == Op::EQ &&
                pred->lhs && pred->lhs->kind == ExprKind::COL_REF &&
                pred->rhs && pred->rhs->kind == ExprKind::COL_REF)
            {
                join->join_pred = std::move(pred);
            } else {
                remaining.push_back(std::move(pred));
            }
        } else if (l_ref) {
            for_left.push_back(std::move(pred));
        } else if (r_ref) {
            for_right.push_back(std::move(pred));
        } else {
            // Constant predicate — keep above
            remaining.push_back(std::move(pred));
        }
    }

    // Push filters down
    if (!for_left.empty()) {
        auto lf   = std::make_unique<PlanNode>();
        lf->kind  = PlanKind::FILTER;
        lf->preds = std::move(for_left);
        lf->left  = std::move(join->left);
        join->left = predicate_pushdown(std::move(lf));
    }
    if (!for_right.empty()) {
        auto rf   = std::make_unique<PlanNode>();
        rf->kind  = PlanKind::FILTER;
        rf->preds = std::move(for_right);
        rf->left  = std::move(join->right);
        join->right = predicate_pushdown(std::move(rf));
    }

    if (remaining.empty()) return join;  // filter dissolved

    filter->preds = std::move(remaining);
    filter->left  = std::move(join);
    return filter;
}

std::unique_ptr<PlanNode> Rewriter::predicate_pushdown(std::unique_ptr<PlanNode> node) {
    if (!node) return nullptr;
    node->left  = predicate_pushdown(std::move(node->left));
    node->right = predicate_pushdown(std::move(node->right));

    if (node->kind != PlanKind::FILTER) return node;
    if (!node->left) return node;

    // Try to push through a JOIN
    if (node->left->kind == PlanKind::JOIN ||
        node->left->kind == PlanKind::CROSS_PRODUCT)
    {
        auto join = std::move(node->left);
        node->left = nullptr;
        return push_preds_through_join(std::move(node), std::move(join));
    }
    return node;
}

// ============================================================
//  Rule 3 — Projection Pushdown
// ============================================================
void Rewriter::collect_expr_cols(const Expr* e,
    std::vector<std::pair<std::string,std::string>>& needed) const
{
    if (!e) return;
    if (e->kind == ExprKind::COL_REF && e->col != "*")
        needed.push_back({e->tbl, e->col});
    collect_expr_cols(e->lhs.get(), needed);
    collect_expr_cols(e->rhs.get(), needed);
    collect_expr_cols(e->agg_arg.get(), needed);
}

void Rewriter::collect_pred_cols(const Pred* p,
    std::vector<std::pair<std::string,std::string>>& needed) const
{
    if (!p || p->kind != PredKind::EXPR_OP_EXPR) return;
    collect_expr_cols(p->lhs.get(), needed);
    collect_expr_cols(p->rhs.get(), needed);
}

void Rewriter::collect_needed(const PlanNode* node,
    std::vector<std::pair<std::string,std::string>>& needed) const
{
    if (!node) return;
    for (auto& p  : node->preds)      collect_pred_cols(p.get(), needed);
    if (node->join_pred)               collect_pred_cols(node->join_pred.get(), needed);
    for (auto& ex : node->proj_exprs)  collect_expr_cols(ex.get(), needed);
    if (node->gb_agg_expr)             collect_expr_cols(node->gb_agg_expr.get(), needed);
    if (node->gb_key_expr)             collect_expr_cols(node->gb_key_expr.get(), needed);
    if (!node->gb_col.empty())         needed.push_back({node->gb_table, node->gb_col});
    collect_needed(node->left.get(),  needed);
    collect_needed(node->right.get(), needed);
}

std::unique_ptr<PlanNode> Rewriter::projection_pushdown(std::unique_ptr<PlanNode> node) {
    if (!node) return nullptr;
    // Only apply at Scan nodes — wrap with a Project that keeps only needed cols
    if (node->kind == PlanKind::SCAN) {
        // Schema is already set; nothing to push further
        return node;
    }
    // For simplicity: collect all needed columns from the entire subtree,
    // then add narrow Project nodes just above each Scan
    // (full projection pushdown would require ancestor info; this is the
    //  standard "scan-level projection" optimisation)
    return node;  // no-op in this simplified form; predicate pushdown is the key win
}

// ============================================================
//  Fixed-point loop
// ============================================================
std::unique_ptr<PlanNode> Rewriter::rewrite(std::unique_ptr<PlanNode> plan) {
    const int MAX_ITERS = 10;
    for (int iter = 0; iter < MAX_ITERS; iter++) {
        std::string before = plan_to_string(plan.get());
        plan = constant_fold(std::move(plan));
        plan = predicate_pushdown(std::move(plan));
        plan = projection_pushdown(std::move(plan));
        reattach_schemas(plan.get());
        std::string after = plan_to_string(plan.get());
        if (before == after) break;
    }
    return plan;
}

// ============================================================
//  Rule 4 — Join Input Swap (applied after cost annotation)
// ============================================================
std::unique_ptr<PlanNode> Rewriter::apply_join_swap(std::unique_ptr<PlanNode> node) {
    if (!node) return nullptr;
    node->left  = apply_join_swap(std::move(node->left));
    node->right = apply_join_swap(std::move(node->right));

    if (node->kind == PlanKind::JOIN && node->left && node->right) {
        // Build side should be smaller → put smaller on left
        if (node->right->cardinality < node->left->cardinality) {
            // Swap children
            std::swap(node->left, node->right);
            // Also swap join condition sides if needed
            if (node->join_pred &&
                node->join_pred->kind == PredKind::EXPR_OP_EXPR) {
                std::swap(node->join_pred->lhs, node->join_pred->rhs);
            }
            reattach_schemas(node.get());
        }
    }
    return node;
}
