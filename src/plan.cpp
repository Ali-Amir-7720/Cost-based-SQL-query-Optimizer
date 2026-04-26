#include "plan.h"
#include <sstream>
#include <cassert>

// ============================================================
//  clone_expr
// ============================================================
std::unique_ptr<Expr> clone_expr(const Expr* e) {
    if (!e) return nullptr;
    auto r       = std::make_unique<Expr>();
    r->kind      = e->kind;
    r->tbl       = e->tbl;
    r->col       = e->col;
    r->lit       = e->lit;
    r->op        = e->op;
    r->lhs       = clone_expr(e->lhs.get());
    r->rhs       = clone_expr(e->rhs.get());
    r->agg       = e->agg;
    r->agg_arg   = clone_expr(e->agg_arg.get());
    r->alias     = e->alias;
    return r;
}

// ============================================================
//  clone_pred
// ============================================================
std::unique_ptr<Pred> clone_pred(const Pred* p) {
    if (!p) return nullptr;
    auto r   = std::make_unique<Pred>();
    r->kind  = p->kind;
    r->lhs   = clone_expr(p->lhs.get());
    r->rhs   = clone_expr(p->rhs.get());
    r->op    = p->op;
    return r;
}

// ============================================================
//  clone_plan  (deep copy)
// ============================================================
std::unique_ptr<PlanNode> clone_plan(const PlanNode* p) {
    if (!p) return nullptr;
    auto r           = std::make_unique<PlanNode>();
    r->kind          = p->kind;
    r->schema        = p->schema;
    r->cardinality   = p->cardinality;
    r->cost          = p->cost;
    r->table_name    = p->table_name;
    r->left          = clone_plan(p->left.get());
    r->right         = clone_plan(p->right.get());
    for (auto& pr : p->preds)      r->preds.push_back(clone_pred(pr.get()));
    r->join_pred     = clone_pred(p->join_pred.get());
    for (auto& ex : p->proj_exprs) r->proj_exprs.push_back(clone_expr(ex.get()));
    r->gb_table      = p->gb_table;
    r->gb_col        = p->gb_col;
    r->gb_agg        = p->gb_agg;
    r->gb_agg_expr   = clone_expr(p->gb_agg_expr.get());
    r->gb_key_expr   = clone_expr(p->gb_key_expr.get());
    r->limit_n       = p->limit_n;
    return r;
}

// ============================================================
//  collect_tables — gather all Scan table names under a node
// ============================================================
std::vector<std::string> collect_tables(const PlanNode* p) {
    if (!p) return {};
    if (p->kind == PlanKind::SCAN) return {p->table_name};
    std::vector<std::string> res;
    auto lt = collect_tables(p->left.get());
    auto rt = collect_tables(p->right.get());
    res.insert(res.end(), lt.begin(), lt.end());
    res.insert(res.end(), rt.begin(), rt.end());
    return res;
}

// ============================================================
//  plan_to_string — canonical string for fixed-point detection
// ============================================================
std::string plan_to_string(const PlanNode* p) {
    if (!p) return "NULL";
    std::ostringstream oss;
    switch (p->kind) {
        case PlanKind::SCAN:
            oss << "SCAN(" << p->table_name << ")";
            break;
        case PlanKind::FILTER:
            oss << "FILTER(";
            for (int i = 0; i < (int)p->preds.size(); i++) {
                if (i) oss << "^";
                oss << p->preds[i]->to_string();
            }
            oss << "|" << plan_to_string(p->left.get()) << ")";
            break;
        case PlanKind::JOIN:
            oss << "JOIN(" << (p->join_pred ? p->join_pred->to_string() : "X")
                << "|" << plan_to_string(p->left.get())
                << "|" << plan_to_string(p->right.get()) << ")";
            break;
        case PlanKind::CROSS_PRODUCT:
            oss << "CROSS(" << plan_to_string(p->left.get())
                << "|" << plan_to_string(p->right.get()) << ")";
            break;
        case PlanKind::PROJECT:
            oss << "PROJ(";
            for (int i = 0; i < (int)p->proj_exprs.size(); i++) {
                if (i) oss << ",";
                oss << p->proj_exprs[i]->to_string();
            }
            oss << "|" << plan_to_string(p->left.get()) << ")";
            break;
        case PlanKind::GROUPBY:
            oss << "GBY(" << p->gb_col << "|" << plan_to_string(p->left.get()) << ")";
            break;
        case PlanKind::LIMIT:
            oss << "LIM(" << p->limit_n << "|" << plan_to_string(p->left.get()) << ")";
            break;
        case PlanKind::EMPTY:
            oss << "EMPTY";
            break;
    }
    return oss.str();
}

// ============================================================
//  explain_plan — human-readable plan tree
// ============================================================
std::string explain_plan(const PlanNode* p, int indent) {
    if (!p) return "";
    std::string pad(indent * 2, ' ');
    std::ostringstream oss;

    auto card_str = [](double c) -> std::string {
        if (c >= 1e9) {
            std::ostringstream o; o << (int64_t)(c/1e9) << "B"; return o.str();
        } else if (c >= 1e6) {
            std::ostringstream o; o << (int64_t)(c/1e6) << "M"; return o.str();
        } else if (c >= 1e3) {
            std::ostringstream o; o << (int64_t)(c/1e3) << "K"; return o.str();
        }
        return std::to_string((int64_t)c);
    };

    switch (p->kind) {
        case PlanKind::SCAN:
            oss << pad << "SeqScan(" << p->table_name << ")"
                << " [est " << card_str(p->cardinality) << " rows]\n";
            break;

        case PlanKind::FILTER: {
            oss << pad << "Filter(";
            for (int i = 0; i < (int)p->preds.size(); i++) {
                if (i) oss << " AND ";
                oss << p->preds[i]->to_string();
            }
            oss << ") [est " << card_str(p->cardinality) << " rows]\n";
            oss << explain_plan(p->left.get(), indent + 1);
            break;
        }

        case PlanKind::JOIN:
            oss << pad << "HashJoin("
                << (p->join_pred ? p->join_pred->to_string() : "CROSS") << ")"
                << " [est " << card_str(p->cardinality) << " rows, cost "
                << (int64_t)p->cost << "]\n";
            oss << explain_plan(p->left.get(),  indent + 1);
            oss << explain_plan(p->right.get(), indent + 1);
            break;

        case PlanKind::CROSS_PRODUCT:
            oss << pad << "CrossProduct"
                << " [est " << card_str(p->cardinality) << " rows, cost "
                << (int64_t)p->cost << "]\n";
            oss << explain_plan(p->left.get(),  indent + 1);
            oss << explain_plan(p->right.get(), indent + 1);
            break;

        case PlanKind::PROJECT: {
            oss << pad << "Project(";
            for (int i = 0; i < (int)p->proj_exprs.size(); i++) {
                if (i) oss << ", ";
                oss << p->proj_exprs[i]->to_string();
                if (!p->proj_exprs[i]->alias.empty())
                    oss << " AS " << p->proj_exprs[i]->alias;
            }
            oss << ") [est " << card_str(p->cardinality) << " rows]\n";
            oss << explain_plan(p->left.get(), indent + 1);
            break;
        }

        case PlanKind::GROUPBY:
            oss << pad << "GroupBy(" << p->gb_col << ", "
                << agg_str(p->gb_agg) << "("
                << (p->gb_agg_expr ? p->gb_agg_expr->to_string() : "*") << "))"
                << " [est " << card_str(p->cardinality) << " rows]\n";
            oss << explain_plan(p->left.get(), indent + 1);
            break;

        case PlanKind::LIMIT:
            oss << pad << "Limit(" << p->limit_n << ")"
                << " [est " << card_str(std::min((double)p->limit_n, p->cardinality)) << " rows]\n";
            oss << explain_plan(p->left.get(), indent + 1);
            break;

        case PlanKind::EMPTY:
            oss << pad << "EmptyResult [0 rows]\n";
            break;
    }
    return oss.str();
}
