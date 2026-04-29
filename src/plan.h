#pragma once
#include <string>
#include <vector>
#include <memory>
#include <sstream>
#include <algorithm>
#include <stdexcept>
#include <cmath>
#include <cstdint>
enum class ValType { INT, DOUBLE, TEXT, BOOL, NUL };

struct Value {
    ValType     type  = ValType::NUL;
    int64_t     ival  = 0;
    double      dval  = 0.0;
    std::string sval;

    bool is_null()   const { return type == ValType::NUL;    }
    bool is_int()    const { return type == ValType::INT;    }
    bool is_double() const { return type == ValType::DOUBLE; }
    bool is_str()    const { return type == ValType::TEXT;   }
    bool is_bool()   const { return type == ValType::BOOL;   }

    static Value from_int(int64_t v)           { Value r; r.type=ValType::INT;    r.ival=v;          return r; }
    static Value from_double(double v)         { Value r; r.type=ValType::DOUBLE; r.dval=v;          return r; }
    static Value from_str(const std::string& s){ Value r; r.type=ValType::TEXT;   r.sval=s;          return r; }
    static Value from_bool(bool b)             { Value r; r.type=ValType::BOOL;   r.ival=(b?1:0);    return r; }
    static Value null_val()                    { return Value{}; }

    double as_numeric() const {
        if (type == ValType::INT)    return static_cast<double>(ival);
        if (type == ValType::DOUBLE) return dval;
        if (type == ValType::BOOL)   return static_cast<double>(ival);
        return 0.0;
    }

    std::string to_string() const {
        switch (type) {
            case ValType::INT:    return std::to_string(ival);
            case ValType::DOUBLE: { std::ostringstream oss; oss << dval; return oss.str(); }
            case ValType::TEXT:   return sval;
            case ValType::BOOL:   return ival ? "true" : "false";
            default:              return "NULL";
        }
    }
};

inline int value_cmp(const Value& a, const Value& b) {
    if (a.is_str() && b.is_str()) {
        if (a.sval < b.sval) return -1;
        if (a.sval > b.sval) return  1;
        return 0;
    }
    double da = a.as_numeric(), db = b.as_numeric();
    if (da < db) return -1;
    if (da > db) return  1;
    return 0;
}

inline bool values_equal(const Value& a, const Value& b) {
    if (a.type == ValType::TEXT && b.type == ValType::TEXT) return a.sval == b.sval;
    return value_cmp(a, b) == 0;
}

struct SchemaCol {
    std::string table;   
    std::string name;    
    std::string alias;  
    ValType     type = ValType::TEXT;
};

using Row    = std::vector<Value>;
using Schema = std::vector<SchemaCol>;

// Find column index in schema. Prefers exact table+col match.
// Returns -1 if not found.
inline int find_col(const Schema& schema, const std::string& tbl, const std::string& col) {
    int fallback = -1;
    for (int i = 0; i < (int)schema.size(); i++) {
        if (schema[i].name == col || schema[i].alias == col) {
            if (!tbl.empty() && (schema[i].table == tbl)) return i;
            if (tbl.empty() && fallback == -1) fallback = i;
        }
    }
    if (!tbl.empty()) {
        // try alias match on table
        for (int i = 0; i < (int)schema.size(); i++) {
            if ((schema[i].name == col || schema[i].alias == col) && schema[i].table == tbl)
                return i;
        }
    }
    return fallback;
}

// ============================================================
//  Operators / enums
// ============================================================
enum class Op      { EQ, NEQ, LT, LE, GT, GE, MUL, ADD, SUB, DIV };
enum class AggType { SUM, COUNT, AVG, MIN, MAX };
enum class ExprKind{ COL_REF, LITERAL, BINARY_OP, AGGREGATE };
enum class PredKind{ EXPR_OP_EXPR, ALWAYS_TRUE, ALWAYS_FALSE };
enum class PlanKind{ SCAN, FILTER, JOIN, CROSS_PRODUCT, PROJECT, GROUPBY, LIMIT, EMPTY };

inline std::string op_str(Op op) {
    switch (op) {
        case Op::EQ:  return "=";   case Op::NEQ: return "!=";
        case Op::LT:  return "<";   case Op::LE:  return "<=";
        case Op::GT:  return ">";   case Op::GE:  return ">=";
        case Op::MUL: return "*";   case Op::ADD: return "+";
        case Op::SUB: return "-";   case Op::DIV: return "/";
    }
    return "?";
}

inline std::string agg_str(AggType a) {
    switch(a){
        case AggType::SUM:   return "SUM";
        case AggType::COUNT: return "COUNT";
        case AggType::AVG:   return "AVG";
        case AggType::MIN:   return "MIN";
        case AggType::MAX:   return "MAX";
    }
    return "?";
}

// ============================================================
//  Expr
// ============================================================
struct Expr {
    ExprKind kind = ExprKind::LITERAL;

    // COL_REF
    std::string tbl, col;

    // LITERAL
    Value lit;

    // BINARY_OP
    Op op = Op::EQ;
    std::unique_ptr<Expr> lhs, rhs;

    // AGGREGATE
    AggType agg = AggType::SUM;
    std::unique_ptr<Expr> agg_arg;

    // display alias
    std::string alias;

    Expr() = default;
    Expr(const Expr&) = delete;
    Expr& operator=(const Expr&) = delete;

    std::string to_string() const {
        switch (kind) {
            case ExprKind::COL_REF:
                return tbl.empty() ? col : tbl + "." + col;
            case ExprKind::LITERAL:
                return lit.to_string();
            case ExprKind::BINARY_OP:
                return "(" + (lhs ? lhs->to_string() : "?") +
                       " " + op_str(op) + " " +
                       (rhs ? rhs->to_string() : "?") + ")";
            case ExprKind::AGGREGATE:
                return agg_str(agg) + "(" +
                       (agg_arg ? agg_arg->to_string() : "*") + ")";
        }
        return "?";
    }

    // Collect all table names referenced
    void collect_tables(std::vector<std::string>& out) const {
        switch (kind) {
            case ExprKind::COL_REF:
                if (!tbl.empty()) out.push_back(tbl);
                break;
            case ExprKind::BINARY_OP:
                if (lhs) lhs->collect_tables(out);
                if (rhs) rhs->collect_tables(out);
                break;
            case ExprKind::AGGREGATE:
                if (agg_arg) agg_arg->collect_tables(out);
                break;
            default: break;
        }
    }

    // True if expression is a pure literal (no column refs)
    bool is_literal() const {
        switch (kind) {
            case ExprKind::LITERAL: return true;
            case ExprKind::COL_REF: return false;
            case ExprKind::BINARY_OP:
                return (!lhs || lhs->is_literal()) && (!rhs || rhs->is_literal());
            case ExprKind::AGGREGATE: return false;
        }
        return false;
    }
};

std::unique_ptr<Expr> clone_expr(const Expr* e);

// ============================================================
//  Pred — a single comparison (part of a WHERE AND-chain)
// ============================================================
struct Pred {
    PredKind kind = PredKind::EXPR_OP_EXPR;
    std::unique_ptr<Expr> lhs, rhs;
    Op op = Op::EQ;

    Pred() = default;
    Pred(const Pred&) = delete;
    Pred& operator=(const Pred&) = delete;

    // Collect all table names referenced by both sides
    std::vector<std::string> referenced_tables() const {
        std::vector<std::string> tbls;
        if (lhs) lhs->collect_tables(tbls);
        if (rhs) rhs->collect_tables(tbls);
        // Deduplicate
        std::sort(tbls.begin(), tbls.end());
        tbls.erase(std::unique(tbls.begin(), tbls.end()), tbls.end());
        return tbls;
    }

    // True if all referenced tables are within the given set
    bool references_only(const std::vector<std::string>& tables) const {
        auto rtbls = referenced_tables();
        for (auto& t : rtbls)
            if (std::find(tables.begin(), tables.end(), t) == tables.end()) return false;
        return true;
    }

    // True if pred spans both left_tables and right_tables (equijoin condition)
    bool is_join_pred(const std::vector<std::string>& left_tables,
                      const std::vector<std::string>& right_tables) const {
        if (kind != PredKind::EXPR_OP_EXPR) return false;
        if (op != Op::EQ) return false;
        if (!lhs || lhs->kind != ExprKind::COL_REF) return false;
        if (!rhs || rhs->kind != ExprKind::COL_REF) return false;
        bool lhs_in_left  = std::find(left_tables.begin(),  left_tables.end(),  lhs->tbl) != left_tables.end();
        bool rhs_in_right = std::find(right_tables.begin(), right_tables.end(), rhs->tbl) != right_tables.end();
        bool lhs_in_right = std::find(right_tables.begin(), right_tables.end(), lhs->tbl) != right_tables.end();
        bool rhs_in_left  = std::find(left_tables.begin(),  left_tables.end(),  rhs->tbl) != left_tables.end();
        return (lhs_in_left && rhs_in_right) || (lhs_in_right && rhs_in_left);
    }

    std::string to_string() const {
        switch (kind) {
            case PredKind::ALWAYS_TRUE:  return "TRUE";
            case PredKind::ALWAYS_FALSE: return "FALSE";
            case PredKind::EXPR_OP_EXPR:
                return (lhs ? lhs->to_string() : "?") +
                       " " + op_str(op) + " " +
                       (rhs ? rhs->to_string() : "?");
        }
        return "?";
    }
};

std::unique_ptr<Pred> clone_pred(const Pred* p);

// ============================================================
//  PlanNode — single node in the logical/physical plan tree
// ============================================================
struct PlanNode {
    PlanKind kind        = PlanKind::SCAN;
    Schema   schema;            // output schema of this operator
    double   cardinality = 0.0; // estimated output row count
    double   cost        = 0.0; // estimated total cost

    // ---- SCAN ----
    std::string table_name;

    // ---- FILTER / JOIN / PROJECT / GROUPBY / LIMIT ----
    std::unique_ptr<PlanNode> left;   // primary child
    std::unique_ptr<PlanNode> right;  // right child (JOIN only)

    // ---- FILTER ----
    std::vector<std::unique_ptr<Pred>> preds;  // AND of all conjuncts

    // ---- JOIN / CROSS_PRODUCT ----
    std::unique_ptr<Pred> join_pred;  // null → cross product

    // ---- PROJECT ----
    std::vector<std::unique_ptr<Expr>> proj_exprs;

    // ---- GROUPBY ----
    std::string             gb_table, gb_col;
    AggType                 gb_agg = AggType::SUM;
    std::unique_ptr<Expr>   gb_agg_expr;
    std::unique_ptr<Expr>   gb_key_expr;  // for GROUP BY expression

    // ---- LIMIT ----
    int64_t limit_n = 0;

    PlanNode() = default;
    PlanNode(const PlanNode&) = delete;
    PlanNode& operator=(const PlanNode&) = delete;
};

// Clone a plan tree (deep copy)
std::unique_ptr<PlanNode> clone_plan(const PlanNode* p);

// Collect all table names from Scan nodes
std::vector<std::string> collect_tables(const PlanNode* p);

// Serialize plan to a canonical string (used for fixed-point detection)
std::string plan_to_string(const PlanNode* p);

// Pretty-print plan tree for EXPLAIN
std::string explain_plan(const PlanNode* p, int indent = 0);
