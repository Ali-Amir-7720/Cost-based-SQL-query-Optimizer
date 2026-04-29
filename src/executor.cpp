#include "executor.h"
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <algorithm>
#include <cassert>
#include <cmath>
#include <iostream>

// ============================================================
//  Expression evaluator
// ============================================================
Value Executor::eval_expr(const Expr* e, const Row& row, const Schema& schema) {
    if (!e) return Value::null_val();
    switch (e->kind) {
        case ExprKind::COL_REF: {
            int idx = find_col(schema, e->tbl, e->col);
            if (idx < 0 || idx >= (int)row.size()) return Value::null_val();
            return row[idx];
        }
        case ExprKind::LITERAL:
            return e->lit;

        case ExprKind::BINARY_OP: {
            Value lv = eval_expr(e->lhs.get(), row, schema);
            Value rv = eval_expr(e->rhs.get(), row, schema);
            switch (e->op) {
                case Op::MUL: {
                    double r = lv.as_numeric() * rv.as_numeric();
                    return (lv.is_int() && rv.is_int())
                        ? Value::from_int((int64_t)r)
                        : Value::from_double(r);
                }
                case Op::ADD: {
                    double r = lv.as_numeric() + rv.as_numeric();
                    return (lv.is_int() && rv.is_int())
                        ? Value::from_int((int64_t)r)
                        : Value::from_double(r);
                }
                case Op::SUB: {
                    double r = lv.as_numeric() - rv.as_numeric();
                    return (lv.is_int() && rv.is_int())
                        ? Value::from_int((int64_t)r)
                        : Value::from_double(r);
                }
                case Op::DIV: {
                    double dv = rv.as_numeric();
                    if (dv == 0.0) return Value::null_val();
                    return Value::from_double(lv.as_numeric() / dv);
                }
                default: {
                    // Comparison → return bool encoded as INT
                    int cmp = value_cmp(lv, rv);
                    bool result = false;
                    switch (e->op) {
                        case Op::EQ:  result = (cmp == 0); break;
                        case Op::NEQ: result = (cmp != 0); break;
                        case Op::LT:  result = (cmp <  0); break;
                        case Op::LE:  result = (cmp <= 0); break;
                        case Op::GT:  result = (cmp >  0); break;
                        case Op::GE:  result = (cmp >= 0); break;
                        default: break;
                    }
                    return Value::from_int(result ? 1 : 0);
                }
            }
        }
        case ExprKind::AGGREGATE:
            // Aggregates are evaluated inside exec_groupby, not row-by-row
            return Value::null_val();
    }
    return Value::null_val();
}

// ============================================================
//  Predicate evaluator
// ============================================================
bool Executor::eval_pred(const Pred* p, const Row& row, const Schema& schema) {
    if (!p) return true;
    if (p->kind == PredKind::ALWAYS_TRUE)  return true;
    if (p->kind == PredKind::ALWAYS_FALSE) return false;
    Value lv = eval_expr(p->lhs.get(), row, schema);
    Value rv = eval_expr(p->rhs.get(), row, schema);
    int cmp = value_cmp(lv, rv);
    // String equality: compare string fields directly
    if (lv.is_str() && rv.is_str()) {
        switch (p->op) {
            case Op::EQ:  return lv.sval == rv.sval;
            case Op::NEQ: return lv.sval != rv.sval;
            case Op::LT:  return lv.sval <  rv.sval;
            case Op::LE:  return lv.sval <= rv.sval;
            case Op::GT:  return lv.sval >  rv.sval;
            case Op::GE:  return lv.sval >= rv.sval;
            default: return false;
        }
    }
    switch (p->op) {
        case Op::EQ:  return cmp == 0;
        case Op::NEQ: return cmp != 0;
        case Op::LT:  return cmp <  0;
        case Op::LE:  return cmp <= 0;
        case Op::GT:  return cmp >  0;
        case Op::GE:  return cmp >= 0;
        default: return false;
    }
}

// ============================================================
//  Instrumentation helper
// ============================================================
void Executor::record(const std::string& label, double est, int64_t actual) {
    ActualStats as;
    as.node_label  = label;
    as.est_rows    = est;
    as.actual_rows = actual;
    stats_.push_back(as);
}

// ============================================================
//  Scan — reads the CSV file for the table
// ============================================================
std::vector<Row> Executor::exec_scan(const PlanNode* node) {
    const TableMeta* tm = cat_.get_table(node->table_name);
    if (!tm) throw std::runtime_error("Executor: unknown table '" + node->table_name + "'");

    std::ifstream f(tm->csv_path);
    if (!f) throw std::runtime_error("Executor: cannot open CSV: " + tm->csv_path);

    // Parse header to get column order
    std::string header_line;
    std::getline(f, header_line);
    // Tokenise header
    std::vector<std::string> col_names;
    {
        std::istringstream ss(header_line);
        std::string tok;
        while (std::getline(ss, tok, ',')) {
            // Trim whitespace / quotes
            while (!tok.empty() && (tok.front()==' '||tok.front()=='"')) tok=tok.substr(1);
            while (!tok.empty() && (tok.back() ==' '||tok.back() =='"'||tok.back()=='\r')) tok.pop_back();
            col_names.push_back(tok);
        }
    }

    // Determine which columns in the CSV map to our schema
    //  schema indices → csv column indices
    std::vector<int> col_map(node->schema.size(), -1);
    for (int i = 0; i < (int)node->schema.size(); i++) {
        for (int j = 0; j < (int)col_names.size(); j++) {
            if (col_names[j] == node->schema[i].name) { col_map[i] = j; break; }
        }
    }

    std::vector<Row> rows;
    rows.reserve(tm->row_count > 0 ? (size_t)tm->row_count : 1000);

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty() || line == "\r") continue;
        // Parse CSV fields
        std::vector<std::string> fields;
        {
            std::string cur; bool in_q = false;
            for (char c : line) {
                if (c == '"') in_q = !in_q;
                else if (c == ',' && !in_q) { fields.push_back(cur); cur.clear(); }
                else if (c != '\r') cur += c;
            }
            fields.push_back(cur);
        }
        Row row(node->schema.size());
        for (int i = 0; i < (int)node->schema.size(); i++) {
            int ci = col_map[i];
            if (ci < 0 || ci >= (int)fields.size()) { row[i] = Value::null_val(); continue; }
            const std::string& fv = fields[ci];
            switch (node->schema[i].type) {
                case ValType::INT:
                    try { row[i] = Value::from_int(std::stoll(fv)); }
                    catch (...) { row[i] = Value::null_val(); }
                    break;
                case ValType::DOUBLE:
                    try { row[i] = Value::from_double(std::stod(fv)); }
                    catch (...) { row[i] = Value::null_val(); }
                    break;
                default:
                    row[i] = Value::from_str(fv);
                    break;
            }
        }
        rows.push_back(std::move(row));
    }

    record("Scan(" + node->table_name + ")", node->cardinality, (int64_t)rows.size());
    return rows;
}

// ============================================================
//  Filter
// ============================================================
std::vector<Row> Executor::exec_filter(const PlanNode* node) {
    auto in_rows = execute(node->left.get());
    const Schema& schema = node->left->schema;
    std::vector<Row> result;
    result.reserve(in_rows.size() / 4 + 1);
    for (auto& row : in_rows) {
        bool pass = true;
        for (auto& p : node->preds) {
            if (!eval_pred(p.get(), row, schema)) { pass = false; break; }
        }
        if (pass) result.push_back(row);
    }
    record("Filter", node->cardinality, (int64_t)result.size());
    return result;
}

// ============================================================
//  Project
// ============================================================
std::vector<Row> Executor::exec_project(const PlanNode* node) {
    auto in_rows = execute(node->left.get());
    const Schema& in_schema = node->left->schema;

    // SELECT * shortcut
    bool is_star = (!node->proj_exprs.empty() &&
                    node->proj_exprs[0]->kind == ExprKind::COL_REF &&
                    node->proj_exprs[0]->col  == "*");
    if (is_star) {
        record("Project(*)", node->cardinality, (int64_t)in_rows.size());
        return in_rows;
    }

    std::vector<Row> result;
    result.reserve(in_rows.size());
    for (auto& row : in_rows) {
        Row out;
        out.reserve(node->proj_exprs.size());
        for (auto& expr : node->proj_exprs) {
            // Skip aggregate exprs here — they're handled by GroupBy
            if (expr->kind == ExprKind::AGGREGATE) {
                out.push_back(Value::null_val());
            } else {
                out.push_back(eval_expr(expr.get(), row, in_schema));
            }
        }
        result.push_back(std::move(out));
    }
    record("Project", node->cardinality, (int64_t)result.size());
    return result;
}

// ============================================================
//  HashJoin
// ============================================================
std::vector<Row> Executor::exec_join(const PlanNode* node) {
    // Cross product case
    if (!node->join_pred) return exec_cross_product(node);

    auto left_rows  = execute(node->left.get());
    auto right_rows = execute(node->right.get());
    const Schema& ls = node->left->schema;
    const Schema& rs = node->right->schema;

    // Determine left and right key column indices
    const Pred* jp = node->join_pred.get();
    assert(jp->lhs && jp->rhs);
    assert(jp->lhs->kind == ExprKind::COL_REF);
    assert(jp->rhs->kind == ExprKind::COL_REF);

    int lkey = find_col(ls, jp->lhs->tbl, jp->lhs->col);
    int rkey = find_col(rs, jp->rhs->tbl, jp->rhs->col);
    if (lkey < 0 && rkey < 0) {
        // Try swapped orientation
        lkey = find_col(ls, jp->rhs->tbl, jp->rhs->col);
        rkey = find_col(rs, jp->lhs->tbl, jp->lhs->col);
    }
    if (lkey < 0 || rkey < 0) {
        // Fallback: nested-loop cross product with predicate filter
        return exec_cross_product(node);
    }

    // Build hash table on left (smaller should be build side)
    std::unordered_map<std::string, std::vector<size_t>> ht;
    ht.reserve(left_rows.size());
    for (size_t i = 0; i < left_rows.size(); i++) {
        std::string key = left_rows[i][lkey].to_string();
        ht[key].push_back(i);
    }

    std::vector<Row> result;
    result.reserve(std::min((int64_t)left_rows.size(), (int64_t)right_rows.size()));

    for (auto& rr : right_rows) {
        if (rkey >= (int)rr.size()) continue;
        auto it = ht.find(rr[rkey].to_string());
        if (it == ht.end()) continue;
        for (size_t li : it->second) {
            Row out = left_rows[li];
            out.insert(out.end(), rr.begin(), rr.end());
            result.push_back(std::move(out));
        }
    }

    record("HashJoin", node->cardinality, (int64_t)result.size());
    return result;
}

// ============================================================
//  CrossProduct — nested loop (intentionally slow)
// ============================================================
std::vector<Row> Executor::exec_cross_product(const PlanNode* node) {
    auto left_rows  = execute(node->left.get());
    auto right_rows = execute(node->right.get());

    const Schema& ls = node->left->schema;
    const Schema& rs = node->right->schema;
    // Build combined schema for predicate eval
    Schema combined = ls;
    for (auto& c : rs) combined.push_back(c);

    std::vector<Row> result;
    // Safety limit to prevent system freeze on naive plan cross-products:
    double total_rows = (double)left_rows.size() * (double)right_rows.size();
    if (total_rows > 10000000.0) {
        throw std::runtime_error("Cross product exceeded 10M rows. Naive plan timed out / out-of-memory.");
    }
    result.reserve(left_rows.size() * right_rows.size());
    for (auto& lr : left_rows) {
        for (auto& rr : right_rows) {
            Row combined_row = lr;
            combined_row.insert(combined_row.end(), rr.begin(), rr.end());
            // Evaluate join predicate if present (for cross product fallback)
            if (node->join_pred &&
                !eval_pred(node->join_pred.get(), combined_row, combined))
                continue;
            result.push_back(std::move(combined_row));
        }
    }
    record("CrossProduct", node->cardinality, (int64_t)result.size());
    return result;
}

// ============================================================
//  GroupBy — hash aggregation
// ============================================================
std::vector<Row> Executor::exec_groupby(const PlanNode* node) {
    auto in_rows = execute(node->left.get());
    const Schema& schema = node->left->schema;

    int key_idx = find_col(schema, node->gb_table, node->gb_col);

    struct AggState {
        double   sum   = 0.0;
        int64_t  count = 0;
        double   min_v = std::numeric_limits<double>::max();
        double   max_v = std::numeric_limits<double>::lowest();
        std::string min_s, max_s;
        Value    key_val;
    };

    std::vector<std::string>                key_order;
    std::unordered_map<std::string, AggState> groups;

    for (auto& row : in_rows) {
        std::string gkey;
        Value kv;
        if (key_idx >= 0 && key_idx < (int)row.size()) {
            kv = row[key_idx];
            gkey = kv.to_string();
        } else {
            gkey = "__all__";
        }

        if (groups.find(gkey) == groups.end()) {
            key_order.push_back(gkey);
            groups[gkey].key_val = kv;
        }
        auto& st = groups[gkey];
        st.count++;

        double agg_val = 0.0;
        std::string agg_str_val;
        bool have_str = false;

        if (node->gb_agg_expr) {
            Value av = eval_expr(node->gb_agg_expr.get(), row, schema);
            if (!av.is_null()) {
                agg_val = av.as_numeric();
                if (av.is_str()) { agg_str_val = av.sval; have_str = true; }
            }
        }

        st.sum += agg_val;
        if (agg_val < st.min_v) st.min_v = agg_val;
        if (agg_val > st.max_v) st.max_v = agg_val;
        if (have_str) {
            if (st.min_s.empty() || agg_str_val < st.min_s) st.min_s = agg_str_val;
            if (st.max_s.empty() || agg_str_val > st.max_s) st.max_s = agg_str_val;
        }
    }

    std::vector<Row> result;
    result.reserve(groups.size());

    for (auto& gk : key_order) {
        auto& st = groups[gk];
        Row out;
        out.push_back(st.key_val);   // group-by key column

        Value agg_result;
        switch (node->gb_agg) {
            case AggType::SUM:   agg_result = Value::from_double(st.sum);   break;
            case AggType::COUNT: agg_result = Value::from_int(st.count);    break;
            case AggType::AVG:   agg_result = (st.count > 0)
                                     ? Value::from_double(st.sum / st.count)
                                     : Value::null_val();
                                 break;
            case AggType::MIN:   agg_result = Value::from_double(st.min_v); break;
            case AggType::MAX:   agg_result = Value::from_double(st.max_v); break;
        }
        out.push_back(agg_result);
        result.push_back(std::move(out));
    }

    record("GroupBy", node->cardinality, (int64_t)result.size());
    return result;
}

// ============================================================
//  Limit
// ============================================================
std::vector<Row> Executor::exec_limit(const PlanNode* node) {
    auto in_rows = execute(node->left.get());
    if ((int64_t)in_rows.size() > node->limit_n)
        in_rows.resize((size_t)node->limit_n);
    record("Limit", node->cardinality, (int64_t)in_rows.size());
    return in_rows;
}

// ============================================================
//  Dispatch
// ============================================================
std::vector<Row> Executor::execute(const PlanNode* node) {
    if (!node) return {};
    switch (node->kind) {
        case PlanKind::SCAN:          return exec_scan(node);
        case PlanKind::FILTER:        return exec_filter(node);
        case PlanKind::PROJECT:       return exec_project(node);
        case PlanKind::JOIN:          return exec_join(node);
        case PlanKind::CROSS_PRODUCT: return exec_cross_product(node);
        case PlanKind::GROUPBY:       return exec_groupby(node);
        case PlanKind::LIMIT:         return exec_limit(node);
        case PlanKind::EMPTY:         return {};
    }
    return {};
}
