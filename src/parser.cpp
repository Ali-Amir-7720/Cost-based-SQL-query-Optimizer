#include "parser.h"
#include <cctype>
#include <cstdlib>
#include <stdexcept>
#include <algorithm>
#include <cassert>

// ============================================================
//  Tokeniser
// ============================================================
static bool isident(char c) { return isalnum((unsigned char)c) || c == '_'; }

static TokKind keyword(const std::string& s) {
    std::string u = s;
    for (char& c : u) c = (char)toupper((unsigned char)c);
    if (u == "SELECT") return TokKind::KW_SELECT;
    if (u == "FROM")   return TokKind::KW_FROM;
    if (u == "WHERE")  return TokKind::KW_WHERE;
    if (u == "AND")    return TokKind::KW_AND;
    if (u == "OR")     return TokKind::KW_OR;
    if (u == "GROUP")  return TokKind::KW_GROUP;
    if (u == "BY")     return TokKind::KW_BY;
    if (u == "LIMIT")  return TokKind::KW_LIMIT;
    if (u == "AS")     return TokKind::KW_AS;
    if (u == "SUM")    return TokKind::KW_SUM;
    if (u == "COUNT")  return TokKind::KW_COUNT;
    if (u == "AVG")    return TokKind::KW_AVG;
    if (u == "MIN")    return TokKind::KW_MIN;
    if (u == "MAX")    return TokKind::KW_MAX;
    return TokKind::IDENT;
}

void Parser::tokenise(const std::string& sql) {
    tokens_.clear();
    pos_ = 0;
    int i = 0, n = (int)sql.size();

    auto add = [&](TokKind k, const std::string& lex, int64_t iv = 0, double dv = 0.0) {
        Token t; t.kind = k; t.lexeme = lex; t.ival = iv; t.dval = dv;
        tokens_.push_back(t);
    };

    while (i < n) {
        // Skip whitespace
        if (isspace((unsigned char)sql[i])) { i++; continue; }

        // Line comment
        if (i+1 < n && sql[i] == '-' && sql[i+1] == '-') {
            while (i < n && sql[i] != '\n') i++;
            continue;
        }

        // String literal (single-quoted)
        if (sql[i] == '\'') {
            std::string s;
            i++; // skip opening '
            while (i < n && sql[i] != '\'') {
                if (sql[i] == '\\' && i+1 < n) { i++; s += sql[i++]; }
                else s += sql[i++];
            }
            if (i < n) i++; // skip closing '
            Token t; t.kind = TokKind::STR_LIT; t.lexeme = s;
            tokens_.push_back(t);
            continue;
        }

        // Identifier or keyword
        if (isalpha((unsigned char)sql[i]) || sql[i] == '_') {
            std::string id;
            while (i < n && isident(sql[i])) id += sql[i++];
            TokKind k = keyword(id);
            add(k, id);
            continue;
        }

        // Number literal
        if (isdigit((unsigned char)sql[i]) || (sql[i] == '-' && i+1 < n && isdigit((unsigned char)sql[i+1]))) {
            std::string num;
            if (sql[i] == '-') num += sql[i++];
            bool has_dot = false;
            while (i < n && (isdigit((unsigned char)sql[i]) || (sql[i] == '.' && !has_dot))) {
                if (sql[i] == '.') has_dot = true;
                num += sql[i++];
            }
            if (has_dot) {
                double d = std::stod(num);
                add(TokKind::FLOAT_LIT, num, 0, d);
            } else {
                int64_t v = std::stoll(num);
                add(TokKind::INT_LIT, num, v, 0.0);
            }
            continue;
        }

        // Operators
        char c = sql[i];
        if (c == '=' ) { add(TokKind::OP_EQ,    "=");  i++; continue; }
        if (c == '*' ) { add(TokKind::OP_STAR,   "*");  i++; continue; }
        if (c == '+' ) { add(TokKind::OP_PLUS,   "+");  i++; continue; }
        if (c == '/' ) { add(TokKind::OP_SLASH,  "/");  i++; continue; }
        if (c == '(' ) { add(TokKind::LPAREN,    "(");  i++; continue; }
        if (c == ')' ) { add(TokKind::RPAREN,    ")");  i++; continue; }
        if (c == ',' ) { add(TokKind::COMMA,     ",");  i++; continue; }
        if (c == '.' ) { add(TokKind::DOT,       ".");  i++; continue; }
        if (c == ';' ) { add(TokKind::SEMICOLON, ";");  i++; continue; }
        if (c == '-' ) { add(TokKind::OP_MINUS,  "-");  i++; continue; }
        if (c == '<') {
            if (i+1 < n && sql[i+1] == '=') { add(TokKind::OP_LE, "<="); i+=2; }
            else if (i+1 < n && sql[i+1] == '>') { add(TokKind::OP_NEQ, "<>"); i+=2; }
            else add(TokKind::OP_LT, "<", 0, 0), i++;
            continue;
        }
        if (c == '>') {
            if (i+1 < n && sql[i+1] == '=') { add(TokKind::OP_GE, ">="); i+=2; }
            else add(TokKind::OP_GT, ">"), i++;
            continue;
        }
        if (c == '!') {
            if (i+1 < n && sql[i+1] == '=') { add(TokKind::OP_NEQ, "!="); i+=2; }
            else { i++; } // skip unknown
            continue;
        }
        // Unknown character — skip
        i++;
    }
    Token end; end.kind = TokKind::END; end.lexeme = "";
    tokens_.push_back(end);
}

// ============================================================
//  Token stream helpers
// ============================================================
Token& Parser::peek()  { return tokens_[pos_]; }
Token& Parser::peek2() { return tokens_[std::min(pos_+1, (int)tokens_.size()-1)]; }
Token  Parser::consume() {
    Token t = tokens_[pos_];
    if (pos_ < (int)tokens_.size()-1) pos_++;
    return t;
}
Token Parser::expect(TokKind k) {
    if (tokens_[pos_].kind != k)
        throw std::runtime_error("Parser: unexpected token '" + tokens_[pos_].lexeme + "'");
    return consume();
}
bool Parser::at(TokKind k)  const { return tokens_[pos_].kind == k; }
bool Parser::at2(TokKind k) const {
    int nx = std::min(pos_+1, (int)tokens_.size()-1);
    return tokens_[nx].kind == k;
}

// ============================================================
//  Expression parsing
// ============================================================
static Op tok_to_op(TokKind k) {
    switch (k) {
        case TokKind::OP_EQ:    return Op::EQ;
        case TokKind::OP_NEQ:   return Op::NEQ;
        case TokKind::OP_LT:    return Op::LT;
        case TokKind::OP_LE:    return Op::LE;
        case TokKind::OP_GT:    return Op::GT;
        case TokKind::OP_GE:    return Op::GE;
        case TokKind::OP_STAR:  return Op::MUL;
        case TokKind::OP_PLUS:  return Op::ADD;
        case TokKind::OP_MINUS: return Op::SUB;
        case TokKind::OP_SLASH: return Op::DIV;
        default:                return Op::EQ;
    }
}
static bool is_arith(TokKind k) {
    return k == TokKind::OP_STAR || k == TokKind::OP_PLUS
        || k == TokKind::OP_MINUS || k == TokKind::OP_SLASH;
}
static bool is_cmp(TokKind k) {
    return k == TokKind::OP_EQ  || k == TokKind::OP_NEQ
        || k == TokKind::OP_LT  || k == TokKind::OP_LE
        || k == TokKind::OP_GT  || k == TokKind::OP_GE;
}
static bool is_agg_kw(TokKind k) {
    return k == TokKind::KW_SUM  || k == TokKind::KW_COUNT
        || k == TokKind::KW_AVG  || k == TokKind::KW_MIN
        || k == TokKind::KW_MAX;
}
static AggType kw_to_agg(TokKind k) {
    switch (k) {
        case TokKind::KW_SUM:   return AggType::SUM;
        case TokKind::KW_COUNT: return AggType::COUNT;
        case TokKind::KW_AVG:   return AggType::AVG;
        case TokKind::KW_MIN:   return AggType::MIN;
        case TokKind::KW_MAX:   return AggType::MAX;
        default:                return AggType::SUM;
    }
}

std::unique_ptr<Expr> Parser::parse_primary_expr() {
    auto& tk = peek();

    // Aggregate: SUM(expr), COUNT(*), AVG(col), MIN(col), MAX(col)
    if (is_agg_kw(tk.kind)) {
        TokKind kind = tk.kind;
        consume();
        expect(TokKind::LPAREN);
        auto agg = std::make_unique<Expr>();
        agg->kind = ExprKind::AGGREGATE;
        agg->agg  = kw_to_agg(kind);
        if (at(TokKind::OP_STAR)) {
            consume();
            // COUNT(*) — arg is null
        } else {
            agg->agg_arg = parse_primary_expr();
        }
        expect(TokKind::RPAREN);
        return agg;
    }

    // Literal integer
    if (tk.kind == TokKind::INT_LIT) {
        auto e = std::make_unique<Expr>();
        e->kind = ExprKind::LITERAL;
        e->lit  = Value::from_int(tk.ival);
        consume();
        return e;
    }

    // Literal float
    if (tk.kind == TokKind::FLOAT_LIT) {
        auto e = std::make_unique<Expr>();
        e->kind = ExprKind::LITERAL;
        e->lit  = Value::from_double(tk.dval);
        consume();
        return e;
    }

    // String literal
    if (tk.kind == TokKind::STR_LIT) {
        auto e = std::make_unique<Expr>();
        e->kind = ExprKind::LITERAL;
        e->lit  = Value::from_str(tk.lexeme);
        consume();
        return e;
    }

    // Identifier (possibly qualified: table.col)
    if (tk.kind == TokKind::IDENT) {
        std::string id = tk.lexeme; consume();
        auto e = std::make_unique<Expr>();
        e->kind = ExprKind::COL_REF;
        if (at(TokKind::DOT)) {
            consume(); // consume '.'
            e->tbl = id;
            e->col = expect(TokKind::IDENT).lexeme;
        } else {
            e->col = id;
        }
        return e;
    }

    // Parenthesised expression
    if (tk.kind == TokKind::LPAREN) {
        consume();
        auto e = parse_expr();
        expect(TokKind::RPAREN);
        return e;
    }

    throw std::runtime_error("Parser: unexpected token in expression: '" + tk.lexeme + "'");
}

// parse_expr handles arithmetic operators (* + - /) between primary expressions
std::unique_ptr<Expr> Parser::parse_expr() {
    auto lhs = parse_primary_expr();
    // Handle arithmetic binary ops
    while (is_arith(peek().kind)) {
        Op op = tok_to_op(peek().kind);
        consume();
        auto rhs = parse_primary_expr();
        auto e   = std::make_unique<Expr>();
        e->kind  = ExprKind::BINARY_OP;
        e->op    = op;
        e->lhs   = std::move(lhs);
        e->rhs   = std::move(rhs);
        lhs      = std::move(e);
    }
    return lhs;
}

// parse_pred: parses "expr op expr"
std::unique_ptr<Pred> Parser::parse_pred() {
    auto lhs = parse_expr();
    if (!is_cmp(peek().kind))
        throw std::runtime_error("Parser: expected comparison operator, got '" + peek().lexeme + "'");
    Op op = tok_to_op(peek().kind);
    consume();
    auto rhs = parse_expr();

    auto p = std::make_unique<Pred>();
    p->kind = PredKind::EXPR_OP_EXPR;
    p->op   = op;
    p->lhs  = std::move(lhs);
    p->rhs  = std::move(rhs);
    return p;
}

// ============================================================
//  Clause parsers
// ============================================================
std::vector<std::unique_ptr<Expr>> Parser::parse_select_list() {
    std::vector<std::unique_ptr<Expr>> exprs;

    // SELECT *
    if (at(TokKind::OP_STAR)) {
        consume();
        auto e = std::make_unique<Expr>();
        e->kind = ExprKind::COL_REF;
        e->col  = "*";
        exprs.push_back(std::move(e));
        return exprs;
    }

    // SELECT expr [AS alias] [, ...]
    do {
        if (!exprs.empty()) {
            if (at(TokKind::COMMA)) consume(); else break;
        }
        auto e = parse_expr();
        if (at(TokKind::KW_AS)) {
            consume();
            e->alias = expect(TokKind::IDENT).lexeme;
        }
        exprs.push_back(std::move(e));
    } while (at(TokKind::COMMA));

    return exprs;
}

std::vector<std::string> Parser::parse_from_list() {
    std::vector<std::string> tables;
    do {
        if (!tables.empty()) {
            if (at(TokKind::COMMA)) consume(); else break;
        }
        tables.push_back(expect(TokKind::IDENT).lexeme);
    } while (at(TokKind::COMMA));
    return tables;
}

std::vector<std::unique_ptr<Pred>> Parser::parse_where_clause() {
    std::vector<std::unique_ptr<Pred>> preds;
    preds.push_back(parse_pred());
    while (at(TokKind::KW_AND)) {
        consume();
        preds.push_back(parse_pred());
    }
    return preds;
}

// ============================================================
//  Column resolution
// ============================================================
void Parser::resolve_expr(Expr* e, const std::vector<std::string>& tables) {
    if (!e) return;
    if (e->kind == ExprKind::COL_REF && e->tbl.empty() && e->col != "*") {
        // Find which table has this column
        std::string found_in;
        for (auto& t : tables) {
            auto* tm = cat_->get_table(t);
            if (!tm) continue;
            if (tm->find_col(e->col)) {
                if (!found_in.empty()) { found_in = ""; break; } // ambiguous
                found_in = t;
            }
        }
        if (!found_in.empty()) e->tbl = found_in;
    }
    if (e->lhs) resolve_expr(e->lhs.get(), tables);
    if (e->rhs) resolve_expr(e->rhs.get(), tables);
    if (e->agg_arg) resolve_expr(e->agg_arg.get(), tables);
}

void Parser::resolve_pred(Pred* p, const std::vector<std::string>& tables) {
    if (!p || p->kind != PredKind::EXPR_OP_EXPR) return;
    if (p->lhs) resolve_expr(p->lhs.get(), tables);
    if (p->rhs) resolve_expr(p->rhs.get(), tables);
}

void Parser::resolve_columns(PlanNode* node, const std::vector<std::string>& tables) {
    if (!node) return;
    for (auto& p : node->preds)        resolve_pred(p.get(), tables);
    if (node->join_pred)               resolve_pred(node->join_pred.get(), tables);
    for (auto& e : node->proj_exprs)   resolve_expr(e.get(), tables);
    if (node->gb_agg_expr)             resolve_expr(node->gb_agg_expr.get(), tables);
    if (node->gb_key_expr)             resolve_expr(node->gb_key_expr.get(), tables);
    resolve_columns(node->left.get(),  tables);
    resolve_columns(node->right.get(), tables);
}

// ============================================================
//  Schema attachment (bottom-up)
// ============================================================
void Parser::attach_schemas(PlanNode* node) {
    if (!node) return;
    attach_schemas(node->left.get());
    attach_schemas(node->right.get());

    switch (node->kind) {
        case PlanKind::SCAN:
            node->schema = cat_->make_schema(node->table_name);
            break;

        case PlanKind::FILTER:
            if (node->left) node->schema = node->left->schema;
            break;

        case PlanKind::JOIN:
        case PlanKind::CROSS_PRODUCT: {
            node->schema = node->left ? node->left->schema : Schema{};
            if (node->right)
                for (auto& c : node->right->schema)
                    node->schema.push_back(c);
            break;
        }

        case PlanKind::PROJECT: {
            Schema s;
            bool star = (!node->proj_exprs.empty() &&
                         node->proj_exprs[0]->kind == ExprKind::COL_REF &&
                         node->proj_exprs[0]->col  == "*");
            if (star) {
                s = node->left ? node->left->schema : Schema{};
            } else {
                for (auto& ex : node->proj_exprs) {
                    SchemaCol sc;
                    if (ex->kind == ExprKind::COL_REF)  {
                        sc.table = ex->tbl; sc.name = ex->col;
                        // Look up type in child schema
                        if (node->left) {
                            int idx = find_col(node->left->schema, ex->tbl, ex->col);
                            if (idx >= 0) sc.type = node->left->schema[idx].type;
                        }
                    } else if (ex->kind == ExprKind::AGGREGATE) {
                        sc.name = agg_str(ex->agg);
                        sc.type = ValType::DOUBLE;
                    } else {
                        sc.name = ex->to_string();
                        sc.type = ValType::DOUBLE;
                    }
                    if (!ex->alias.empty()) sc.alias = ex->alias;
                    s.push_back(sc);
                }
            }
            node->schema = s;
            break;
        }

        case PlanKind::GROUPBY: {
            // Schema: group-by column + aggregate column
            Schema s;
            if (node->left) {
                int idx = find_col(node->left->schema, node->gb_table, node->gb_col);
                if (idx >= 0) s.push_back(node->left->schema[idx]);
            }
            SchemaCol agg_col;
            agg_col.name = agg_str(node->gb_agg);
            agg_col.type = ValType::DOUBLE;
            s.push_back(agg_col);
            node->schema = s;
            break;
        }

        case PlanKind::LIMIT:
        case PlanKind::EMPTY:
            node->schema = node->left ? node->left->schema : Schema{};
            break;
    }
}

// ============================================================
//  Build naive plan from parsed components
// ============================================================
std::unique_ptr<PlanNode> Parser::build_naive_plan(
    std::vector<std::unique_ptr<Expr>>  select_exprs,
    const std::vector<std::string>&     tables,
    std::vector<std::unique_ptr<Pred>>  preds,
    const std::string&                  groupby_table,
    const std::string&                  groupby_col,
    AggType                             gb_agg,
    std::unique_ptr<Expr>               gb_agg_expr,
    int64_t                             limit_n,
    bool                                has_groupby,
    bool                                has_limit)
{
    // 1. Build left-deep join tree (all JOINs with null join_pred = cross product)
    assert(!tables.empty());
    std::unique_ptr<PlanNode> join_tree;
    for (auto& tbl : tables) {
        auto scan = std::make_unique<PlanNode>();
        scan->kind       = PlanKind::SCAN;
        scan->table_name = tbl;
        if (!join_tree) {
            join_tree = std::move(scan);
        } else {
            auto join     = std::make_unique<PlanNode>();
            join->kind    = PlanKind::JOIN;  // null join_pred = cross product
            join->left    = std::move(join_tree);
            join->right   = std::move(scan);
            join_tree     = std::move(join);
        }
    }

    // 2. Wrap with Filter (all predicates on top)
    std::unique_ptr<PlanNode> top = std::move(join_tree);
    if (!preds.empty()) {
        auto filter  = std::make_unique<PlanNode>();
        filter->kind = PlanKind::FILTER;
        filter->preds = std::move(preds);
        filter->left = std::move(top);
        top = std::move(filter);
    }

    // 3. Project
    {
        auto proj = std::make_unique<PlanNode>();
        proj->kind = PlanKind::PROJECT;
        proj->proj_exprs = std::move(select_exprs);
        proj->left = std::move(top);
        top = std::move(proj);
    }

    // 4. GroupBy (wraps Project if present)
    if (has_groupby) {
        auto gb   = std::make_unique<PlanNode>();
        gb->kind  = PlanKind::GROUPBY;
        gb->gb_table    = groupby_table;
        gb->gb_col      = groupby_col;
        gb->gb_agg      = gb_agg;
        gb->gb_agg_expr = std::move(gb_agg_expr);
        gb->left  = std::move(top);
        top = std::move(gb);
    }

    // 5. Limit
    if (has_limit) {
        auto lim   = std::make_unique<PlanNode>();
        lim->kind  = PlanKind::LIMIT;
        lim->limit_n = limit_n;
        lim->left  = std::move(top);
        top = std::move(lim);
    }

    return top;
}

// ============================================================
//  Main entry point: parse SQL → PlanNode
// ============================================================
std::unique_ptr<PlanNode> Parser::parse(const std::string& sql, const Catalog& cat) {
    cat_ = &cat;
    tokenise(sql);

    // Consume optional trailing semicolons and skip
    while (at(TokKind::SEMICOLON)) consume();

    expect(TokKind::KW_SELECT);
    auto select_exprs = parse_select_list();

    expect(TokKind::KW_FROM);
    auto tables = parse_from_list();

    std::vector<std::unique_ptr<Pred>> preds;
    if (at(TokKind::KW_WHERE)) {
        consume();
        preds = parse_where_clause();
    }

    // GROUP BY
    std::string gb_table, gb_col;
    AggType     gb_agg = AggType::SUM;
    std::unique_ptr<Expr> gb_agg_expr;
    bool has_groupby = false;
    if (at(TokKind::KW_GROUP)) {
        consume();
        expect(TokKind::KW_BY);
        has_groupby = true;
        // Parse group-by column (possibly qualified)
        std::string id = expect(TokKind::IDENT).lexeme;
        if (at(TokKind::DOT)) {
            consume();
            gb_table = id;
            gb_col   = expect(TokKind::IDENT).lexeme;
        } else {
            gb_col = id;
        }
        // Locate aggregate expression in select list
        for (auto& ex : select_exprs) {
            if (ex->kind == ExprKind::AGGREGATE) {
                gb_agg      = ex->agg;
                gb_agg_expr = clone_expr(ex->agg_arg.get());
                break;
            }
        }
    }

    // LIMIT
    int64_t limit_n   = 0;
    bool    has_limit = false;
    if (at(TokKind::KW_LIMIT)) {
        consume();
        limit_n   = expect(TokKind::INT_LIT).ival;
        has_limit = true;
    }

    // Build the naive plan
    auto plan = build_naive_plan(
        std::move(select_exprs), tables, std::move(preds),
        gb_table, gb_col, gb_agg, std::move(gb_agg_expr),
        limit_n, has_groupby, has_limit);

    // Resolve unqualified column names against the catalog
    resolve_columns(plan.get(), tables);

    // Attach schemas bottom-up
    attach_schemas(plan.get());

    return plan;
}
