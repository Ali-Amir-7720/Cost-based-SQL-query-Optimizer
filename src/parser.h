#pragma once
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include "plan.h"
#include "catalog.h"
enum class TokKind {
    KW_SELECT, KW_FROM, KW_WHERE, KW_AND, KW_GROUP, KW_BY,
    KW_LIMIT, KW_AS, KW_OR,
    KW_SUM, KW_COUNT, KW_AVG, KW_MIN, KW_MAX,
    IDENT,          // table/column name (possibly table.col)
    INT_LIT,        // integer literal
    FLOAT_LIT,      // floating-point literal
    STR_LIT,        // 'string' literal
    OP_EQ,          // =
    OP_NEQ,         // !=  or  <>
    OP_LT,          // <
    OP_LE,          // <=
    OP_GT,          // >
    OP_GE,          // >=
    OP_STAR,        // * (select all  OR  multiplication)
    OP_PLUS,        // +
    OP_MINUS,       // -
    OP_SLASH,       // /
    LPAREN,         // (
    RPAREN,         // )
    COMMA,          // ,
    DOT,            // .
    SEMICOLON,      // ;
    END             // end of input
};

struct Token {
    TokKind     kind;
    std::string lexeme;   // raw text
    int64_t     ival  = 0;
    double      dval  = 0.0;
};

// ============================================================
//  Parser — hand-written recursive descent
// ============================================================
class Parser {
public:
    // Parse a SQL query string.  Returns the root plan node.
    // On error throws std::runtime_error with a message.
    std::unique_ptr<PlanNode> parse(const std::string& sql, const Catalog& cat);

private:
    // ---- tokeniser state ----
    std::vector<Token> tokens_;
    int                pos_ = 0;

    // ---- catalog reference (for column resolution) ----
    const Catalog* cat_ = nullptr;

    // ---- tokeniser ----
    void tokenise(const std::string& sql);
    Token& peek();
    Token& peek2();
    Token  consume();
    Token  expect(TokKind);
    bool   at(TokKind) const;
    bool   at2(TokKind) const;     // look ahead one more

    // ---- parser helpers ----
    std::string parse_qualified_name(std::string& tbl_out, std::string& col_out);

    // ---- expression / predicate parsing ----
    std::unique_ptr<Expr> parse_expr();
    std::unique_ptr<Expr> parse_primary_expr();
    std::unique_ptr<Pred> parse_pred();

    // ---- clause parsers ----
    std::vector<std::unique_ptr<Expr>> parse_select_list();
    std::vector<std::string>           parse_from_list();
    std::vector<std::unique_ptr<Pred>> parse_where_clause();

    // ---- plan builder ----
    // Builds the naive (unoptimised) plan from parsed components
    std::unique_ptr<PlanNode> build_naive_plan(
        std::vector<std::unique_ptr<Expr>>  select_exprs,
        const std::vector<std::string>&     tables,
        std::vector<std::unique_ptr<Pred>>  preds,
        const std::string&                  groupby_table,
        const std::string&                  groupby_col,
        AggType                             gb_agg,
        std::unique_ptr<Expr>               gb_agg_expr,
        int64_t                             limit_n,
        bool                                has_groupby,
        bool                                has_limit);

    // Resolve unqualified column names using the catalog
    void resolve_columns(PlanNode* node, const std::vector<std::string>& tables);
    void resolve_expr(Expr* e, const std::vector<std::string>& tables);
    void resolve_pred(Pred* p, const std::vector<std::string>& tables);

    // Attach schemas to every node bottom-up
    void attach_schemas(PlanNode* node);
};
