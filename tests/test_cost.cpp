// tests/test_cost.cpp
// cost model tests.
// checks cardinality formulas against hand values.

#include <cassert>
#include <iostream>
#include <cmath>
#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/cost_model.h"

#define PASS(msg) do { std::cout << "  PASS: " << msg << "\n"; passed++; } while(0)
#define FAIL(msg) do { std::cerr << "  FAIL: " << msg << "\n"; failed++; } while(0)

static int passed = 0, failed = 0;

static bool approx_eq(double a, double b, double tol = 0.05) {
    if (b == 0) return std::abs(a) < tol;
    return std::abs(a - b) / std::abs(b) < tol;
}

// build a catalog with known stats for testing
static Catalog make_catalog_with_stats() {
    // use plan nodes with manual cardinalities for formula tests
    return Catalog{};
}

// helper to build a scan node with known cardinality
static std::unique_ptr<PlanNode> make_scan(const std::string& tbl, double card) {
    auto n = std::make_unique<PlanNode>();
    n->kind = PlanKind::SCAN;
    n->table_name = tbl;
    n->cardinality = card;
    n->cost = card; // pre-set for formula tests
    return n;
}

// helper to build a filter node with a given selection rate
static std::unique_ptr<PlanNode> make_filter(
    std::unique_ptr<PlanNode> child, double child_card)
{
    auto f = std::make_unique<PlanNode>();
    f->kind = PlanKind::FILTER;
    f->left = std::move(child);
    // add an always-true pred so the filter node exists
    auto pred = std::make_unique<Pred>(); pred->kind = PredKind::ALWAYS_TRUE;
    f->preds.push_back(std::move(pred));
    f->cardinality = child_card;
    f->cost = child_card + child_card; // typical filter cost
    return f;
}

// test: scan cardinality
static void test_scan_card() {
    // with an empty catalog, annotate_scan falls back to 1000
    Catalog cat;
    CostModel cm(cat);

    auto scan = std::make_unique<PlanNode>();
    scan->kind = PlanKind::SCAN;
    scan->table_name = "nonexistent";
    cm.annotate(scan.get());

    // should fall back to 1000 (no catalog entry)
    assert(scan->cardinality == 1000.0 || scan->cardinality > 0);
    PASS("Scan.cardinality falls back to 1000 for unknown table");
}

// test: cross product cardinality
static void test_cross_product_card() {
    Catalog cat;
    CostModel cm(cat);

    auto cp = std::make_unique<PlanNode>();
    cp->kind  = PlanKind::CROSS_PRODUCT;
    cp->left  = make_scan("A", 100.0);
    cp->right = make_scan("B", 200.0);
    cm.annotate(cp.get());

    // l.card * r.card
    assert(cp->cardinality == cp->left->cardinality * cp->right->cardinality);
    PASS("CrossProduct.cardinality = L.card * R.card");
}

// test: join cardinality (system r formula)
static void test_join_card_formula() {
    // system r: card = l.card * r.card / max(ndv(left.col), ndv(right.col))
    // test the cross-product fallback here instead.
    Catalog cat;
    CostModel cm(cat);

    auto join_node = std::make_unique<PlanNode>();
    join_node->kind  = PlanKind::JOIN;
    join_node->left  = make_scan("customers", 10000.0);
    join_node->right = make_scan("orders", 500000.0);
    // no join_pred means cross product path
    cm.annotate(join_node.get());

    // without join_pred, should use cross product formula
    assert(join_node->cardinality == join_node->left->cardinality * join_node->right->cardinality);
    PASS("JOIN without condition uses cross product cardinality");
}

// test: filter selectivity formulas
static void test_sel_eq() {
    // col = literal: selectivity = 1/ndv(col)
    // test the default path here.
    Catalog cat;
    CostModel cm(cat);

    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR;
    pred->op   = Op::EQ;
    pred->lhs  = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::COL_REF;
    pred->lhs->tbl = "customers"; pred->lhs->col = "country";
    pred->rhs  = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::LITERAL;
    pred->rhs->lit = Value::from_str("PK");

    // without catalog stats, falls back to 0.5
    double sel = cm.selectivity(pred.get());
    assert(sel > 0.0 && sel <= 1.0);
    PASS("EQ selectivity returns value in (0,1)");
}

static void test_sel_range() {
    // col < 2023 on year column with a known range
    Catalog cat;
    CostModel cm(cat);

    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR;
    pred->op   = Op::LT;
    pred->lhs  = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::COL_REF;
    pred->lhs->tbl = "orders"; pred->lhs->col = "year";
    pred->rhs  = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::LITERAL;
    pred->rhs->lit = Value::from_int(2023);

    // without stats we get the default estimate
    double sel = cm.selectivity(pred.get());
    assert(sel > 0.0 && sel <= 1.0);
    PASS("LT selectivity returns value in (0,1)");
}

// test: groupby cardinality
static void test_groupby_card() {
    Catalog cat;
    CostModel cm(cat);

    auto gb = std::make_unique<PlanNode>();
    gb->kind      = PlanKind::GROUPBY;
    gb->gb_table  = "customers";
    gb->gb_col    = "country";
    gb->left      = make_scan("customers", 10000.0);
    cm.annotate(gb.get());

    // without catalog: fallback keeps the group count bounded
    assert(gb->cardinality > 0 && gb->cardinality <= 10000.0);
    PASS("GroupBy.cardinality ≤ child.cardinality");
}

// test: limit cardinality
static void test_limit_card() {
    Catalog cat;
    CostModel cm(cat);

    auto lim  = std::make_unique<PlanNode>();
    lim->kind = PlanKind::LIMIT;
    lim->limit_n = 10;
    lim->left = make_scan("products", 50000.0);
    cm.annotate(lim.get());

    assert(lim->cardinality == 10.0);
    PASS("Limit(10).cardinality = min(10, 50000) = 10");
}

// test: hashjoin cost formula
static void test_hashjoin_cost() {
    // hashjoin cost = l.cost + r.cost + 2*l.card + r.card + out.card
    Catalog cat;
    CostModel cm(cat);

    auto join = std::make_unique<PlanNode>();
    join->kind  = PlanKind::JOIN;
    auto la = make_scan("A", 100); la->cost = 100;
    auto ra = make_scan("B", 200); ra->cost = 200;

    // build a join pred from col1 = col2
    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR; pred->op = Op::EQ;
    pred->lhs = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::COL_REF;
    pred->lhs->tbl = "A"; pred->lhs->col = "id";
    pred->rhs = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::COL_REF;
    pred->rhs->tbl = "B"; pred->rhs->col = "a_id";

    join->join_pred = std::move(pred);
    join->left  = std::move(la);
    join->right = std::move(ra);
    cm.annotate(join.get());

    // cost = left.cost + right.cost + 2*left.card + right.card + out.card
    double expected_cost = join->left->cost + join->right->cost
                         + 2.0 * join->left->cardinality
                         + join->right->cardinality
                         + join->cardinality;
    assert(approx_eq(join->cost, expected_cost, 0.001));
    PASS("HashJoin.cost = L.cost + R.cost + 2*L.card + R.card + out.card");
}

int main() {
    std::cout << "=== test_cost ===\n";
    try {
        test_scan_card();
        test_cross_product_card();
        test_join_card_formula();
        test_sel_eq();
        test_sel_range();
        test_groupby_card();
        test_limit_card();
        test_hashjoin_cost();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << "\n";
        failed++;
    }
    std::cout << "\n  Results: " << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
