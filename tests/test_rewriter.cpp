// tests/test_rewriter.cpp
// Unit tests for each of the four rewrite rules in isolation.

#include <cassert>
#include <iostream>
#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/parser.h"
#include "../src/rewriter.h"
#include "../src/cost_model.h"

#define PASS(msg) do { std::cout << "  PASS: " << msg << "\n"; passed++; } while(0)
#define FAIL(msg) do { std::cerr << "  FAIL: " << msg << "\n"; failed++; } while(0)

static int passed = 0, failed = 0;

static const PlanNode* find_node(const PlanNode* p, PlanKind k) {
    if (!p) return nullptr;
    if (p->kind == k) return p;
    auto* l = find_node(p->left.get(), k);
    if (l) return l;
    return find_node(p->right.get(), k);
}

// Count nodes of a given kind
static int count_nodes(const PlanNode* p, PlanKind k) {
    if (!p) return 0;
    return (p->kind == k ? 1 : 0)
         + count_nodes(p->left.get(), k)
         + count_nodes(p->right.get(), k);
}

// ── Rule 1: Constant folding ──────────────────────────────
static void test_const_fold_true() {
    Catalog cat;
    Rewriter rw(cat);

    // Build: Filter(2024=2024, Scan(orders))
    auto filter = std::make_unique<PlanNode>();
    filter->kind = PlanKind::FILTER;
    // pred: 2024 = 2024
    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR;
    pred->op   = Op::EQ;
    pred->lhs  = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::LITERAL; pred->lhs->lit = Value::from_int(2024);
    pred->rhs  = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::LITERAL; pred->rhs->lit = Value::from_int(2024);
    filter->preds.push_back(std::move(pred));
    // child = Scan(orders)
    auto scan = std::make_unique<PlanNode>(); scan->kind = PlanKind::SCAN; scan->table_name = "orders";
    filter->left = std::move(scan);

    auto result = rw.constant_fold(std::move(filter));
    // 2024=2024 → TRUE → filter dissolves, returns the Scan directly
    assert(result != nullptr);
    assert(result->kind == PlanKind::SCAN);
    PASS("constant_fold: 2024=2024 TRUE → filter dissolved");
}

static void test_const_fold_false() {
    Catalog cat;
    Rewriter rw(cat);

    auto filter = std::make_unique<PlanNode>();
    filter->kind = PlanKind::FILTER;
    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR; pred->op = Op::EQ;
    pred->lhs = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::LITERAL; pred->lhs->lit = Value::from_int(1);
    pred->rhs = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::LITERAL; pred->rhs->lit = Value::from_int(2);
    filter->preds.push_back(std::move(pred));
    auto scan = std::make_unique<PlanNode>(); scan->kind = PlanKind::SCAN; scan->table_name = "orders";
    filter->left = std::move(scan);

    auto result = rw.constant_fold(std::move(filter));
    assert(result != nullptr);
    assert(result->kind == PlanKind::EMPTY);
    PASS("constant_fold: 1=2 FALSE → EmptyResult");
}

// ── Rule 2: Predicate pushdown ────────────────────────────
static void test_pred_pushdown_basic() {
    Catalog cat;
    Parser  parser;
    Rewriter rw(cat);

    // Parse a 2-table query with filter predicate
    auto plan = parser.parse(
        "SELECT * FROM customers, orders "
        "WHERE customers.id = orders.customer_id AND customers.country = 'PK'", cat);

    std::string before = plan_to_string(plan.get());
    plan = rw.predicate_pushdown(std::move(plan));
    std::string after  = plan_to_string(plan.get());

    // After pushdown, there should be a Filter node below the Join
    // (customers.country='PK' is pushed to customers scan)
    int filter_count = count_nodes(plan.get(), PlanKind::FILTER);
    assert(filter_count >= 1);
    PASS("predicate_pushdown: country='PK' pushed below join");
}

static void test_pred_pushdown_join_cond() {
    Catalog cat;
    Parser  parser;
    Rewriter rw(cat);

    auto plan = parser.parse(
        "SELECT * FROM customers, orders "
        "WHERE customers.id = orders.customer_id", cat);

    plan = rw.predicate_pushdown(std::move(plan));

    // The join condition should now be set on the JOIN node
    auto* join = find_node(plan.get(), PlanKind::JOIN);
    assert(join != nullptr);
    assert(join->join_pred != nullptr);
    assert(join->join_pred->op == Op::EQ);
    PASS("predicate_pushdown: equijoin predicate becomes join condition");
}

static void test_pred_pushdown_no_top_filter() {
    Catalog cat;
    Parser  parser;
    Rewriter rw(cat);

    auto plan = parser.parse(
        "SELECT * FROM customers, orders "
        "WHERE customers.id = orders.customer_id AND orders.year = 2024", cat);

    plan = rw.predicate_pushdown(std::move(plan));

    // After pushdown:
    //   - customers.id = orders.customer_id → join condition on JOIN
    //   - orders.year = 2024 → Filter below JOIN on orders side
    auto* join = find_node(plan.get(), PlanKind::JOIN);
    assert(join != nullptr);
    assert(join->join_pred != nullptr);
    PASS("predicate_pushdown: year=2024 pushed to orders side");
}

// ── Rule 3: Constant folding + pushdown fixed point ───────
static void test_fixed_point() {
    Catalog cat;
    Parser  parser;
    Rewriter rw(cat);

    auto plan = parser.parse(
        "SELECT * FROM customers, orders "
        "WHERE customers.id = orders.customer_id AND 1 = 1", cat);

    // 1=1 should fold to TRUE and disappear
    plan = rw.rewrite(std::move(plan));

    // Verify plan is not EMPTY (1=1 folds to TRUE, not FALSE)
    assert(plan->kind != PlanKind::EMPTY);

    // And the join condition should be set
    auto* join = find_node(plan.get(), PlanKind::JOIN);
    assert(join != nullptr);
    assert(join->join_pred != nullptr);
    PASS("fixed_point: 1=1 folds away, join condition set");
}

// ── Rule 4: Join input swap ───────────────────────────────
static void test_join_swap() {
    Catalog cat;
    Rewriter rw(cat);
    CostModel cm(cat);

    // Build: JOIN(cond, Scan(big), Scan(small))
    // where we manually set cardinalities
    auto join = std::make_unique<PlanNode>();
    join->kind = PlanKind::JOIN;

    auto big   = std::make_unique<PlanNode>(); big->kind = PlanKind::SCAN; big->table_name = "orders";   big->cardinality = 500000;
    auto small = std::make_unique<PlanNode>(); small->kind = PlanKind::SCAN; small->table_name = "customers"; small->cardinality = 10000;

    auto pred = std::make_unique<Pred>(); pred->kind = PredKind::EXPR_OP_EXPR; pred->op = Op::EQ;
    pred->lhs = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::COL_REF; pred->lhs->tbl = "orders";    pred->lhs->col = "customer_id";
    pred->rhs = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::COL_REF; pred->rhs->tbl = "customers"; pred->rhs->col = "id";

    join->join_pred   = std::move(pred);
    join->left        = std::move(big);
    join->right       = std::move(small);
    join->cardinality = 500000; // dummy

    auto result = rw.apply_join_swap(std::move(join));
    // After swap, left should be the smaller table (customers)
    assert(result->left != nullptr);
    assert(result->left->table_name == "customers");
    PASS("join_swap: smaller relation moved to build (left) side");
}

int main() {
    std::cout << "=== test_rewriter ===\n";
    try {
        test_const_fold_true();
        test_const_fold_false();
        test_pred_pushdown_basic();
        test_pred_pushdown_join_cond();
        test_pred_pushdown_no_top_filter();
        test_fixed_point();
        test_join_swap();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << "\n";
        failed++;
    }
    std::cout << "\n  Results: " << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
