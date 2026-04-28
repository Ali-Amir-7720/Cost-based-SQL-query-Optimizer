// tests/test_parser.cpp
// parser tests.
// checks ast structure for representative queries.

#include <cassert>
#include <iostream>
#include <stdexcept>
#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/parser.h"

#define PASS(msg) do { std::cout << "  PASS: " << msg << "\n"; passed++; } while(0)
#define FAIL(msg) do { std::cerr << "  FAIL: " << msg << "\n"; failed++; } while(0)

static int passed = 0, failed = 0;

// build a minimal catalog for testing
static Catalog make_test_catalog() {
    // use an empty catalog for parser tests
    return Catalog{};
}

// helper: find first node of a given kind in the tree
static const PlanNode* find_node(const PlanNode* p, PlanKind k) {
    if (!p) return nullptr;
    if (p->kind == k) return p;
    auto* l = find_node(p->left.get(), k);
    if (l) return l;
    return find_node(p->right.get(), k);
}

static void test_simple_select() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse("SELECT id FROM customers", cat);
    assert(plan != nullptr);
    // should have a project node at root
    assert(plan->kind == PlanKind::PROJECT);
    // with a scan below
    auto* scan = find_node(plan.get(), PlanKind::SCAN);
    assert(scan != nullptr);
    assert(scan->table_name == "customers");
    PASS("simple SELECT id FROM customers");
}

static void test_select_star() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse("SELECT * FROM orders", cat);
    assert(plan != nullptr);
    assert(plan->kind == PlanKind::PROJECT);
    assert(!plan->proj_exprs.empty());
    assert(plan->proj_exprs[0]->col == "*");
    PASS("SELECT * produces star expr");
}

static void test_two_table_join() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT * FROM customers, orders WHERE customers.id = orders.customer_id", cat);
    assert(plan != nullptr);
    // there should be a join node
    auto* join = find_node(plan.get(), PlanKind::JOIN);
    assert(join != nullptr);
    PASS("2-table join produces JOIN node");
}

static void test_where_filter() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT * FROM customers WHERE customers.country = 'PK'", cat);
    assert(plan != nullptr);
    auto* filter = find_node(plan.get(), PlanKind::FILTER);
    assert(filter != nullptr);
    assert(!filter->preds.empty());
    auto& pred = filter->preds[0];
    assert(pred->op == Op::EQ);
    PASS("WHERE predicate produces FILTER with EQ pred");
}

static void test_and_predicates() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT * FROM orders WHERE orders.year = 2024 AND orders.total > 100", cat);
    assert(plan != nullptr);
    auto* filter = find_node(plan.get(), PlanKind::FILTER);
    assert(filter != nullptr);
    assert(filter->preds.size() == 2);
    PASS("AND predicates produce 2 conjuncts in FILTER");
}

static void test_group_by() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT country, COUNT(id) FROM customers GROUP BY country", cat);
    assert(plan != nullptr);
    auto* gb = find_node(plan.get(), PlanKind::GROUPBY);
    assert(gb != nullptr);
    assert(gb->gb_col == "country");
    assert(gb->gb_agg == AggType::COUNT);
    PASS("GROUP BY produces GROUPBY node");
}

static void test_limit() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse("SELECT * FROM products LIMIT 10", cat);
    assert(plan != nullptr);
    auto* lim = find_node(plan.get(), PlanKind::LIMIT);
    assert(lim != nullptr);
    assert(lim->limit_n == 10);
    PASS("LIMIT 10 produces LIMIT node with n=10");
}

static void test_arithmetic_expr() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT line_items.qty * line_items.price FROM line_items", cat);
    assert(plan != nullptr);
    assert(plan->kind == PlanKind::PROJECT);
    assert(!plan->proj_exprs.empty());
    assert(plan->proj_exprs[0]->kind == ExprKind::BINARY_OP);
    assert(plan->proj_exprs[0]->op == Op::MUL);
    PASS("qty * price produces BINARY_OP(MUL) expr");
}

static void test_aggregate_sum() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT SUM(orders.total) FROM orders GROUP BY orders.year", cat);
    assert(plan != nullptr);
    auto* gb = find_node(plan.get(), PlanKind::GROUPBY);
    assert(gb != nullptr);
    assert(gb->gb_agg == AggType::SUM);
    PASS("SUM aggregate in GROUP BY");
}

static void test_three_table() {
    Catalog cat;
    Parser  parser;
    auto plan = parser.parse(
        "SELECT * FROM customers, orders, line_items "
        "WHERE customers.id = orders.customer_id "
        "AND orders.id = line_items.order_id", cat);
    assert(plan != nullptr);
    // should have 3 scan nodes
    auto tables = collect_tables(plan.get());
    assert(tables.size() == 3);
    PASS("3-table query has exactly 3 Scan nodes");
}

static void test_parse_error() {
    Catalog cat;
    Parser  parser;
    bool threw = false;
    try { parser.parse("SELECT FROM WHERE", cat); }
    catch (std::runtime_error&) { threw = true; }
    assert(threw);
    PASS("Malformed SQL throws runtime_error");
}

int main() {
    std::cout << "=== test_parser ===\n";
    try {
        test_simple_select();
        test_select_star();
        test_two_table_join();
        test_where_filter();
        test_and_predicates();
        test_group_by();
        test_limit();
        test_arithmetic_expr();
        test_aggregate_sum();
        test_three_table();
        test_parse_error();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << "\n";
        failed++;
    }
    std::cout << "\n  Results: " << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
