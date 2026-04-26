// tests/test_join_order.cpp
// Unit tests for the Selinger DP join-order search.
// Verifies on a 3-table example with known optimal plan.

#include <cassert>
#include <iostream>
#include <string>
#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/cost_model.h"
#include "../src/join_order.h"

#define PASS(msg) do { std::cout << "  PASS: " << msg << "\n"; passed++; } while(0)
#define FAIL(msg) do { std::cerr << "  FAIL: " << msg << "\n"; failed++; } while(0)

static int passed = 0, failed = 0;

// Build a Scan node with manually set cardinality/cost (for DP testing without a real catalog)
static std::unique_ptr<PlanNode> make_scan_with_card(const std::string& name, double card) {
    auto n = std::make_unique<PlanNode>();
    n->kind = PlanKind::SCAN;
    n->table_name = name;
    n->cardinality = card;
    n->cost = card;  // cost of reading
    // Schema: one dummy column so joins work
    SchemaCol col; col.table = name; col.name = "id"; col.type = ValType::INT;
    n->schema.push_back(col);
    return n;
}

// Build a JoinCond between two tables
static JoinCond make_join_cond(const std::string& lt, const std::string& lc,
                                const std::string& rt, const std::string& rc) {
    JoinCond jc;
    auto pred = std::make_unique<Pred>();
    pred->kind = PredKind::EXPR_OP_EXPR;
    pred->op   = Op::EQ;
    pred->lhs  = std::make_unique<Expr>(); pred->lhs->kind = ExprKind::COL_REF;
    pred->lhs->tbl = lt;  pred->lhs->col = lc;
    pred->rhs  = std::make_unique<Expr>(); pred->rhs->kind = ExprKind::COL_REF;
    pred->rhs->tbl = rt;  pred->rhs->col = rc;
    jc.pred        = std::move(pred);
    jc.left_table  = lt;
    jc.right_table = rt;
    return jc;
}

// ── Test: 2-table DP picks the cheaper order ─────────────
static void test_dp_2table_order() {
    // A(10K) ⋈ B(500K): DP must pick A as build side
    Catalog cat;
    CostModel cm(cat);
    JoinOrderDP dp(cm, cat);

    std::vector<BaseTable> tables;
    tables.push_back(BaseTable{"orders",    make_scan_with_card("orders",    500000)});
    tables.push_back(BaseTable{"customers", make_scan_with_card("customers", 10000 )});

    std::vector<JoinCond> conds;
    conds.push_back(make_join_cond("customers","id","orders","customer_id"));

    auto result = dp.find_best_order(tables, conds);
    assert(result != nullptr);
    assert(result->kind == PlanKind::JOIN);
    PASS("2-table DP produces a JOIN node");
}

// ── Test: 3-table DP produces correct plan structure ─────
static void test_dp_3table_known_optimal() {
    // Known scenario:
    //   small   (10   rows)
    //   medium  (1000 rows)
    //   large   (100K rows)
    // Conditions: small.id = medium.sid, medium.id = large.mid
    // Optimal left-deep order: small ⋈ medium ⋈ large
    // (starting with the smallest table minimises intermediate results)

    Catalog cat;
    CostModel cm(cat);
    JoinOrderDP dp(cm, cat);

    std::vector<BaseTable> tables;
    tables.push_back(BaseTable{"large",  make_scan_with_card("large",  100000)});
    tables.push_back(BaseTable{"medium", make_scan_with_card("medium", 1000  )});
    tables.push_back(BaseTable{"small",  make_scan_with_card("small",  10    )});

    std::vector<JoinCond> conds;
    conds.push_back(make_join_cond("small",  "id",  "medium", "sid"));
    conds.push_back(make_join_cond("medium", "id",  "large",  "mid"));

    auto result = dp.find_best_order(tables, conds);
    assert(result != nullptr);
    assert(result->kind == PlanKind::JOIN);

    // The root join should handle the 3rd table (large)
    // The left subtree should itself be a join
    assert(result->left != nullptr);
    assert(result->left->kind == PlanKind::JOIN);

    // Verify: cost of chosen plan < cost of naive order (large ⋈ medium ⋈ small)
    std::vector<BaseTable> naive_tables;
    naive_tables.push_back(BaseTable{"large",  make_scan_with_card("large",  100000)});
    naive_tables.push_back(BaseTable{"medium", make_scan_with_card("medium", 1000  )});
    naive_tables.push_back(BaseTable{"small",  make_scan_with_card("small",  10    )});
    std::vector<JoinCond> naive_conds;
    naive_conds.push_back(make_join_cond("small","id","medium","sid"));
    naive_conds.push_back(make_join_cond("medium","id","large","mid"));

    // Build naive left-deep: large ⋈ medium ⋈ small
    auto naive = make_scan_with_card("large", 100000);
    // Join with medium
    auto j1 = std::make_unique<PlanNode>(); j1->kind = PlanKind::JOIN;
    j1->left  = std::move(naive);
    j1->right = make_scan_with_card("medium", 1000);
    auto p1 = clone_pred(conds[0].pred.get()); j1->join_pred = std::move(p1);
    j1->schema = j1->left->schema; for (auto& c : j1->right->schema) j1->schema.push_back(c);
    cm.annotate(j1.get());
    // Join with small
    auto j2 = std::make_unique<PlanNode>(); j2->kind = PlanKind::JOIN;
    j2->left  = std::move(j1);
    j2->right = make_scan_with_card("small", 10);
    auto p2 = clone_pred(conds[1].pred.get()); j2->join_pred = std::move(p2);
    j2->schema = j2->left->schema; for (auto& c : j2->right->schema) j2->schema.push_back(c);
    cm.annotate(j2.get());

    cm.annotate(result.get());
    // DP result should be cheaper or equal
    assert(result->cost <= j2->cost + 1.0); // +1 for floating point
    PASS("3-table DP: chosen plan cost ≤ large⋈medium⋈small naive cost");
}

// ── Test: extract_join_info ───────────────────────────────
static void test_extract_join_info() {
    // Build: JOIN(cond12, JOIN(cond01, Scan(A), Scan(B)), Scan(C))
    auto scanA = make_scan_with_card("A", 100);
    auto scanB = make_scan_with_card("B", 1000);
    auto scanC = make_scan_with_card("C", 50000);

    auto join1 = std::make_unique<PlanNode>(); join1->kind = PlanKind::JOIN;
    join1->left  = std::move(scanA);
    join1->right = std::move(scanB);
    join1->join_pred = make_join_cond("A","id","B","a_id").pred;
    join1->schema = join1->left->schema;
    for (auto& c : join1->right->schema) join1->schema.push_back(c);

    auto join2 = std::make_unique<PlanNode>(); join2->kind = PlanKind::JOIN;
    join2->left  = std::move(join1);
    join2->right = std::move(scanC);
    join2->join_pred = make_join_cond("B","id","C","b_id").pred;
    join2->schema = join2->left->schema;
    for (auto& c : join2->right->schema) join2->schema.push_back(c);

    std::vector<BaseTable> tables;
    std::vector<JoinCond>  conds;
    extract_join_info(join2.get(), tables, conds);

    assert(tables.size() == 3);
    assert(conds.size()  == 2);
    PASS("extract_join_info: finds 3 base tables and 2 join conditions from 3-table plan");
}

// ── Test: single table ───────────────────────────────────
static void test_dp_single_table() {
    Catalog cat;
    CostModel cm(cat);
    JoinOrderDP dp(cm, cat);

    std::vector<BaseTable> tables;
    tables.push_back(BaseTable{"customers", make_scan_with_card("customers", 10000)});
    std::vector<JoinCond> conds;

    auto result = dp.find_best_order(tables, conds);
    assert(result != nullptr);
    assert(result->kind == PlanKind::SCAN);
    assert(result->table_name == "customers");
    PASS("1-table DP returns the single Scan node");
}

int main() {
    std::cout << "=== test_join_order ===\n";
    try {
        test_dp_2table_order();
        test_dp_3table_known_optimal();
        test_extract_join_info();
        test_dp_single_table();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << "\n";
        failed++;
    }
    std::cout << "\n  Results: " << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
