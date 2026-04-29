// tests/test_e2e.cpp
<<<<<<< HEAD
// End-to-end correctness tests.
// Parses and executes queries against small in-memory CSV files,
// verifies result counts and correctness.
// Requires that the benchmark data is already generated in benchmark/benchdata/
=======
// end-to-end tests.
// parses and executes queries against tiny csv datasets.
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177

#include <cassert>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/parser.h"
#include "../src/executor.h"
#include "../src/rewriter.h"
#include "../src/cost_model.h"
#include "../src/join_order.h"
#ifdef _WIN32
#include <direct.h>
#endif

#define PASS(msg) do { std::cout << "  PASS: " << msg << "\n"; passed++; } while(0)
#define FAIL(msg) do { std::cerr << "  FAIL: " << msg << "\n"; failed++; } while(0)

static int passed = 0, failed = 0;

<<<<<<< HEAD
// ── Write a tiny CSV to a temp path ─────────────────────
=======
// write a tiny csv to a temp path
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
static std::string tmp_dir;

static void write_csv(const std::string& name, const std::string& content) {
    std::string path = tmp_dir + "/" + name;
    std::ofstream f(path);
    f << content;
}

static void setup_tiny_dataset() {
<<<<<<< HEAD
    // customers: 4 rows, 2 in 'PK'
=======
    // customers: 4 rows, 2 in pk
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    write_csv("customers.csv",
        "id,name,country,age\n"
        "1,Alice,PK,30\n"
        "2,Bob,US,25\n"
        "3,Carol,PK,35\n"
        "4,Dave,UK,40\n");

    // orders: 6 rows, mixed years
    write_csv("orders.csv",
        "id,customer_id,total,year,status\n"
        "1,1,100.0,2024,shipped\n"
        "2,1,200.0,2023,delivered\n"
        "3,2,150.0,2024,pending\n"
        "4,3,300.0,2024,shipped\n"
        "5,3,50.0,2023,delivered\n"
        "6,4,400.0,2024,shipped\n");

    // line_items: 2 per order
    write_csv("line_items.csv",
        "order_id,product_id,qty,price\n"
        "1,1,2,50.0\n"
        "1,2,1,100.0\n"
        "2,3,3,66.0\n"
        "2,4,1,200.0\n"
        "3,1,1,150.0\n"
        "3,2,2,75.0\n"
        "4,5,4,75.0\n"
        "4,6,1,300.0\n"
        "5,7,2,25.0\n"
        "5,8,1,50.0\n"
        "6,9,1,400.0\n"
        "6,10,2,200.0\n");

    // products: simple
    write_csv("products.csv",
        "id,name,category,supplier_id\n"
        "1,Widget,Electronics,10\n"
        "2,Gadget,Electronics,11\n"
        "3,Doohickey,Clothing,12\n"
        "4,Thingamajig,Furniture,13\n"
        "5,Whatchamacallit,Electronics,10\n"
        "6,Gizmo,Books,14\n"
        "7,Contraption,Sports,15\n"
        "8,Device,Electronics,10\n"
        "9,Apparatus,Clothing,12\n"
        "10,Instrument,Music,16\n");
}

static std::vector<Row> run_query(const std::string& sql, const Catalog& cat, bool optimize = true) {
    Parser parser;
    auto plan = parser.parse(sql, cat);
    if (optimize) {
        Rewriter  rw(cat);
        CostModel cm(cat);
        plan = rw.rewrite(std::move(plan));
        cm.annotate(plan.get());
        plan = apply_join_ordering(std::move(plan), cm, cat);
        cm.annotate(plan.get());
        plan = rw.apply_join_swap(std::move(plan));
        cm.annotate(plan.get());
    } else {
        CostModel cm(cat);
        cm.annotate(plan.get());
    }
    Executor exec(cat);
    return exec.execute(plan.get());
}

<<<<<<< HEAD
// ── Tests ─────────────────────────────────────────────────
=======
// tests
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
static void test_scan_all() {
    Catalog cat;
    cat.load(tmp_dir);

    auto rows = run_query("SELECT * FROM customers", cat, false);
    assert(rows.size() == 4);
    PASS("SELECT * FROM customers → 4 rows");
}

static void test_filter_equality() {
    Catalog cat;
    cat.load(tmp_dir);

    auto rows = run_query("SELECT * FROM customers WHERE customers.country = 'PK'", cat, true);
<<<<<<< HEAD
    assert(rows.size() == 2);  // Alice + Carol
=======
    assert(rows.size() == 2);  // alice + carol
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    PASS("WHERE country='PK' → 2 rows (Alice, Carol)");
}

static void test_2table_join_correct_result() {
    Catalog cat;
    cat.load(tmp_dir);

<<<<<<< HEAD
    // customers ⋈ orders on customer_id; expect 6 rows (all orders have a customer)
=======
    // customers ⋈ orders on customer_id; expect 6 rows
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    auto rows_opt   = run_query(
        "SELECT * FROM customers, orders WHERE customers.id = orders.customer_id", cat, true);
    auto rows_naive = run_query(
        "SELECT * FROM customers, orders WHERE customers.id = orders.customer_id", cat, false);

<<<<<<< HEAD
    // Both modes must produce the same count
=======
    // both modes must produce the same count
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    assert(rows_opt.size() == rows_naive.size());
    assert(rows_opt.size() == 6);
    PASS("2-table join: optimized and naive produce same 6 rows");
}

static void test_filter_pushdown_correct_result() {
    Catalog cat;
    cat.load(tmp_dir);

<<<<<<< HEAD
    // PK customers with year=2024 orders:
    //   Alice(PK): orders 1(2024), 2(2023) → only order 1
    //   Carol(PK): orders 4(2024), 5(2023) → only order 4
    //   → 2 result rows
=======
    // pk customers with year=2024 orders should give 2 rows
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    auto rows = run_query(
        "SELECT * FROM customers, orders "
        "WHERE customers.id = orders.customer_id "
        "AND customers.country = 'PK' AND orders.year = 2024", cat, true);
    assert(rows.size() == 2);
    PASS("Q2-style: PK customers in year 2024 → 2 rows");
}

static void test_3table_join_correct() {
    Catalog cat;
    cat.load(tmp_dir);

<<<<<<< HEAD
    // Join customers, orders, line_items
    // PK customers: Alice(1), Carol(3)
    // Alice's year=2024 orders: 1 → 2 line items
    // Carol's year=2024 orders: 4 → 2 line items
    // → 4 result rows
=======
    // join customers, orders, and line_items
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    auto rows_opt   = run_query(
        "SELECT * FROM customers, orders, line_items "
        "WHERE customers.id = orders.customer_id "
        "AND orders.id = line_items.order_id "
        "AND customers.country = 'PK' AND orders.year = 2024", cat, true);
    auto rows_naive = run_query(
        "SELECT * FROM customers, orders, line_items "
        "WHERE customers.id = orders.customer_id "
        "AND orders.id = line_items.order_id "
        "AND customers.country = 'PK' AND orders.year = 2024", cat, false);

    assert(rows_opt.size() == rows_naive.size());
<<<<<<< HEAD
    // 4 = Alice×order1(2 li) + Carol×order4(2 li)
=======
    // 4 = alice×order1(2 li) + carol×order4(2 li)
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    assert(rows_opt.size() == 4);
    PASS("3-table Q2-style: optimized == naive == 4 rows");
}

static void test_group_by_result() {
    Catalog cat;
    cat.load(tmp_dir);

<<<<<<< HEAD
    // GROUP BY country → 3 groups (PK, US, UK)
=======
    // group by country → 3 groups
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    auto rows = run_query(
        "SELECT customers.country, SUM(orders.total) FROM customers, orders "
        "WHERE customers.id = orders.customer_id GROUP BY customers.country", cat, true);
    assert(rows.size() == 3);
    PASS("GROUP BY country → 3 distinct countries");
}

static void test_limit() {
    Catalog cat;
    cat.load(tmp_dir);

    auto rows = run_query("SELECT * FROM orders LIMIT 3", cat, false);
    assert(rows.size() == 3);
    PASS("LIMIT 3 → exactly 3 rows");
}

static void test_optimizer_same_result_as_naive() {
<<<<<<< HEAD
    // Run Q3-style 4-table query in both modes and compare counts
=======
    // run the 4-table query in both modes and compare counts
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
    Catalog cat;
    cat.load(tmp_dir);

    std::string q4 =
        "SELECT * FROM customers, orders, line_items, products "
        "WHERE customers.id = orders.customer_id "
        "AND orders.id = line_items.order_id "
        "AND line_items.product_id = products.id "
        "AND customers.country = 'PK' AND products.category = 'Electronics'";

    auto rows_opt   = run_query(q4, cat, true);
    auto rows_naive = run_query(q4, cat, false);
    assert(rows_opt.size() == rows_naive.size());
    PASS("Q3-style 4-table: optimizer and naive return identical row count");
}

<<<<<<< HEAD
// ── Setup + main ─────────────────────────────────────────
=======
// setup + main
>>>>>>> 5ffcc872dc5e9ad8dfa2b98676c9177934a11177
static std::string get_tmp_dir() {
#ifdef _WIN32
    std::string d = "qopt_test_tmp";
    _mkdir(d.c_str());
    return d;
#else
    std::string d = "/tmp/qopt_test";
    system(("mkdir -p " + d).c_str());
    return d;
#endif
}

int main() {
    std::cout << "=== test_e2e ===\n";

    tmp_dir = get_tmp_dir();
    setup_tiny_dataset();
    std::cout << "  Using temp dataset in: " << tmp_dir << "\n";

    try {
        test_scan_all();
        test_filter_equality();
        test_2table_join_correct_result();
        test_filter_pushdown_correct_result();
        test_3table_join_correct();
        test_group_by_result();
        test_limit();
        test_optimizer_same_result_as_naive();
    } catch (std::exception& e) {
        std::cerr << "EXCEPTION: " << e.what() << "\n";
        failed++;
    }
    std::cout << "\n  Results: " << passed << " passed, " << failed << " failed\n";
    return failed > 0 ? 1 : 0;
}
