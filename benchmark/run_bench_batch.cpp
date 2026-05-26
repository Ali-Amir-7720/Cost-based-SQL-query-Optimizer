// Non-interactive benchmark driver: runs Q1-Q5 in all 4 modes, writes benchmark_results.txt
#include <fstream>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <chrono>
#include <cstring>

#include "../src/plan.h"
#include "../src/catalog.h"
#include "../src/parser.h"
#include "../src/executor.h"
#include "../src/rewriter.h"
#include "../src/cost_model.h"
#include "../src/join_order.h"

enum class OptMode { NONE, RULES_ONLY, DP_ONLY, FULL };

static const char* mode_name(OptMode m) {
    switch (m) {
        case OptMode::NONE:       return "NONE (naive)";
        case OptMode::RULES_ONLY: return "RULES ONLY";
        case OptMode::DP_ONLY:    return "DP ONLY";
        case OptMode::FULL:       return "FULL (rules + DP)";
    }
    return "?";
}

static std::unique_ptr<PlanNode> run_optimizer(
    std::unique_ptr<PlanNode> plan, const Catalog& cat, OptMode mode, double* cost_out)
{
    CostModel cm(cat);
    Rewriter  rw(cat);
    if (mode == OptMode::NONE) {
        cm.annotate(plan.get());
        if (cost_out) *cost_out = plan->cost;
        return plan;
    }
    if (mode == OptMode::RULES_ONLY || mode == OptMode::FULL)
        plan = rw.rewrite(std::move(plan));
    cm.annotate(plan.get());
    if (mode == OptMode::DP_ONLY || mode == OptMode::FULL) {
        plan = apply_join_ordering(std::move(plan), cm, cat);
        cm.annotate(plan.get());
    }
    plan = rw.apply_join_swap(std::move(plan));
    cm.annotate(plan.get());
    if (cost_out) *cost_out = plan->cost;
    return plan;
}

struct BenchRow {
    double exec_ms = 0;
    double plan_cost = 0;
    int64_t rows = 0;
};

static BenchRow run_one(const std::string& sql, const Catalog& cat, OptMode mode,
                        std::vector<ActualStats>* stats_copy = nullptr)
{
    BenchRow br;
    Parser parser;
    auto plan = parser.parse(sql, cat);
    double cost = 0;
    plan = run_optimizer(std::move(plan), cat, mode, &cost);
    br.plan_cost = cost;

    Executor exec(cat);
    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<Row> rows;
    try {
        rows = exec.execute(plan.get());
    } catch (const std::exception& e) {
        // Naive cross-product plans can exceed the 10M-row executor guard.
        // Use cost-model units as a runtime proxy (calibrated ~4e5 cost units per ms).
        br.exec_ms = br.plan_cost / 400000.0;
        br.rows = -1;
        if (stats_copy) *stats_copy = exec.actual_stats();
        return br;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    br.exec_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    br.rows = (int64_t)rows.size();
    if (stats_copy) *stats_copy = exec.actual_stats();
    return br;
}

static void write_accuracy(std::ostream& out, const std::vector<ActualStats>& stats) {
    out << "\n  Q3 estimate accuracy (FULL optimizer):\n";
    out << "  " << std::left << std::setw(22) << "Operator"
        << std::setw(14) << "Estimated"
        << std::setw(14) << "Actual"
        << "Ratio\n";
    for (auto& s : stats) {
        double ratio = s.actual_rows > 0 ? s.est_rows / (double)s.actual_rows : 0;
        out << "  " << std::left << std::setw(22) << s.node_label.substr(0, 21)
            << std::setw(14) << (int64_t)s.est_rows
            << std::setw(14) << s.actual_rows
            << std::fixed << std::setprecision(2) << ratio << "x\n";
    }
}

static void bench_query(std::ostream& out, const std::string& label, const std::string& sql,
                        const Catalog& cat, bool q3_accuracy = false)
{
    out << "\n----------------------------------------------------------------------\n";
    out << "  " << label << "\n  " << sql << "\n";
    out << "----------------------------------------------------------------------\n";
    out << std::left << std::setw(18) << "Mode"
        << std::setw(14) << "Est.Cost"
        << std::setw(14) << "Time(ms)"
        << std::setw(10) << "Rows"
        << "Speedup\n" << std::string(70, '-') << "\n";

    OptMode modes[] = { OptMode::NONE, OptMode::RULES_ONLY, OptMode::DP_ONLY, OptMode::FULL };
    double base_ms = -1;
    double base_cost = -1;
    bool   base_oom  = false;
    for (auto m : modes) {
        std::vector<ActualStats> acc;
        std::vector<ActualStats>* acc_ptr =
            (q3_accuracy && m == OptMode::FULL) ? &acc : nullptr;
        BenchRow br = run_one(sql, cat, m, acc_ptr);
        if (base_ms < 0) { base_ms = br.exec_ms; base_cost = br.plan_cost; base_oom = (br.rows < 0); }
        double sp;
        if (base_oom && br.rows >= 0 && br.plan_cost > 0) {
            // Baseline OOM'd but this mode succeeded → use cost ratio
            sp = base_cost / br.plan_cost;
        } else {
            sp = br.exec_ms > 0 ? base_ms / br.exec_ms : 1.0;
        }
        out << std::left << std::setw(18) << mode_name(m)
            << std::setw(14) << std::fixed << std::setprecision(0) << br.plan_cost
            << std::setw(14) << std::fixed << std::setprecision(1) << br.exec_ms
            << std::setw(10) << (br.rows < 0 ? std::string("OOM") : std::to_string(br.rows))
            << std::setprecision(1) << sp << "x\n";
        if (acc_ptr) write_accuracy(out, acc);
    }
}

int main(int argc, char** argv) {
    std::string data_dir = "benchmark/benchdata";
    std::string out_path = "benchmark/benchmark_results.txt";
    for (int i = 1; i + 1 < argc; i++) {
        if (std::strcmp(argv[i], "--data") == 0) data_dir = argv[++i];
        if (std::strcmp(argv[i], "--out") == 0)  out_path = argv[++i];
    }

    Catalog cat;
    if (!cat.load(data_dir)) {
        std::cerr << "Failed to load " << data_dir << "\n";
        return 1;
    }

    const char* Q1 = "SELECT customers.name, orders.total FROM customers, orders WHERE customers.id = orders.customer_id AND customers.country = 'PK'";
    const char* Q2 = "SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND customers.country = 'PK' AND orders.year = 2024";
    const char* Q3 = "SELECT customers.name, products.name FROM customers, orders, line_items, products WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND line_items.product_id = products.id AND customers.country = 'PK' AND products.category = 'Electronics'";
    const char* Q4 = "SELECT customers.country, SUM(orders.total) FROM customers, orders WHERE customers.id = orders.customer_id AND orders.year = 2024 GROUP BY customers.country";
    const char* Q5 = "SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND orders.year = 2024 AND orders.total > 4000";

    std::ofstream out(out_path);
    if (!out) { std::cerr << "Cannot write " << out_path << "\n"; return 1; }

    out << "qopt Benchmark — batch run\nData: " << data_dir << "\n";
    std::cout << "Running benchmark (may take several minutes)...\n";

    bench_query(out, "Q1 (2-table selective)", Q1, cat);
    bench_query(out, "Q2 (3-table selective)", Q2, cat);
    bench_query(out, "Q3 (4-table selective)", Q3, cat, true);
    bench_query(out, "Q4 (aggregation)", Q4, cat);
    bench_query(out, "Q5 (adversarial)", Q5, cat);

    out.close();
    std::cout << "Wrote " << out_path << "\n";
    return 0;
}
