#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <chrono>
#include <iomanip>
#include <algorithm>
#include <cstring>

#include "plan.h"
#include "catalog.h"
#include "parser.h"
#include "executor.h"
#include "rewriter.h"
#include "cost_model.h"
#include "join_order.h"

// optimizer mode enum
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

// global session statistics
struct SessionStats {
    int    queries_run     = 0;
    int    optimizer_wins  = 0;   // optimized faster than naive
    double total_speedup   = 0.0;
    double total_plan_ms   = 0.0;
    double total_exec_ms   = 0.0;
} g_stats;

// optimizer pipeline
std::unique_ptr<PlanNode> run_optimizer(
    std::unique_ptr<PlanNode> plan,
    const Catalog&            cat,
    OptMode                   mode,
    double*                   plan_cost_out = nullptr,
    double*                   plan_ms_out   = nullptr)
{
    auto t0 = std::chrono::high_resolution_clock::now();

    CostModel cm(cat);
    Rewriter  rw(cat);

    if (mode == OptMode::NONE) {
        cm.annotate(plan.get());
        if (plan_cost_out) *plan_cost_out = plan->cost;
    } else {
        // rules (predicate pushdown, constant folding, etc.)
        if (mode == OptMode::RULES_ONLY || mode == OptMode::FULL) {
            plan = rw.rewrite(std::move(plan));
        }
        cm.annotate(plan.get());

        // selinger dp join ordering
        if (mode == OptMode::DP_ONLY || mode == OptMode::FULL) {
            plan = apply_join_ordering(std::move(plan), cm, cat);
            cm.annotate(plan.get());
        }

        // join input swap (build smaller side on left)
        plan = rw.apply_join_swap(std::move(plan));
        cm.annotate(plan.get());

        if (plan_cost_out) *plan_cost_out = plan->cost;
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    if (plan_ms_out) *plan_ms_out = ms;

    return plan;
}

// print result rows (up to 30 shown)
static void print_results(const std::vector<Row>& rows, const Schema& schema) {
    if (rows.empty()) { std::cout << "(0 rows)\n"; return; }

    // column headers
    std::cout << "  ";
    for (auto& c : schema) {
        std::string nm = c.alias.empty() ? (c.table.empty() ? c.name : c.table + "." + c.name) : c.alias;
        std::cout << std::left << std::setw(18) << nm.substr(0, 17);
    }
    std::cout << "\n  " << std::string(schema.size() * 18, '-') << "\n";

    int shown = 0;
    for (auto& row : rows) {
        if (shown >= 30) { std::cout << "  ... (" << rows.size() - shown << " more rows)\n"; break; }
        std::cout << "  ";
        for (int i = 0; i < (int)std::min(row.size(), schema.size()); i++)
            std::cout << std::left << std::setw(18) << row[i].to_string().substr(0, 17);
        std::cout << "\n";
        shown++;
    }
    std::cout << "(" << rows.size() << " rows)\n";
}

// print estimate accuracy table (for explain + after exec)
static void print_accuracy_table(const Executor& exec) {
    auto& stats = exec.actual_stats();
    if (stats.empty()) return;
    std::cout << "\n  Estimate accuracy:\n";
    std::cout << "  " << std::left
              << std::setw(22) << "Operator"
              << std::setw(14) << "Estimated"
              << std::setw(14) << "Actual"
              << "Ratio\n";
    std::cout << "  " << std::string(60, '-') << "\n";
    for (auto& s : stats) {
        double ratio = (s.actual_rows > 0) ? s.est_rows / (double)s.actual_rows : 0.0;
        std::cout << "  " << std::left
                  << std::setw(22) << s.node_label.substr(0, 21)
                  << std::setw(14) << (int64_t)s.est_rows
                  << std::setw(14) << s.actual_rows
                  << std::fixed << std::setprecision(2) << ratio << "x\n";
    }
}

// execute a query in one mode, return timing + row count
struct RunResult {
    int64_t  result_rows = 0;
    double   exec_ms     = 0.0;
    double   plan_cost   = 0.0;
    double   plan_ms     = 0.0;
    std::string plan_str;
};

static RunResult run_query_mode(
    const std::string& sql,
    const Catalog&     cat,
    OptMode            mode,
    bool               print_plan = false,
    bool               print_rows = false,
    bool               print_accuracy = false)
{
    Parser parser;
    std::unique_ptr<PlanNode> plan;
    try {
        plan = parser.parse(sql, cat);
    } catch (std::exception& e) {
        std::cerr << "Parse error: " << e.what() << "\n";
        return {};
    }

    double plan_cost = 0.0, plan_ms = 0.0;
    plan = run_optimizer(std::move(plan), cat, mode, &plan_cost, &plan_ms);

    RunResult res;
    res.plan_cost = plan_cost;
    res.plan_ms   = plan_ms;
    res.plan_str  = explain_plan(plan.get());

    if (print_plan) {
        std::cout << "  Plan [" << mode_name(mode) << "]:\n";
        for (auto& line : [&](){
            std::vector<std::string> lines;
            std::istringstream ss(res.plan_str);
            std::string l;
            while (std::getline(ss, l)) lines.push_back(l);
            return lines;
        }()) std::cout << "    " << line << "\n";
        std::cout << "  Estimated cost: " << (int64_t)plan_cost << "\n";
    }

    Executor exec(cat);
    Schema   schema;
    if (plan) schema = plan->schema;

    auto t0 = std::chrono::high_resolution_clock::now();
    std::vector<Row> rows;
    try {
        rows = exec.execute(plan.get());
    } catch (std::exception& e) {
        std::cerr << "Execution error: " << e.what() << "\n";
        return res;
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    res.exec_ms     = std::chrono::duration<double, std::milli>(t1 - t0).count();
    res.result_rows = (int64_t)rows.size();

    if (print_rows)     print_results(rows, schema);
    if (print_accuracy) print_accuracy_table(exec);

    return res;
}

// main select handler: run none and full, show comparison
static void handle_select(const std::string& sql, const Catalog& cat) {
    std::cout << "\n--- WITHOUT optimizer (naive plan) ---\n";
    RunResult naive = run_query_mode(sql, cat, OptMode::NONE, true, false, false);
    std::cout << "  actual time: " << std::fixed << std::setprecision(3)
              << naive.exec_ms / 1000.0 << " seconds, "
              << naive.result_rows << " result rows\n";

    std::cout << "\n--- WITH optimizer (rules + DP + join swap) ---\n";
    RunResult opt   = run_query_mode(sql, cat, OptMode::FULL, true, true, true);
    std::cout << "  actual time: " << std::fixed << std::setprecision(3)
              << opt.exec_ms / 1000.0 << " seconds, "
              << opt.result_rows << " result rows\n";

    double speedup    = (opt.exec_ms > 0.0) ? naive.exec_ms / opt.exec_ms : 1.0;
    double cost_ratio = (opt.plan_cost > 0.0) ? naive.plan_cost / opt.plan_cost : 1.0;

    std::cout << "\n  speedup:         " << std::fixed << std::setprecision(1) << speedup << "x\n";
    std::cout << "  plan cost ratio: " << std::fixed << std::setprecision(1) << cost_ratio << "x\n";
    std::cout << "  plan time:       " << std::fixed << std::setprecision(2) << opt.plan_ms << " ms\n\n";

    // update session stats
    g_stats.queries_run++;
    if (speedup > 1.1) { g_stats.optimizer_wins++; g_stats.total_speedup += speedup; }
    g_stats.total_plan_ms += opt.plan_ms;
    g_stats.total_exec_ms += opt.exec_ms;
}

// explain handler
static void handle_explain(const std::string& sql, const Catalog& cat) {
    std::cout << "\n--- Naive plan ---\n";
    run_query_mode(sql, cat, OptMode::NONE, true, false, false);
    std::cout << "\n--- Optimised plan (rules + DP) ---\n";
    run_query_mode(sql, cat, OptMode::FULL, true, false, false);
    std::cout << "\n";
}

// benchmark handler: runs a query in all 4 modes
static void handle_benchmark(const std::string& sql, const Catalog& cat) {
    OptMode modes[] = { OptMode::NONE, OptMode::RULES_ONLY,
                        OptMode::DP_ONLY, OptMode::FULL };
    std::cout << "\n  Benchmark: " << sql.substr(0, 60) << "...\n";
    std::cout << "  " << std::string(70, '-') << "\n";
    std::cout << "  " << std::left
              << std::setw(18) << "Mode"
              << std::setw(14) << "Est.Cost"
              << std::setw(14) << "Time(ms)"
              << std::setw(10) << "Rows"
              << "Speedup\n";
    std::cout << "  " << std::string(70, '-') << "\n";

    double base_ms = -1.0;
    for (auto m : modes) {
        RunResult r = run_query_mode(sql, cat, m, false, false, false);
        if (base_ms < 0) base_ms = r.exec_ms;
        double speedup = (r.exec_ms > 0) ? base_ms / r.exec_ms : 1.0;
        std::cout << "  " << std::left
                  << std::setw(18) << mode_name(m)
                  << std::setw(14) << (int64_t)r.plan_cost
                  << std::setw(14) << std::fixed << std::setprecision(1) << r.exec_ms
                  << std::setw(10) << r.result_rows
                  << std::setprecision(1) << speedup << "x\n";
    }
    std::cout << "\n";
}

// \stats command
static void print_stats() {
    std::cout << "\n  Session statistics:\n";
    std::cout << "  queries executed:  " << g_stats.queries_run << "\n";
    std::cout << "  optimizer wins:    " << g_stats.optimizer_wins;
    if (g_stats.optimizer_wins > 0)
        std::cout << " (avg speedup "
                  << std::fixed << std::setprecision(1)
                  << g_stats.total_speedup / g_stats.optimizer_wins << "x)";
    std::cout << "\n";
    if (g_stats.queries_run > 0) {
        std::cout << "  avg plan time:     "
                  << std::fixed << std::setprecision(2)
                  << g_stats.total_plan_ms / g_stats.queries_run << " ms\n";
        std::cout << "  avg exec time:     "
                  << std::fixed << std::setprecision(2)
                  << g_stats.total_exec_ms / g_stats.queries_run << " ms\n";
    }
    std::cout << "\n";
}

// banner
static void print_banner() {
    std::cout << R"(
  ╔══════════════════════════════════════════════════════╗
  ║   qopt — Cost-Based SQL Query Optimizer   v1.0       ║
  ║   Advanced DBMS Project 02                           ║
  ╚══════════════════════════════════════════════════════╝
  Commands:
    SELECT ...        run query (shows naive vs optimised)
    EXPLAIN SELECT .. show plan without executing
    \bench SELECT ..  benchmark query across 4 optimizer modes
    \stats            show session statistics
    LOAD <dir>        reload catalog from data directory
    \quit  or  \q     exit
)" << "\n";
}

// main
int main(int argc, char** argv) {
    print_banner();

    std::string data_dir;
    for (int i = 1; i < argc; i++) {
        if ((std::strcmp(argv[i], "--data") == 0 || std::strcmp(argv[i], "-d") == 0)
            && i + 1 < argc)
        {
            data_dir = argv[++i];
        }
    }

    Catalog cat;
    if (!data_dir.empty()) {
        if (!cat.load(data_dir)) {
            std::cerr << "Failed to load catalog from " << data_dir << "\n";
        } else {
            cat.print_summary();
        }
    } else {
        std::cout << "  (no data directory specified — use LOAD <dir> or --data <dir>)\n\n";
    }

    // repl
    std::string line;
    for (;;) {
        std::cout << "qopt> " << std::flush;
        if (!std::getline(std::cin, line)) break;

        // trim
        while (!line.empty() && (line.back() == '\r' || line.back() == ' ')) line.pop_back();
        if (line.empty()) continue;

        std::string upper = line;
        for (char& c : upper) c = (char)std::toupper((unsigned char)c);

        // \quit / \q
        if (upper == "\\QUIT" || upper == "\\Q" || upper == "EXIT" || upper == "QUIT") {
            std::cout << "Bye.\n";
            break;
        }

        // \stats
        if (upper == "\\STATS") { print_stats(); continue; }

        // \help
        if (upper == "\\HELP") { print_banner(); continue; }

        // load <dir>
        if (upper.substr(0, 4) == "LOAD") {
            std::string dir = line.substr(4);
            while (!dir.empty() && dir.front() == ' ') dir = dir.substr(1);
            if (dir.empty()) { std::cout << "Usage: LOAD <data_dir>\n"; continue; }
            if (!cat.load(dir)) std::cerr << "Failed to load catalog from " << dir << "\n";
            else cat.print_summary();
            continue;
        }

        // \bench select ...
        if (upper.substr(0, 6) == "\\BENCH") {
            std::string sql = line.substr(6);
            while (!sql.empty() && sql.front() == ' ') sql = sql.substr(1);
            if (cat.table_count() == 0) { std::cout << "No catalog loaded. Use LOAD <dir>.\n"; continue; }
            handle_benchmark(sql, cat);
            continue;
        }

        // explain select ...
        if (upper.substr(0, 7) == "EXPLAIN") {
            std::string sql = line.substr(7);
            while (!sql.empty() && sql.front() == ' ') sql = sql.substr(1);
            if (cat.table_count() == 0) { std::cout << "No catalog loaded. Use LOAD <dir>.\n"; continue; }
            handle_explain(sql, cat);
            continue;
        }

        // select ...
        if (upper.substr(0, 6) == "SELECT") {
            if (cat.table_count() == 0) { std::cout << "No catalog loaded. Use LOAD <dir>.\n"; continue; }
            // accumulate multi-line queries
            std::string sql = line;
            while (sql.find(';') == std::string::npos) {
                std::cout << "   ... " << std::flush;
                std::string cont;
                if (!std::getline(std::cin, cont)) break;
                sql += " " + cont;
            }
            // strip semicolons
            while (!sql.empty() && (sql.back() == ';' || sql.back() == ' ')) sql.pop_back();
            handle_select(sql, cat);
            continue;
        }

        std::cout << "  Unknown command. Type \\help for usage.\n";
    }
    return 0;
}
