#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <chrono>
#include "plan.h"
#include "catalog.h"

// ============================================================
//  Actual cardinality instrumentation (filled during execution)
// ============================================================
struct ActualStats {
    std::string node_label;
    int64_t     actual_rows = 0;
    double      est_rows    = 0.0;
};

// ============================================================
//  Executor — materialised operator model
//  Each operator produces a complete vector<Row>.
// ============================================================
class Executor {
public:
    explicit Executor(const Catalog& cat) : cat_(cat) {}

    // Execute the plan tree rooted at node.
    // Returns all result rows; fills actual_stats_ as a side effect.
    std::vector<Row> execute(const PlanNode* node);

    // Instrumentation: actual cardinalities collected during last execute()
    const std::vector<ActualStats>& actual_stats() const { return stats_; }
    void clear_stats() { stats_.clear(); }

    // ---- Individual operator implementations (public for tests) ----
    std::vector<Row> exec_scan          (const PlanNode* node);
    std::vector<Row> exec_filter        (const PlanNode* node);
    std::vector<Row> exec_project       (const PlanNode* node);
    std::vector<Row> exec_join          (const PlanNode* node);
    std::vector<Row> exec_cross_product (const PlanNode* node);
    std::vector<Row> exec_groupby       (const PlanNode* node);
    std::vector<Row> exec_limit         (const PlanNode* node);

    // Expression / predicate evaluators (public for reuse)
    static Value eval_expr(const Expr* e, const Row& row, const Schema& schema);
    static bool  eval_pred(const Pred* p, const Row& row, const Schema& schema);

private:
    const Catalog&          cat_;
    std::vector<ActualStats> stats_;

    void record(const std::string& label, double est, int64_t actual);
};
