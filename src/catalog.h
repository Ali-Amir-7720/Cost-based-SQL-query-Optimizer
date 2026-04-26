#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include "plan.h"

// ============================================================
//  Column statistics (per column in a table)
// ============================================================
struct ColStats {
    std::string name;
    ValType     type          = ValType::TEXT;
    int64_t     distinct_count = 1;
    double      min_val       = 0.0;   // numeric min
    double      max_val       = 0.0;   // numeric max
    int64_t     null_count    = 0;
    std::string min_str, max_str;      // string min/max (for TEXT cols)
};

// ============================================================
//  Table metadata
// ============================================================
struct TableMeta {
    std::string              name;
    std::vector<ColStats>    cols;       // ordered list of columns
    int64_t                  row_count = 0;
    std::string              csv_path;

    const ColStats* find_col(const std::string& col_name) const {
        for (auto& c : cols)
            if (c.name == col_name) return &c;
        return nullptr;
    }
};

// ============================================================
//  Catalog — manages all table metadata
// ============================================================
class Catalog {
public:
    // Load all CSVs from data_dir, build stats, cache to catalog.json
    bool load(const std::string& data_dir);

    // Access a table's metadata (nullptr if not found)
    const TableMeta* get_table(const std::string& name) const;

    // All table names
    std::vector<std::string> table_names() const;

    // Column stats lookup
    const ColStats* get_col(const std::string& table, const std::string& col) const;

    // Schema for a table (list of SchemaCol)
    Schema make_schema(const std::string& table_name) const;

    // Print catalog summary (for startup message)
    void print_summary() const;

    // Number of tables loaded
    int table_count() const { return (int)tables_.size(); }

    // Path to data directory
    const std::string& data_dir() const { return data_dir_; }

private:
    std::map<std::string, TableMeta> tables_;
    std::string data_dir_;

    bool load_csv(TableMeta& meta);
    bool load_cache(const std::string& cache_path);
    bool save_cache(const std::string& cache_path) const;
};
