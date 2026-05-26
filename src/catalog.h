#pragma once
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include "plan.h"

struct ColStats {
    std::string name;
    ValType     type          = ValType::TEXT;
    int64_t     distinct_count = 1;
    double      min_val       = 0.0;
    double      max_val       = 0.0;
    int64_t     null_count    = 0;
    std::string min_str, max_str;
    std::vector<double> histogram; // 32 equi-depth buckets for numeric cols
};

struct TableMeta {
    std::string              name;
    std::vector<ColStats>    cols;
    int64_t                  row_count = 0;
    std::string              csv_path;

    const ColStats* find_col(const std::string& col_name) const {
        for (auto& c : cols)
            if (c.name == col_name) return &c;
        return nullptr;
    }
};

//catalog - table metadata manager
class Catalog {
public:
    bool load(const std::string& data_dir);
    const TableMeta* get_table(const std::string& name) const;
    std::vector<std::string> table_names() const;

    const ColStats* get_col(const std::string& table, const std::string& col) const;
    Schema make_schema(const std::string& table_name) const;
    void print_summary() const;
    int table_count() const { return (int)tables_.size(); }
    const std::string& data_dir() const { return data_dir_; }

private:
    std::map<std::string, TableMeta> tables_;
    std::string data_dir_;
    bool load_csv(TableMeta& meta);
    bool load_cache(const std::string& cache_path);
    bool save_cache(const std::string& cache_path) const;
};
