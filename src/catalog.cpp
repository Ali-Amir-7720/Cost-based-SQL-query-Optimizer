#include "catalog.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <set>
#include <map>
#include <algorithm>
#include <cmath>
#include <limits>
#include <cstring>

#ifdef _WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <windows.h>
#else
#  include <dirent.h>
#endif

// ============================================================
//  String helpers
// ============================================================
static std::string trim(const std::string& s) {
    auto a = s.find_first_not_of(" \t\r\n\"");
    if (a == std::string::npos) return "";
    auto b = s.find_last_not_of(" \t\r\n\"");
    return s.substr(a, b - a + 1);
}

static std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> res;
    std::string cur;
    bool in_q = false;
    for (char c : line) {
        if (c == '"') { in_q = !in_q; }
        else if (c == ',' && !in_q) { res.push_back(trim(cur)); cur.clear(); }
        else { cur += c; }
    }
    res.push_back(trim(cur));
    return res;
}

static ValType detect_type(const std::string& s) {
    if (s.empty()) return ValType::TEXT;
    bool is_num = true, has_dot = false;
    int start = (s[0] == '-') ? 1 : 0;
    if (start >= (int)s.size()) return ValType::TEXT;
    for (int i = start; i < (int)s.size(); i++) {
        if (s[i] == '.' && !has_dot) { has_dot = true; }
        else if (!isdigit((unsigned char)s[i])) { is_num = false; break; }
    }
    if (!is_num) return ValType::TEXT;
    return has_dot ? ValType::DOUBLE : ValType::INT;
}

// ============================================================
//  Directory listing
// ============================================================
static std::vector<std::string> list_csv_files(const std::string& dir) {
    std::vector<std::string> files;
#ifdef _WIN32
    WIN32_FIND_DATAA fd;
    std::string pattern = dir + "\\*.csv";
    HANDLE h = FindFirstFileA(pattern.c_str(), &fd);
    if (h != INVALID_HANDLE_VALUE) {
        do {
            files.push_back(dir + "\\" + fd.cFileName);
        } while (FindNextFileA(h, &fd));
        FindClose(h);
    }
#else
    DIR* d = opendir(dir.c_str());
    if (d) {
        struct dirent* e;
        while ((e = readdir(d)) != nullptr) {
            std::string nm = e->d_name;
            if (nm.size() > 4 && nm.substr(nm.size() - 4) == ".csv")
                files.push_back(dir + "/" + nm);
        }
        closedir(d);
    }
#endif
    std::sort(files.begin(), files.end());
    return files;
}

static std::string table_name_from_path(const std::string& path) {
    size_t sep = path.find_last_of("/\\");
    std::string base = (sep == std::string::npos) ? path : path.substr(sep + 1);
    if (base.size() > 4 && base.substr(base.size() - 4) == ".csv")
        base = base.substr(0, base.size() - 4);
    return base;
}

// ============================================================
//  CSV loader — one pass to compute statistics
// ============================================================
bool Catalog::load_csv(TableMeta& meta) {
    std::ifstream f(meta.csv_path);
    if (!f.is_open()) {
        std::cerr << "  [catalog] Cannot open: " << meta.csv_path << "\n";
        return false;
    }

    std::string header_line;
    if (!std::getline(f, header_line)) return false;
    auto col_names = split_csv_line(header_line);

    meta.cols.clear();
    int ncols = (int)col_names.size();
    for (auto& nm : col_names) {
        ColStats cs;
        cs.name          = nm;
        cs.type          = ValType::TEXT;
        cs.min_val       = std::numeric_limits<double>::max();
        cs.max_val       = std::numeric_limits<double>::lowest();
        cs.distinct_count = 0;
        cs.null_count    = 0;
        meta.cols.push_back(cs);
    }

    // Distinct tracking structures (capped at 200K distinct values per column)
    std::vector<std::set<std::string>> dsets(ncols);
    std::vector<bool> type_set(ncols, false);

    std::string line;
    meta.row_count = 0;

    while (std::getline(f, line)) {
        if (line.empty() || line == "\r") continue;
        auto vals = split_csv_line(line);
        if ((int)vals.size() < ncols) continue;
        meta.row_count++;

        for (int i = 0; i < ncols; i++) {
            const std::string& v = vals[i];
            ColStats& cs = meta.cols[i];

            if (v.empty()) { cs.null_count++; continue; }

            if (!type_set[i]) {
                cs.type = detect_type(v);
                type_set[i] = true;
            }
            if (dsets[i].size() < 200000)
                dsets[i].insert(v);

            if (cs.type == ValType::INT || cs.type == ValType::DOUBLE) {
                try {
                    double num = std::stod(v);
                    if (num < cs.min_val) cs.min_val = num;
                    if (num > cs.max_val) cs.max_val = num;
                } catch (...) {}
            } else {
                if (cs.min_str.empty() || v < cs.min_str) cs.min_str = v;
                if (cs.max_str.empty() || v > cs.max_str) cs.max_str = v;
            }
        }
    }

    for (int i = 0; i < ncols; i++) {
        meta.cols[i].distinct_count = std::max((int64_t)1, (int64_t)dsets[i].size());
        // If numeric but type never set (all nulls or empty), default TEXT
    }
    return true;
}

// ============================================================
//  Simple hand-written JSON serialiser / parser
//  (no external library — required by spec)
// ============================================================
static std::string json_esc(const std::string& s) {
    std::string r;
    for (char c : s) {
        if (c == '"') r += "\\\"";
        else if (c == '\\') r += "\\\\";
        else r += c;
    }
    return r;
}

static std::string type_str(ValType t) {
    switch (t) {
        case ValType::INT:    return "INT";
        case ValType::DOUBLE: return "DOUBLE";
        case ValType::TEXT:   return "TEXT";
        default:              return "TEXT";
    }
}
static ValType str_to_type(const std::string& s) {
    if (s == "INT")    return ValType::INT;
    if (s == "DOUBLE") return ValType::DOUBLE;
    return ValType::TEXT;
}

bool Catalog::save_cache(const std::string& path) const {
    std::ofstream f(path);
    if (!f) return false;
    f << "{\n  \"tables\": [\n";
    bool first_tbl = true;
    for (auto& kv : tables_) { const auto& meta = kv.second;
        if (!first_tbl) f << ",\n";
        first_tbl = false;
        f << "    {\n";
        f << "      \"name\": \"" << json_esc(meta.name) << "\",\n";
        f << "      \"row_count\": " << meta.row_count << ",\n";
        f << "      \"csv_path\": \"" << json_esc(meta.csv_path) << "\",\n";
        f << "      \"cols\": [\n";
        bool first_col = true;
        for (auto& cs : meta.cols) {
            if (!first_col) f << ",\n";
            first_col = false;
            f << "        {";
            f << "\"name\":\"" << json_esc(cs.name) << "\",";
            f << "\"type\":\"" << type_str(cs.type) << "\",";
            f << "\"distinct_count\":" << cs.distinct_count << ",";
            f << "\"min_val\":" << cs.min_val << ",";
            f << "\"max_val\":" << cs.max_val << ",";
            f << "\"null_count\":" << cs.null_count << ",";
            f << "\"min_str\":\"" << json_esc(cs.min_str) << "\",";
            f << "\"max_str\":\"" << json_esc(cs.max_str) << "\"";
            f << "}";
        }
        f << "\n      ]\n    }";
    }
    f << "\n  ]\n}\n";
    return true;
}

// Very minimal JSON parser — reads only our specific catalog.json format
static std::string extract_str(const std::string& line, const std::string& key) {
    std::string pat = "\"" + key + "\":\"";
    auto pos = line.find(pat);
    if (pos == std::string::npos) return "";
    pos += pat.size();
    auto end = line.find('"', pos);
    if (end == std::string::npos) return line.substr(pos);
    return line.substr(pos, end - pos);
}
static double extract_num(const std::string& line, const std::string& key) {
    std::string pat = "\"" + key + "\":";
    auto pos = line.find(pat);
    if (pos == std::string::npos) return 0;
    pos += pat.size();
    // Skip whitespace
    while (pos < line.size() && line[pos] == ' ') pos++;
    std::string num;
    while (pos < line.size() && (isdigit((unsigned char)line[pos]) || line[pos]=='-' || line[pos]=='.' || line[pos]=='e' || line[pos]=='E' || line[pos]=='+'))
        num += line[pos++];
    if (num.empty()) return 0;
    try { return std::stod(num); } catch (...) { return 0; }
}

bool Catalog::load_cache(const std::string& path) {
    std::ifstream f(path);
    if (!f) return false;

    tables_.clear();
    TableMeta cur_meta;
    ColStats  cur_col;
    bool in_table = false, in_cols = false, in_col = false;

    std::string line;
    while (std::getline(f, line)) {
        // Detect table start
        if (line.find("\"name\":") != std::string::npos && line.find("\"cols\"") == std::string::npos) {
            if (in_col && in_cols) {
                cur_meta.cols.push_back(cur_col);
                cur_col = ColStats{};
                in_col = false;
            }
            if (in_cols) {
                in_cols = false;
            }
            if (in_table && !cur_meta.name.empty()) {
                tables_[cur_meta.name] = cur_meta;
            }
        }

        // New col object
        if (line.find("{\"name\":") != std::string::npos && in_cols) {
            if (in_col) { cur_meta.cols.push_back(cur_col); cur_col = ColStats{}; }
            in_col = true;
            cur_col.name          = extract_str(line, "name");
            cur_col.type          = str_to_type(extract_str(line, "type"));
            cur_col.distinct_count = (int64_t)extract_num(line, "distinct_count");
            cur_col.min_val        = extract_num(line, "min_val");
            cur_col.max_val        = extract_num(line, "max_val");
            cur_col.null_count     = (int64_t)extract_num(line, "null_count");
            cur_col.min_str        = extract_str(line, "min_str");
            cur_col.max_str        = extract_str(line, "max_str");
            continue;
        }

        if (line.find("\"cols\":") != std::string::npos) {
            in_cols = true;
            continue;
        }

        if (line.find("\"name\":") != std::string::npos && !in_cols) {
            cur_meta = TableMeta{};
            cur_meta.name = extract_str(line, "name");
            in_table = true;
            continue;
        }
        if (in_table && !in_cols) {
            if (line.find("\"row_count\":") != std::string::npos)
                cur_meta.row_count = (int64_t)extract_num(line, "row_count");
            if (line.find("\"csv_path\":") != std::string::npos)
                cur_meta.csv_path = extract_str(line, "csv_path");
        }
    }
    // Flush last col and table
    if (in_col) cur_meta.cols.push_back(cur_col);
    if (in_table && !cur_meta.name.empty()) tables_[cur_meta.name] = cur_meta;

    return !tables_.empty();
}

// ============================================================
//  Catalog::load  — main entry point
// ============================================================
bool Catalog::load(const std::string& data_dir) {
    data_dir_ = data_dir;

    // Try cache first
    std::string cache_path = data_dir + "/catalog.json";
    {
        std::ifstream chk(cache_path);
        if (chk.good()) {
            chk.close();
            if (load_cache(cache_path)) {
                std::cout << "qopt: stats loaded from cache (" << tables_.size() << " tables)\n";
                // Verify CSV paths still exist
                bool all_ok = true;
                for (auto& kv2 : tables_) {
                    std::ifstream test(kv2.second.csv_path);
                    if (!test.good()) { all_ok = false; break; }
                }
                if (all_ok) return true;
                std::cout << "qopt: CSV paths changed, re-scanning...\n";
                tables_.clear();
            }
        }
    }

    // Full scan
    auto csv_files = list_csv_files(data_dir);
    if (csv_files.empty()) {
        std::cerr << "qopt: no CSV files found in " << data_dir << "\n";
        return false;
    }

    std::cout << "qopt: scanning " << csv_files.size() << " CSV files...\n";
    for (auto& path : csv_files) {
        TableMeta meta;
        meta.name     = table_name_from_path(path);
        meta.csv_path = path;
        if (!load_csv(meta)) continue;
        tables_[meta.name] = std::move(meta);
        std::cout << "  " << tables_[table_name_from_path(path)].name
                  << ": " << tables_[table_name_from_path(path)].row_count << " rows, "
                  << tables_[table_name_from_path(path)].cols.size() << " columns\n";
    }

    save_cache(cache_path);
    std::cout << "qopt: catalog cached to " << cache_path << "\n";
    return !tables_.empty();
}

// ============================================================
//  Accessors
// ============================================================
const TableMeta* Catalog::get_table(const std::string& name) const {
    auto it = tables_.find(name);
    return (it == tables_.end()) ? nullptr : &it->second;
}

std::vector<std::string> Catalog::table_names() const {
    std::vector<std::string> names;
    for (auto& kv : tables_) names.push_back(kv.first);
    return names;
}

const ColStats* Catalog::get_col(const std::string& table, const std::string& col) const {
    auto* tm = get_table(table);
    if (!tm) return nullptr;
    return tm->find_col(col);
}

Schema Catalog::make_schema(const std::string& table_name) const {
    Schema s;
    auto* tm = get_table(table_name);
    if (!tm) return s;
    for (auto& cs : tm->cols) {
        SchemaCol sc;
        sc.table = table_name;
        sc.name  = cs.name;
        sc.type  = cs.type;
        s.push_back(sc);
    }
    return s;
}

void Catalog::print_summary() const {
    std::cout << "qopt: opened catalog with " << tables_.size() << " tables\n";
    for (auto& kv : tables_) { const auto& meta = kv.second;
        std::cout << "  " << meta.name << "\n";
        std::cout << "    " << meta.row_count << " rows (";
        for (int i = 0; i < (int)meta.cols.size(); i++) {
            if (i) std::cout << ", ";
            std::cout << meta.cols[i].name << " " << type_str(meta.cols[i].type);
        }
        std::cout << ")\n";
    }
}
