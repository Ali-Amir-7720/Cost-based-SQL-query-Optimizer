// benchmark/gen_data.cpp
// Deterministic CSV data generator for the qopt benchmark dataset.
// Usage:   gen_data <output_dir>
// Seed:    42424242 (fixed — produces reproducible data)
//
// Tables generated:
//   customers  (10,000 rows): id, name, country, age
//   orders     (500,000 rows): id, customer_id, total, year, status
//   line_items (2,000,000 rows): order_id, product_id, qty, price
//   products   (50,000 rows):  id, name, category, supplier_id

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <sys/stat.h>
#ifdef _WIN32
#include <direct.h>
#endif
using namespace std;

// ── PRNG (LCG) for reproducibility across platforms ────────
static uint64_t rng_state = 42424242ULL;
static uint64_t rng_next() {
    rng_state = rng_state * 6364136223846793005ULL + 1442695040888963407ULL;
    return rng_state;
}
static int rng_int(int lo, int hi) {   // inclusive
    return lo + (int)(rng_next() % (uint64_t)(hi - lo + 1));
}
static double rng_dbl(double lo, double hi) {
    return lo + (double)(rng_next() & 0xFFFFFF) / (double)0xFFFFFF * (hi - lo);
}
static const char* rng_pick(const char* const* arr, int n) {
    return arr[rng_next() % (uint64_t)n];
}

// ── Static lookup tables ───────────────────────────────────
static const char* COUNTRIES[] = {
    "PK","US","UK","DE","FR","IN","CN","JP","AU","CA",
    "BR","MX","IT","ES","NL","SE","NO","PL","KR","SG",
    "ZA","NG","EG","AR"
};  // 24 distinct — 'PK' appears ~1/24 ≈ 4.2%

static const char* STATUSES[] = { "pending","shipped","delivered","cancelled","returned" };

// 87 distinct categories — 'Electronics' is one of them (~1.1%)
static const char* CATEGORIES[87];
static void init_categories() {
    CATEGORIES[0]  = "Electronics";
    CATEGORIES[1]  = "Clothing";    CATEGORIES[2]  = "Furniture";
    CATEGORIES[3]  = "Sports";      CATEGORIES[4]  = "Books";
    CATEGORIES[5]  = "Toys";        CATEGORIES[6]  = "Automotive";
    CATEGORIES[7]  = "Garden";      CATEGORIES[8]  = "Health";
    CATEGORIES[9]  = "Beauty";      CATEGORIES[10] = "Music";
    CATEGORIES[11] = "Movies";      CATEGORIES[12] = "Office";
    CATEGORIES[13] = "Tools";       CATEGORIES[14] = "Baby";
    CATEGORIES[15] = "Food";        CATEGORIES[16] = "Pets";
    CATEGORIES[17] = "Industrial";  CATEGORIES[18] = "Jewelry";
    CATEGORIES[19] = "Luggage";
    // Fill remaining 67 with generated names
    for (int i = 20; i < 87; i++) {
        static char bufs[67][16];
        snprintf(bufs[i-20], 16, "Cat%02d", i);
        CATEGORIES[i] = bufs[i-20];
    }
}

// Simple name generator
static std::string gen_name(const char* prefix, int id) {
    char buf[32];
    snprintf(buf, 32, "%s_%05d", prefix, id);
    return buf;
}

// ── mkdir portable ─────────────────────────────────────────
static void ensure_dir(const std::string& dir) {
#ifdef _WIN32
    _mkdir(dir.c_str());
#else
    mkdir(dir.c_str(), 0755);
#endif
}

// ── Table generators ───────────────────────────────────────
static void gen_customers(const std::string& dir) {
    std::string path = dir + "/customers.csv";
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    fprintf(f, "id,name,country,age\n");
    for (int i = 1; i <= 10000; i++) {
        const char* country = COUNTRIES[((uint64_t)i * 2654435761ULL >> 8) % 24];
        int age = rng_int(18, 75);
        fprintf(f, "%d,%s,%s,%d\n", i, gen_name("Customer", i).c_str(), country, age);
    }
    fclose(f);
    printf("  customers.csv:   10,000 rows\n");
}

static void gen_products(const std::string& dir) {
    std::string path = dir + "/products.csv";
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    fprintf(f, "id,name,category,supplier_id\n");
    for (int i = 1; i <= 50000; i++) {
        const char* cat = CATEGORIES[((uint64_t)i * 2246822519ULL >> 8) % 87];
        int sup = rng_int(1, 500);
        fprintf(f, "%d,%s,%s,%d\n", i, gen_name("Product", i).c_str(), cat, sup);
    }
    fclose(f);
    printf("  products.csv:    50,000 rows\n");
}

static void gen_orders(const std::string& dir) {
    std::string path = dir + "/orders.csv";
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    fprintf(f, "id,customer_id,total,year,status\n");
    for (int i = 1; i <= 500000; i++) {
        int cust_id   = rng_int(1, 10000);
        double total  = rng_dbl(5.0, 5000.0);
        int year      = 2020 + (int)((uint64_t)i * 3266489917ULL % 5); // 2020-2024 uniform
        const char* st = STATUSES[rng_int(0, 4)];
        fprintf(f, "%d,%d,%.2f,%d,%s\n", i, cust_id, total, year, st);
    }
    fclose(f);
    printf("  orders.csv:      500,000 rows\n");
}

static void gen_line_items(const std::string& dir) {
    std::string path = dir + "/line_items.csv";
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); return; }
    fprintf(f, "order_id,product_id,qty,price\n");
    int li = 0;
    // Each order gets 3-5 line items (avg ~4) → ~2M rows
    for (int ord = 1; ord <= 500000; ord++) {
        int n_items = rng_int(3, 5);
        for (int j = 0; j < n_items; j++) {
            int prod_id = rng_int(1, 50000);
            int qty     = rng_int(1, 10);
            double price = rng_dbl(1.0, 500.0);
            fprintf(f, "%d,%d,%d,%.2f\n", ord, prod_id, qty, price);
            li++;
        }
    }
    fclose(f);
    printf("  line_items.csv:  ~%d rows\n", li);
}

// ── Entry point ────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: gen_data <output_dir>\n");
        return 1;
    }
    std::string dir = argv[1];
    ensure_dir(dir);
    init_categories();

    printf("Generating benchmark dataset in '%s'...\n", dir.c_str());
    gen_customers(dir);
    gen_products(dir);
    gen_orders(dir);
    gen_line_items(dir);
    printf("Done. Delete catalog.json in that directory to force a re-scan.\n");
    return 0;
}
