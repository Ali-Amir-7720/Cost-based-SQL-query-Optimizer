#!/usr/bin/env bash
# benchmark/run_bench.sh
# Runs all 5 benchmark queries × 4 optimizer modes and prints the results table.
# Must be run from the project root after: make && ./benchmark/gen_data benchmark/benchdata

set -euo pipefail

QOPT=./qopt
DATADIR=benchmark/benchdata

if [ ! -f "$QOPT" ]; then
    echo "ERROR: $QOPT not found. Run 'make' first."
    exit 1
fi
if [ ! -d "$DATADIR" ]; then
    echo "ERROR: $DATADIR not found. Run 'make bench' to generate data."
    exit 1
fi

# ── The 5 benchmark queries ────────────────────────────────
Q1="SELECT customers.name, orders.total FROM customers, orders WHERE customers.id = orders.customer_id AND customers.country = 'PK'"

Q2="SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND customers.country = 'PK' AND orders.year = 2024"

Q3="SELECT customers.name, products.name FROM customers, orders, line_items, products WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND line_items.product_id = products.id AND customers.country = 'PK' AND products.category = 'Electronics'"

Q4="SELECT customers.country, SUM(orders.total) FROM customers, orders WHERE customers.id = orders.customer_id AND orders.year = 2024 GROUP BY customers.country"

Q5="SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND orders.year = 2024 AND orders.total > 4000"

declare -a QUERIES=("$Q1" "$Q2" "$Q3" "$Q4" "$Q5")
declare -a QNAMES=("Q1(2-table)" "Q2(3-table)" "Q3(4-table)" "Q4(aggregation)" "Q5(adversarial)")

echo "======================================================================"
echo "  qopt Benchmark  —  $(date)"
echo "======================================================================"
echo ""

for i in "${!QUERIES[@]}"; do
    qname="${QNAMES[$i]}"
    query="${QUERIES[$i]}"
    echo "----------------------------------------------------------------------"
    echo "  $qname"
    echo "  $query"
    echo "----------------------------------------------------------------------"
    # Run \bench command via stdin
    echo -e "\\\\bench $query\n\\\\q" | "$QOPT" --data "$DATADIR" 2>/dev/null
    echo ""
done

echo "======================================================================"
echo "  Benchmark complete. See design.pdf for detailed analysis."
echo "======================================================================"
