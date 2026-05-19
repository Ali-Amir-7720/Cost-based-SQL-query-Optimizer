# Run Q1-Q5 benchmark (4 optimizer modes each) and save output.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Qopt = Join-Path $Root "qopt.exe"
$Data = Join-Path $Root "benchmark\benchdata"
$Out  = Join-Path $PSScriptRoot "benchmark_results.txt"

if (-not (Test-Path $Qopt)) {
    Write-Error "qopt.exe not found. Run 'mingw32-make' from project root."
}
if (-not (Test-Path $Data)) {
    Write-Error "benchdata not found. Run 'mingw32-make bench' or gen_data first."
}

$Q1 = "SELECT customers.name, orders.total FROM customers, orders WHERE customers.id = orders.customer_id AND customers.country = 'PK'"
$Q2 = "SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND customers.country = 'PK' AND orders.year = 2024"
$Q3 = "SELECT customers.name, products.name FROM customers, orders, line_items, products WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND line_items.product_id = products.id AND customers.country = 'PK' AND products.category = 'Electronics'"
$Q4 = "SELECT customers.country, SUM(orders.total) FROM customers, orders WHERE customers.id = orders.customer_id AND orders.year = 2024 GROUP BY customers.country"
$Q5 = "SELECT customers.name, orders.total FROM customers, orders, line_items WHERE customers.id = orders.customer_id AND orders.id = line_items.order_id AND orders.year = 2024 AND orders.total > 4000"

$queries = @(
    @{ Name = "Q1 (2-table selective)"; Sql = $Q1 },
    @{ Name = "Q2 (3-table selective)"; Sql = $Q2 },
    @{ Name = "Q3 (4-table selective)"; Sql = $Q3 },
    @{ Name = "Q4 (aggregation)"; Sql = $Q4 },
    @{ Name = "Q5 (adversarial)"; Sql = $Q5 }
)

$header = @"
======================================================================
  qopt Benchmark — $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
  Data: $Data
======================================================================

"@

$all = New-Object System.Text.StringBuilder
[void]$all.AppendLine($header)

foreach ($q in $queries) {
    $block = @"

----------------------------------------------------------------------
  $($q.Name)
  $($q.Sql)
----------------------------------------------------------------------
"@
    [void]$all.AppendLine($block)
    $input = "\bench $($q.Sql)`n\q"
    $result = $input | & $Qopt --data $Data 2>&1 | Out-String
    [void]$all.AppendLine($result)
}

# Q3 with full optimizer + estimate accuracy (for design document)
$q3acc = @"

----------------------------------------------------------------------
  Q3 — Estimate accuracy (FULL optimizer, with execution)
----------------------------------------------------------------------
"@
[void]$all.AppendLine($q3acc)
$q3sql = $Q3
$q3input = "SELECT $q3sql`n\q"
$q3out = $q3input | & $Qopt --data $Data 2>&1 | Out-String
[void]$all.AppendLine($q3out)

$text = $all.ToString()
$text | Set-Content -Path $Out -Encoding UTF8
Write-Host "Benchmark written to $Out"
