#!/bin/bash
# Monitor extraction progress

echo "======================================"
echo "Bolivia Extraction Monitor"
echo "======================================"
echo ""

echo "📊 Current Status:"
echo "  Time: $(date '+%H:%M:%S')"
echo ""

# Check if process is running
if ps aux 2>/dev/null | grep -q "[s]ql-databricks-bridge"; then
    echo "  ✅ Extraction process: RUNNING"
else
    echo "  ⏸️  Extraction process: COMPLETED or NOT RUNNING"
fi

echo ""
echo "📈 Progress:"

# Count successes
success_count=$(grep "Created table" newly_extracted_tables.log 2>/dev/null | wc -l)
echo "  ✅ Tables created: $success_count"

# Show last 3 successful tables
echo ""
echo "  Last 3 tables created:"
grep "Created table" newly_extracted_tables.log 2>/dev/null | tail -3 | while read line; do
    table_name=$(echo "$line" | grep -oP "bolivia\.\`\K[^`]+")
    row_count=$(echo "$line" | grep -oP "with \K[0-9,]+")
    echo "    • $table_name ($row_count rows)"
done

# Show currently processing
echo ""
echo "  🔄 Currently processing:"
current=$(grep "Executing query:" newly_extracted_tables.log 2>/dev/null | tail -1 | grep -oP "query: \K[^ ]+")
if [ -n "$current" ]; then
    echo "    • $current"
else
    echo "    • (none)"
fi

# Check for our two special tables
echo ""
echo "🎯 Target Tables Status:"
if grep -q "bolivia.nac_ato" newly_extracted_tables.log 2>/dev/null; then
    nac_rows=$(grep "bolivia.nac_ato" newly_extracted_tables.log | grep -oP "with \K[0-9,]+")
    echo "  ✅ nac_ato: SUCCESS ($nac_rows rows)"
else
    echo "  ⏳ nac_ato: Pending"
fi

if grep -q "bolivia.loc_psdata_compras" newly_extracted_tables.log 2>/dev/null; then
    loc_rows=$(grep "bolivia.loc_psdata_compras" newly_extracted_tables.log | grep -oP "with \K[0-9,]+")
    echo "  ✅ loc_psdata_compras: SUCCESS ($loc_rows rows)"
else
    echo "  ⏳ loc_psdata_compras: Pending or Processing"
fi

# Check for errors
echo ""
error_count=$(grep -i "error\|failed" newly_extracted_tables.log 2>/dev/null | grep -v DEBUG | wc -l)
if [ "$error_count" -gt 0 ]; then
    echo "⚠️  Errors detected: $error_count"
else
    echo "✅ No errors detected"
fi

echo ""
echo "======================================"
echo "Log file size: $(ls -lh newly_extracted_tables.log 2>/dev/null | awk '{print $5}')"
echo ""
echo "To check full log: tail -100 newly_extracted_tables.log"
echo "To monitor live: tail -f newly_extracted_tables.log"
echo "======================================"
