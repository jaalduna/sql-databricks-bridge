#!/bin/bash
# =============================================================================
# Extract PS_LATAM Tables from All KTCLSQL Servers
# =============================================================================
# Description: Extracts loc_psdata_compras and loc_psdata_procesado from
#              each KTCLSQL server to Databricks Unity Catalog
#
# Target Schemas:
#   - 000-sql-databricks-bridge.KTCLSQL001
#   - 000-sql-databricks-bridge.KTCLSQL002
#   - 000-sql-databricks-bridge.KTCLSQL003
#   - 000-sql-databricks-bridge.KTCLSQL004
#   - 000-sql-databricks-bridge.KTCLSQL005
#
# Usage:
#   ./extract_ps_latam_all_servers.sh          # Extract from all servers
#   ./extract_ps_latam_all_servers.sh 2        # Extract from KTCLSQL002 only
# =============================================================================

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
CATALOG="000-sql-databricks-bridge"
QUERIES_BASE="queries/servers"
DATABASE="PS_LATAM"

# Server list (or single server from argument)
if [ -n "$1" ]; then
    SERVERS=("$1")
    echo -e "${YELLOW}📍 Single server mode: KTCLSQL00${1}${NC}"
else
    SERVERS=(1 2 3 4 5)
    echo -e "${GREEN}🌐 Multi-server mode: All KTCLSQL servers${NC}"
fi

# Tables to extract
TABLES=("loc_psdata_compras" "loc_psdata_procesado")

# Function to extract a single table
extract_table() {
    local server_num=$1
    local table_name=$2
    local server="KTCLSQL00${server_num}.KT.group.local"
    local schema="KTCLSQL00${server_num}"
    local queries_path="${QUERIES_BASE}/KTCLSQL00${server_num}"
    
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}🔄 Extracting: ${table_name}${NC}"
    echo -e "${GREEN}   Server:     ${server}${NC}"
    echo -e "${GREEN}   Database:   ${DATABASE}${NC}"
    echo -e "${GREEN}   Target:     ${CATALOG}.${schema}.${table_name}${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Check if query file exists
    if [ ! -f "${queries_path}/${table_name}.sql" ]; then
        echo -e "${RED}❌ Query file not found: ${queries_path}/${table_name}.sql${NC}"
        return 1
    fi
    
    # Run extraction
    poetry run sql-databricks-bridge extract \
        --queries-path "${queries_path}" \
        --server "${server}" \
        --database "${DATABASE}" \
        --query-name "${table_name}" \
        --destination "${CATALOG}.${schema}" \
        --verbose
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Success: ${table_name} from ${server}${NC}"
    else
        echo -e "${RED}❌ Failed: ${table_name} from ${server}${NC}"
        return 1
    fi
}

# Main extraction loop
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║          Starting PS_LATAM Multi-Server Extraction                   ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
echo ""

TOTAL_SUCCESS=0
TOTAL_FAILED=0

for server_num in "${SERVERS[@]}"; do
    echo ""
    echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}   Server: KTCLSQL00${server_num}.KT.group.local${NC}"
    echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
    
    for table in "${TABLES[@]}"; do
        if extract_table "${server_num}" "${table}"; then
            ((TOTAL_SUCCESS++))
        else
            ((TOTAL_FAILED++))
            echo -e "${YELLOW}⚠️  Continuing with next table...${NC}"
        fi
    done
done

# Summary
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    EXTRACTION SUMMARY                                ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ✅ Successful extractions: ${GREEN}${TOTAL_SUCCESS}${NC}"
echo -e "  ❌ Failed extractions:     ${RED}${TOTAL_FAILED}${NC}"
echo ""

if [ $TOTAL_FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 All extractions completed successfully!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  Some extractions failed. Check logs for details.${NC}"
    exit 1
fi
