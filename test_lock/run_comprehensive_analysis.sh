#!/bin/bash

# Comprehensive Lock Fairness Analysis Script
# Runs all tests and generates graphs automatically

set -e  # Exit on any error

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
OUTPUT_DIR="analysis_${TIMESTAMP}"
CSV_FILE="${OUTPUT_DIR}/fairness_results_comprehensive.csv"
GRAPHS_DIR="${OUTPUT_DIR}/graphs"
LOG_FILE="${OUTPUT_DIR}/analysis.log"

echo "=== COMPREHENSIVE LOCK FAIRNESS ANALYSIS ==="
echo "Starting analysis at $(date)"
echo "Output directory: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"
mkdir -p "$GRAPHS_DIR"

# Redirect all output to log file as well as console
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Step 1: Running comprehensive fairness tests..."
echo "This may take 30-60 minutes depending on system performance"
echo ""

# Modify the comprehensive test script to use our output directory
sed "s|OUTPUT_FILE=\"fairness_results.csv\"|OUTPUT_FILE=\"$CSV_FILE\"|g" test_fairness_comprehensive.sh > "${OUTPUT_DIR}/test_script.sh"
sed -i "s|LOG_DIR=\"./test_logs\"|LOG_DIR=\"${OUTPUT_DIR}/test_logs\"|g" "${OUTPUT_DIR}/test_script.sh"
chmod +x "${OUTPUT_DIR}/test_script.sh"

# Run the comprehensive tests
"${OUTPUT_DIR}/test_script.sh"

echo ""
echo "Step 2: Generating graphs and visualizations..."
echo ""

# Check if CSV file was created and has data
if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: CSV file not found at $CSV_FILE"
    exit 1
fi

# Count the number of successful tests
successful_tests=$(grep -v "ERROR\|TIMEOUT" "$CSV_FILE" | wc -l)
total_lines=$(wc -l < "$CSV_FILE")
echo "Found $successful_tests successful tests out of $((total_lines - 1)) total tests"

if [ $successful_tests -lt 5 ]; then
    echo "WARNING: Very few successful tests ($successful_tests). Graphs may not be meaningful."
fi

# Generate graphs
python3 plot_fairness_results.py "$CSV_FILE" --output "$GRAPHS_DIR"

echo ""
echo "Step 3: Creating analysis summary..."
echo ""

# Create comprehensive analysis report
REPORT_FILE="${OUTPUT_DIR}/analysis_report.md"
{
    echo "# Comprehensive Lock Fairness Analysis Report"
    echo ""
    echo "**Generated:** $(date)"
    echo "**Total Tests:** $((total_lines - 1))"
    echo "**Successful Tests:** $successful_tests"
    echo ""
    
    echo "## Test Configuration"
    echo "- **Threads:** 10"
    echo "- **Duration:** 15 seconds per test"
    echo "- **Lock Types:** MUTEX, SPINLOCK, RWLOCK, ADAPTIVE_MUTEX"
    echo "- **Operation Ratios:** All combinations from 0.1 to 0.8 (0.1 increments)"
    echo ""
    
    echo "## Key Findings"
    echo ""
    
    # Extract top-level findings from CSV
    echo "### Best Performing Configurations:"
    grep -v "ERROR\|TIMEOUT" "$CSV_FILE" | sort -t',' -k7 -nr | head -3 | while IFS=',' read -r lock insert find update threads duration jains cov gini spread min max avg total crit high norm low bg switches assess; do
        echo "- **$lock** (I:$insert, F:$find, U:$update): Jain's Index = $jains ($assess)"
    done
    echo ""
    
    echo "### Worst Performing Configurations:"
    grep -v "ERROR\|TIMEOUT" "$CSV_FILE" | sort -t',' -k7 -n | head -3 | while IFS=',' read -r lock insert find update threads duration jains cov gini spread min max avg total crit high norm low bg switches assess; do
        echo "- **$lock** (I:$insert, F:$find, U:$update): Jain's Index = $jains ($assess)"
    done
    echo ""
    
    echo "### Lock Type Performance Summary:"
    for lock_type in "MUTEX" "SPINLOCK" "RWLOCK" "ADAPTIVE_MUTEX"; do
        if grep -q "^$lock_type," "$CSV_FILE"; then
            avg_jains=$(grep "^$lock_type," "$CSV_FILE" | grep -v "ERROR\|TIMEOUT" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.4f", sum/count; else print "N/A"}')
            test_count=$(grep "^$lock_type," "$CSV_FILE" | grep -v "ERROR\|TIMEOUT" | wc -l)
            echo "- **$lock_type:** Average Jain's Index = $avg_jains ($test_count tests)"
        fi
    done
    echo ""
    
    echo "## Generated Files"
    echo ""
    echo "### Data Files:"
    echo "- \`fairness_results_comprehensive.csv\` - Complete test results"
    echo "- \`test_logs/\` - Individual test logs"
    echo "- \`summary_statistics.csv\` - Statistical summary"
    echo ""
    echo "### Visualizations:"
    echo "- \`fairness_comparison_boxplot.png\` - Box plot comparing lock types"
    echo "- \`fairness_heatmaps.png\` - Heatmaps showing ratio impact"
    echo "- \`fairness_violin_plot.png\` - Distribution analysis"
    echo "- \`metrics_scatter_matrix.png\` - Correlation analysis"
    echo "- \`operation_ratio_impact.png\` - Ratio impact analysis"
    echo "- \`performance_ranking.png\` - Lock type ranking"
    echo "- \`summary_statistics.png\` - Statistical table"
    echo ""
    
    echo "## Interpretation Guide"
    echo ""
    echo "### Jain's Fairness Index:"
    echo "- **1.0:** Perfect fairness (all threads get equal access)"
    echo "- **≥0.95:** Excellent fairness"
    echo "- **≥0.80:** Good fairness"
    echo "- **≥0.60:** Moderate fairness"
    echo "- **≥0.40:** Poor fairness"
    echo "- **<0.40:** Very poor fairness"
    echo ""
    echo "### Other Metrics:"
    echo "- **CoV (Coefficient of Variation):** Lower is better (less variation)"
    echo "- **Gini Coefficient:** Lower is better (less inequality)"
    echo "- **Throughput Spread:** Lower is better (less performance variation)"
    echo ""
    
    echo "## Usage"
    echo ""
    echo "To view the graphs, open the PNG files in the \`graphs/\` directory."
    echo "For detailed analysis, examine the CSV file with spreadsheet software."
    echo ""
    echo "---"
    echo "*Analysis completed on $(date)*"
    
} > "$REPORT_FILE"

# Create a simple index file
INDEX_FILE="${OUTPUT_DIR}/index.html"
{
    echo "<!DOCTYPE html>"
    echo "<html><head><title>Lock Fairness Analysis Results</title></head><body>"
    echo "<h1>Lock Fairness Analysis Results</h1>"
    echo "<p>Generated: $(date)</p>"
    echo "<h2>Graphs</h2><ul>"
    for graph in "$GRAPHS_DIR"/*.png; do
        if [ -f "$graph" ]; then
            basename_graph=$(basename "$graph")
            echo "<li><a href=\"graphs/$basename_graph\">$basename_graph</a></li>"
        fi
    done
    echo "</ul>"
    echo "<h2>Data Files</h2><ul>"
    echo "<li><a href=\"$(basename "$CSV_FILE")\">Complete Results (CSV)</a></li>"
    echo "<li><a href=\"graphs/summary_statistics.csv\">Summary Statistics (CSV)</a></li>"
    echo "<li><a href=\"$(basename "$REPORT_FILE")\">Analysis Report (Markdown)</a></li>"
    echo "</ul></body></html>"
} > "$INDEX_FILE"

echo ""
echo "=== ANALYSIS COMPLETE ==="
echo ""
echo "Results saved to: $OUTPUT_DIR/"
echo ""
echo "Files created:"
echo "  📊 $CSV_FILE - Complete test results"
echo "  📈 $GRAPHS_DIR/ - All visualization graphs"
echo "  📋 $REPORT_FILE - Analysis summary report"
echo "  🌐 $INDEX_FILE - HTML index for easy browsing"
echo "  📝 $LOG_FILE - Complete analysis log"
echo ""
echo "To view results:"
echo "  • Open graphs: ls $GRAPHS_DIR/"
echo "  • View report: cat $REPORT_FILE"
echo "  • Browse HTML: open $INDEX_FILE (or use a web browser)"
echo ""
echo "Analysis completed at $(date)"
echo "Total time: $(date -d@$(($(date +%s) - $(stat -c %Y "$LOG_FILE" 2>/dev/null || echo $(date +%s))))) -u +%H:%M:%S)"
