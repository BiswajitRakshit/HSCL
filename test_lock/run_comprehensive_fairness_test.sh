#!/bin/bash

# Comprehensive Fairness Test Runner and Analyzer
# This script runs upscale_fairness_test for every ratio and hierarchy type combination
# and generates a complete summary of fairness metrics

set -e

echo "=========================================="
echo "UPSCALE FAIRNESS COMPREHENSIVE TEST SUITE"
echo "=========================================="
echo "Started at: $(date)"
echo ""

# Configuration
THREADS=10
DURATION=15
DB_FILE="./comprehensive_test.db"
RESULTS_FILE="fairness_results_comprehensive.csv"
SUMMARY_CSV="fairness_summary_simple.csv"
SUMMARY_REPORT="fairness_comprehensive_summary.txt"

# Hierarchy types: 0=FLAT, 1=BALANCED, 2=SKEWED, 3=DEEP, 4=GROUPED
HIERARCHY_TYPES=(0 1 2 3 4)
HIERARCHY_NAMES=("FLAT" "BALANCED" "SKEWED" "DEEP" "GROUPED")

# Clean up previous results
rm -f "$DB_FILE" "$RESULTS_FILE" "$SUMMARY_CSV" "$SUMMARY_REPORT"

# Initialize results file
echo "Hierarchy,Insert Ratio,Find Ratio,Update Ratio,Output" > "$RESULTS_FILE"

echo "Test Configuration:"
echo "  Threads: $THREADS"
echo "  Duration per test: $DURATION seconds"
echo "  Database file: $DB_FILE"
echo ""

# Count total tests to run
total_tests=0
for hierarchy in "${HIERARCHY_TYPES[@]}"; do
    for insert_ratio in $(seq 0.1 0.1 0.8); do
        for find_ratio in $(seq 0.1 0.1 0.8); do
            update_ratio=$(echo "1.0 - $insert_ratio - $find_ratio" | bc -l)
            if (( $(echo "$update_ratio >= 0.1" | bc -l) )); then
                ((total_tests++))
            fi
        done
    done
done

echo "Total tests to run: $total_tests"
echo ""

# Run the comprehensive tests
test_count=0
failed_tests=0

for hierarchy in "${HIERARCHY_TYPES[@]}"; do
    hierarchy_name="${HIERARCHY_NAMES[$hierarchy]}"
    echo "=== Testing $hierarchy_name HIERARCHY (Type $hierarchy) ==="
    
    for insert_ratio in $(seq 0.1 0.1 0.8); do
        for find_ratio in $(seq 0.1 0.1 0.8); do
            update_ratio=$(echo "1.0 - $insert_ratio - $find_ratio" | bc -l)
            
            if (( $(echo "$update_ratio >= 0.1" | bc -l) )); then
                ((test_count++))
                
                # Clean database file
                rm -f "$DB_FILE"
                
                printf "[%3d/%3d] H:%s I:%.1f F:%.1f U:%.1f ... " \
                    "$test_count" "$total_tests" "$hierarchy_name" \
                    "$insert_ratio" "$find_ratio" "$update_ratio"
                
                # Run the test with timeout
                if timeout 120s ./upscale_fairness_test "$THREADS" "$DURATION" "$DB_FILE" \
                   "$insert_ratio" "$find_ratio" "$hierarchy" > temp_test_output.txt 2>&1; then
                    
                    # Capture the output
                    result=$(cat temp_test_output.txt | tr '\n' ' ')
                    echo "$hierarchy,$insert_ratio,$find_ratio,$update_ratio,$result" >> "$RESULTS_FILE"
                    echo "SUCCESS"
                else
                    echo "FAILED (timeout or error)"
                    ((failed_tests++))
                    echo "$hierarchy,$insert_ratio,$find_ratio,$update_ratio,TEST FAILED" >> "$RESULTS_FILE"
                fi
                
                # Clean up temp file
                rm -f temp_test_output.txt
            fi
        done
    done
    echo ""
done

echo "=========================================="
echo "TEST EXECUTION COMPLETED"
echo "=========================================="
echo "Total tests run: $test_count"
echo "Failed tests: $failed_tests"
echo "Success rate: $(echo "scale=2; ($test_count - $failed_tests) * 100 / $test_count" | bc -l)%"
echo ""

# Extract fairness metrics
echo "Extracting fairness metrics..."
if [[ -x "./extract_fairness_simple.sh" ]]; then
    ./extract_fairness_simple.sh > /dev/null
else
    echo "Warning: extract_fairness_simple.sh not found or not executable"
fi

# Generate comprehensive summary report
echo "Generating comprehensive summary report..."
{
    echo "UPSCALE FAIRNESS TEST - COMPREHENSIVE SUMMARY REPORT"
    echo "===================================================="
    echo "Generated on: $(date)"
    echo "Test Configurations: All hierarchy types with operation ratios from 0.1 to 0.8"
    echo ""
    
    if [[ -f "$SUMMARY_CSV" ]]; then
        total_configs=$(tail -n +2 "$SUMMARY_CSV" | wc -l)
        echo "EXECUTIVE SUMMARY:"
        echo "=================="
        echo "- Total test configurations: $total_configs"
        echo "- Failed tests: $failed_tests"
        
        # Calculate summary statistics
        if [[ $total_configs -gt 0 ]]; then
            echo ""
            echo "HIERARCHY PERFORMANCE SUMMARY:"
            echo "=============================="
            
            for i in "${!HIERARCHY_TYPES[@]}"; do
                hierarchy="${HIERARCHY_TYPES[$i]}"
                hierarchy_name="${HIERARCHY_NAMES[$i]}"
                
                count=$(awk -F',' -v h="$hierarchy" '$1==h {count++} END {print count+0}' "$SUMMARY_CSV")
                if [[ $count -gt 0 ]]; then
                    avg_fairness=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$6; count++} END {if(count>0) printf "%.4f", sum/count}' "$SUMMARY_CSV")
                    avg_variation=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$7; count++} END {if(count>0) printf "%.2f", sum/count}' "$SUMMARY_CSV")
                    min_variation=$(awk -F',' -v h="$hierarchy" '$1==h {print $7}' "$SUMMARY_CSV" | sort -n | head -1)
                    max_variation=$(awk -F',' -v h="$hierarchy" '$1==h {print $7}' "$SUMMARY_CSV" | sort -rn | head -1)
                    
                    echo ""
                    echo "$hierarchy_name HIERARCHY (Type $hierarchy):"
                    echo "$(printf '%.0s-' {1..40})"
                    echo "  Tests conducted: $count configurations"
                    echo "  Average Fairness Index: $avg_fairness"
                    echo "  Average Throughput Variation: $avg_variation%"
                    echo "  Variation Range: $min_variation% - $max_variation%"
                fi
            done
            
            echo ""
            echo "RANKING BY CONSISTENCY (Lower variation = Better):"
            echo "=================================================="
            awk -F',' 'NR>1 {hierarchy[$1] += $7; count[$1]++} END {
                for (h in hierarchy) {
                    avg = hierarchy[h]/count[h]
                    printf "%s,%.2f\n", h, avg
                }
            }' "$SUMMARY_CSV" | sort -t',' -k2 -n | while IFS=',' read -r h avg; do
                hierarchy_name="${HIERARCHY_NAMES[$h]}"
                echo "  $((++rank)). $hierarchy_name: $avg% average variation"
            done
        fi
    else
        echo "- Summary extraction failed - see raw results in $RESULTS_FILE"
    fi
    
    echo ""
    echo "FILES GENERATED:"
    echo "================"
    echo "- $RESULTS_FILE: Complete raw test results"
    echo "- $SUMMARY_CSV: Extracted fairness metrics"
    echo "- $SUMMARY_REPORT: This comprehensive report"
    echo ""
    echo "USAGE:"
    echo "======"
    echo "- View detailed results: head -50 $RESULTS_FILE"
    echo "- Analyze metrics: cat $SUMMARY_CSV"
    echo "- Read full report: cat $SUMMARY_REPORT"
    
} > "$SUMMARY_REPORT"

echo "=========================================="
echo "COMPREHENSIVE ANALYSIS COMPLETED"
echo "=========================================="
echo "Files generated:"
echo "  - Raw results: $RESULTS_FILE ($(wc -l < "$RESULTS_FILE") lines)"
if [[ -f "$SUMMARY_CSV" ]]; then
    echo "  - Metrics CSV: $SUMMARY_CSV ($(tail -n +2 "$SUMMARY_CSV" | wc -l) configurations)"
fi
echo "  - Summary report: $SUMMARY_REPORT"
echo ""
echo "Quick Summary:"
if [[ -f "$SUMMARY_CSV" ]]; then
    echo "Hierarchy Performance (by consistency):"
    for hierarchy in "${HIERARCHY_TYPES[@]}"; do
        hierarchy_name="${HIERARCHY_NAMES[$hierarchy]}"
        if avg_var=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$7; count++} END {if(count>0) printf "%.2f", sum/count}' "$SUMMARY_CSV" 2>/dev/null); then
            echo "  $hierarchy_name: $avg_var% average throughput variation"
        fi
    done
fi
echo ""
echo "Completed at: $(date)"
