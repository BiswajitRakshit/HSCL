#!/bin/bash

set -e

# Test configuration (quicker settings)
THREADS=8
DURATION=10
DB_FILE="./test_fairness_db_quick"
LOG_DIR="./fairness_logs_hfair_quick"
RESULTS_FILE="fairness_results_hfair_quick.csv"
SUMMARY_FILE="fairness_summary_hfair_quick.txt"

# Hierarchy types and their names
declare -A HIERARCHY_NAMES=(
    [0]="FLAT"
    [1]="BALANCED"
    [2]="SKEWED"
    [3]="DEEP"
    [4]="GROUPED"
)

# Reduced operation ratios for quicker testing
INSERT_RATIOS=(0.2 0.3 0.4)
FIND_RATIOS=(0.2 0.4 0.6)

# Create necessary directories
mkdir -p "$LOG_DIR"

# Initialize database file
if [ -f "$DB_FILE" ]; then
    rm -f "$DB_FILE"
fi
touch "$DB_FILE"

# Initialize results CSV with headers
echo "Hierarchy_Type,Hierarchy_Name,Insert_Ratio,Find_Ratio,Update_Ratio,Jain_Index,Coefficient_Variation,Gini_Coefficient,Throughput_Spread,Min_Ops,Max_Ops,Avg_Ops,Fairness_Assessment,Total_Operations" > "$RESULTS_FILE"

# Function to extract fairness metrics from output
extract_metrics() {
    local output="$1"
    
    # Extract numerical values using grep and awk
    jain_index=$(echo "$output" | grep "Jain's Fairness Index:" | awk '{print $NF}')
    coeff_var=$(echo "$output" | grep "Coefficient of Variation:" | awk '{print $NF}')
    gini_coeff=$(echo "$output" | grep "Gini coefficient:" | awk '{print $NF}')
    throughput_spread=$(echo "$output" | grep "Throughput spread:" | awk '{print $NF}')
    min_ops=$(echo "$output" | grep "Min operations per thread:" | awk '{print $NF}')
    max_ops=$(echo "$output" | grep "Max operations per thread:" | awk '{print $NF}')
    avg_ops=$(echo "$output" | grep "Average operations per thread:" | awk '{print $NF}')
    fairness_assessment=$(echo "$output" | grep "Fairness assessment:" | awk -F': ' '{print $2}')
    total_ops=$(echo "$output" | grep "Total operations:" | awk '{print $NF}')
    
    # Handle cases where values might be empty
    jain_index=${jain_index:-"N/A"}
    coeff_var=${coeff_var:-"N/A"}
    gini_coeff=${gini_coeff:-"N/A"}
    throughput_spread=${throughput_spread:-"N/A"}
    min_ops=${min_ops:-"N/A"}
    max_ops=${max_ops:-"N/A"}
    avg_ops=${avg_ops:-"N/A"}
    fairness_assessment=${fairness_assessment:-"N/A"}
    total_ops=${total_ops:-"N/A"}
    
    echo "$jain_index,$coeff_var,$gini_coeff,$throughput_spread,$min_ops,$max_ops,$avg_ops,$fairness_assessment,$total_ops"
}

# Function to run a single test
run_test() {
    local hierarchy="$1"
    local insert_ratio="$2"
    local find_ratio="$3"
    local update_ratio="$4"
    local hierarchy_name="${HIERARCHY_NAMES[$hierarchy]}"
    
    local log_file="$LOG_DIR/test_${hierarchy_name}_i${insert_ratio}_f${find_ratio}_u${update_ratio}.log"
    
    echo "Running test: Hierarchy=$hierarchy_name, Insert=$insert_ratio, Find=$find_ratio, Update=$update_ratio"
    
    # Run the test and capture output
    local output
    if output=$(timeout 180 ./upscale_fairness_test "$THREADS" "$DURATION" "$DB_FILE" "$insert_ratio" "$find_ratio" "$hierarchy" 2>&1); then
        echo "$output" > "$log_file"
        
        # Extract metrics
        local metrics
        metrics=$(extract_metrics "$output")
        
        # Write to CSV
        echo "$hierarchy,$hierarchy_name,$insert_ratio,$find_ratio,$update_ratio,$metrics" >> "$RESULTS_FILE"
        
        echo "  ✓ Completed successfully"
    else
        echo "  ✗ Test failed or timed out"
        echo "$hierarchy,$hierarchy_name,$insert_ratio,$find_ratio,$update_ratio,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED,FAILED" >> "$RESULTS_FILE"
    fi
    
    # Clean up database file for next test
    rm -f "$DB_FILE"
    touch "$DB_FILE"
}

# Main testing loop
echo "Starting quick fairness testing with hierarchical fairlocks..."
echo "Test configuration: $THREADS threads, $DURATION seconds per test"
echo "Results will be saved to: $RESULTS_FILE"
echo "Individual logs will be saved to: $LOG_DIR/"
echo ""

test_count=0
total_tests=0

# Calculate total number of tests
for hierarchy in "${!HIERARCHY_NAMES[@]}"; do
    for insert_ratio in "${INSERT_RATIOS[@]}"; do
        for find_ratio in "${FIND_RATIOS[@]}"; do
            update_ratio=$(echo "1.0 - $insert_ratio - $find_ratio" | bc -l)
            if (( $(echo "$update_ratio >= 0.05" | bc -l) )); then
                ((total_tests++))
            fi
        done
    done
done

echo "Total tests to run: $total_tests"
echo ""

# Run all tests
for hierarchy in "${!HIERARCHY_NAMES[@]}"; do
    for insert_ratio in "${INSERT_RATIOS[@]}"; do
        for find_ratio in "${FIND_RATIOS[@]}"; do
            update_ratio=$(echo "1.0 - $insert_ratio - $find_ratio" | bc -l)
            
            # Only run tests where update ratio is at least 0.05 (5%)
            if (( $(echo "$update_ratio >= 0.05" | bc -l) )); then
                ((test_count++))
                echo "[$test_count/$total_tests]"
                run_test "$hierarchy" "$insert_ratio" "$find_ratio" "$update_ratio"
                echo ""
            fi
        done
    done
done

# Generate summary report
echo "Generating summary report..."

{
    echo "HIERARCHICAL FAIRLOCK QUICK TEST SUMMARY"
    echo "========================================="
    echo "Test Configuration:"
    echo "  - Threads: $THREADS"
    echo "  - Duration per test: $DURATION seconds"
    echo "  - Total tests completed: $test_count"
    echo "  - Results file: $RESULTS_FILE"
    echo ""
    
    echo "FAIRNESS ANALYSIS BY HIERARCHY TYPE:"
    echo "===================================="
    
    for hierarchy in "${!HIERARCHY_NAMES[@]}"; do
        hierarchy_name="${HIERARCHY_NAMES[$hierarchy]}"
        echo ""
        echo "$hierarchy_name HIERARCHY (Type $hierarchy):"
        echo "$(printf '%.0s-' {1..40})"
        
        # Best Jain's Index for this hierarchy
        best_jain=$(awk -F',' -v h="$hierarchy" '$1==h && $6!="N/A" && $6!="FAILED" {print $6}' "$RESULTS_FILE" | sort -rn | head -1)
        if [ -n "$best_jain" ]; then
            best_config=$(awk -F',' -v h="$hierarchy" -v j="$best_jain" '$1==h && $6==j {print "Insert:" $3 " Find:" $4 " Update:" $5; exit}' "$RESULTS_FILE")
            echo "  Best Jain's Index: $best_jain ($best_config)"
        fi
        
        # Average Jain's Index for this hierarchy
        avg_jain=$(awk -F',' -v h="$hierarchy" '$1==h && $6!="N/A" && $6!="FAILED" {sum+=$6; count++} END {if(count>0) printf "%.4f", sum/count}' "$RESULTS_FILE")
        if [ -n "$avg_jain" ]; then
            echo "  Average Jain's Index: $avg_jain"
        fi
        
        # Count of fair vs unfair assessments
        fair_count=$(awk -F',' -v h="$hierarchy" '$1==h && $12=="Fair" {count++} END {print count+0}' "$RESULTS_FILE")
        unfair_count=$(awk -F',' -v h="$hierarchy" '$1==h && ($12=="Unfair" || $12=="Highly unfair") {count++} END {print count+0}' "$RESULTS_FILE")
        echo "  Fair assessments: $fair_count"
        echo "  Unfair assessments: $unfair_count"
    done
    
    echo ""
    echo "OVERALL RESULTS:"
    echo "================"
    
    # Overall best configuration
    overall_best=$(awk -F',' '$6!="N/A" && $6!="FAILED" {print $0}' "$RESULTS_FILE" | sort -t',' -k6 -rn | head -1)
    if [ -n "$overall_best" ]; then
        hierarchy_name=$(echo "$overall_best" | cut -d',' -f2)
        jain=$(echo "$overall_best" | cut -d',' -f6)
        config=$(echo "$overall_best" | cut -d',' -f3,4,5)
        echo "Best overall fairness: $hierarchy_name (Jain's Index: $jain)"
        echo "  Configuration: Insert:$(echo $config | cut -d',' -f1) Find:$(echo $config | cut -d',' -f2) Update:$(echo $config | cut -d',' -f3)"
    fi
    
    echo ""
    echo "Test completed at: $(date)"
    
} > "$SUMMARY_FILE"

echo ""
echo "Quick tests completed!"
echo "Results saved to: $RESULTS_FILE"
echo "Summary report saved to: $SUMMARY_FILE"
echo "Individual test logs saved to: $LOG_DIR/"
echo ""
echo "Quick summary:"
cat "$SUMMARY_FILE"
