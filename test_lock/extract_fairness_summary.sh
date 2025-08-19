#!/bin/bash

# Advanced Fairness Metrics Extraction Script
INPUT_FILE="fairness_results_comprehensive.csv"
OUTPUT_SUMMARY="fairness_summary.csv"
DETAILED_SUMMARY="fairness_detailed_summary.txt"

# Initialize output files
echo "Hierarchy_Type,Hierarchy_Name,Insert_Ratio,Find_Ratio,Update_Ratio,Fairness_Index,Throughput_Variation,Total_Ops_Per_Sec,Min_Ops,Max_Ops,Avg_Ops" > "$OUTPUT_SUMMARY"

# Hierarchy type mappings
declare -A HIERARCHY_NAMES=(
    [0]="FLAT"
    [1]="BALANCED"
    [2]="SKEWED"
    [3]="DEEP"
    [4]="GROUPED"
)

# Extract fairness metrics from the comprehensive results
# Correct previous awk command to use a compatible approach
IFS=""
echo "Extracting fairness metrics from comprehensive test results..."

# Process the results file
awk -F',' '
BEGIN {
    current_hierarchy = ""
    current_insert = ""
    current_find = ""
    current_update = ""
}

# Match test configuration lines
/^[0-4],[0-9]\.[0-9],[0-9]\.[0-9]/ {
    current_hierarchy = $1
    current_insert = $2
    current_find = $3
    current_update = $4
}

# Extract fairness metrics
/Fairness Index:/ {
    fairness_index = $0 ~ /Fairness Index:/ ? substr($(NF), index($(NF), ":") + 2) : "N/A"
}

/Throughput Variation:/ {
    variation = $0 ~ /Throughput Variation:/ ? substr($(NF), index($(NF), ":") + 2) : "N/A"
}

/Total:.*ops\/sec/ {
    total_ops = $0 ~ /Total:/ ? substr($(NF-1), index($(NF-1), " ") + 1) : "N/A"
}

/Min ops:.*Max ops:.*Avg ops:/ {
    match($0, /Min ops: ([0-9.]+), Max ops: ([0-9.]+), Avg ops: ([0-9.]+)/, ops)
    min_ops = ops[1]
    max_ops = ops[2]
    avg_ops = ops[3]
    
    # Output the extracted metrics
    if (current_hierarchy != "" && fairness_index != "" && variation != "") {
        print current_hierarchy "," current_insert "," current_find "," current_update "," fairness_index "," variation "," total_ops "," min_ops "," max_ops "," avg_ops
        
        # Reset variables
        fairness_index = ""
        variation = ""
        total_ops = ""
        min_ops = ""
        max_ops = ""
        avg_ops = ""
    }
}
' "$INPUT_FILE" > temp_metrics.csv

# Add hierarchy names and create final summary
echo "Processing extracted metrics..."
while IFS=',' read -r hierarchy insert find update fairness variation total_ops min_ops max_ops avg_ops; do
    hierarchy_name=${HIERARCHY_NAMES[$hierarchy]:-"UNKNOWN"}
    echo "$hierarchy,$hierarchy_name,$insert,$find,$update,$fairness,$variation,$total_ops,$min_ops,$max_ops,$avg_ops" >> "$OUTPUT_SUMMARY"
done < temp_metrics.csv

# Generate detailed summary report
echo "Generating detailed summary report..."
{
    echo "COMPREHENSIVE FAIRNESS TEST SUMMARY"
    echo "==================================="
    echo "Generated on: $(date)"
    echo ""
    
    total_tests=$(wc -l < temp_metrics.csv)
    echo "Total test configurations: $total_tests"
    echo ""
    
    echo "SUMMARY BY HIERARCHY TYPE:"
    echo "==========================="
    
    for hierarchy in "${!HIERARCHY_NAMES[@]}"; do
        hierarchy_name=${HIERARCHY_NAMES[$hierarchy]}
        echo ""
        echo "$hierarchy_name HIERARCHY (Type $hierarchy):"
        echo "$(printf '%.0s-' {1..40})"
        
        # Count tests for this hierarchy
        test_count=$(awk -F',' -v h="$hierarchy" '$1==h {count++} END {print count+0}' temp_metrics.csv)
        echo "  Number of test configurations: $test_count"
        
        if [ $test_count -gt 0 ]; then
            # Best fairness index
            best_fairness=$(awk -F',' -v h="$hierarchy" '$1==h {print $5}' temp_metrics.csv | sort -rn | head -1)
            best_config=$(awk -F',' -v h="$hierarchy" -v f="$best_fairness" '$1==h && $5==f {print "Insert:" $3 " Find:" $4 " Update:" $5; exit}' temp_metrics.csv)
            echo "  Best Fairness Index: $best_fairness ($best_config)"
            
            # Worst fairness index
            worst_fairness=$(awk -F',' -v h="$hierarchy" '$1==h {print $5}' temp_metrics.csv | sort -n | head -1)
            worst_config=$(awk -F',' -v h="$hierarchy" -v f="$worst_fairness" '$1==h && $5==f {print "Insert:" $3 " Find:" $4 " Update:" $5; exit}' temp_metrics.csv)
            echo "  Worst Fairness Index: $worst_fairness ($worst_config)"
            
            # Average fairness index
            avg_fairness=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$5; count++} END {if(count>0) printf "%.4f", sum/count}' temp_metrics.csv)
            echo "  Average Fairness Index: $avg_fairness"
            
            # Average throughput variation
            avg_variation=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$6; count++} END {if(count>0) printf "%.2f", sum/count}' temp_metrics.csv)
            echo "  Average Throughput Variation: $avg_variation%"
            
            # Best throughput (highest ops/sec)
            best_throughput=$(awk -F',' -v h="$hierarchy" '$1==h && $7!="" {print $7}' temp_metrics.csv | sort -rn | head -1)
            if [ -n "$best_throughput" ]; then
                echo "  Best Throughput: $best_throughput ops/sec"
            fi
        fi
    done
    
    echo ""
    echo "OVERALL STATISTICS:"
    echo "=================="
    
    # Overall best fairness
    overall_best=$(awk -F',' 'NR>1 {print $0}' temp_metrics.csv | sort -t',' -k5 -rn | head -1)
    if [ -n "$overall_best" ]; then
        hierarchy=$(echo "$overall_best" | cut -d',' -f1)
        hierarchy_name=${HIERARCHY_NAMES[$hierarchy]}
        fairness=$(echo "$overall_best" | cut -d',' -f5)
        config=$(echo "$overall_best" | cut -d',' -f3,4,5)
        echo "Best Overall Fairness: $fairness ($hierarchy_name hierarchy)"
        echo "  Configuration: Insert:$(echo $config | cut -d',' -f1) Find:$(echo $config | cut -d',' -f2) Update:$(echo $config | cut -d',' -f3)"
    fi
    
    # Overall worst fairness
    overall_worst=$(awk -F',' 'NR>1 {print $0}' temp_metrics.csv | sort -t',' -k5 -n | head -1)
    if [ -n "$overall_worst" ]; then
        hierarchy=$(echo "$overall_worst" | cut -d',' -f1)
        hierarchy_name=${HIERARCHY_NAMES[$hierarchy]}
        fairness=$(echo "$overall_worst" | cut -d',' -f5)
        config=$(echo "$overall_worst" | cut -d',' -f3,4,5)
        echo "Worst Overall Fairness: $fairness ($hierarchy_name hierarchy)"
        echo "  Configuration: Insert:$(echo $config | cut -d',' -f1) Find:$(echo $config | cut -d',' -f2) Update:$(echo $config | cut -d',' -f3)"
    fi
    
    echo ""
    echo "FILES GENERATED:"
    echo "================"
    echo "  $OUTPUT_SUMMARY - Structured CSV data"
    echo "  $DETAILED_SUMMARY - This detailed report"
    echo ""
    
} > "$DETAILED_SUMMARY"

# Cleanup
rm -f temp_metrics.csv

echo "Fairness metrics extraction completed!"
echo "Results saved to:"
echo "  - Summary CSV: $OUTPUT_SUMMARY"
echo "  - Detailed report: $DETAILED_SUMMARY"
echo ""
echo "Quick stats:"
test_count=$(tail -n +2 "$OUTPUT_SUMMARY" | wc -l)
echo "  Total test configurations processed: $test_count"
echo "  Use 'cat $DETAILED_SUMMARY' to see the full analysis"
