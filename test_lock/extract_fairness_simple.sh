#!/bin/bash

# Simple Fairness Metrics Extraction Script
INPUT_FILE="fairness_results_comprehensive.csv"
OUTPUT_SUMMARY="fairness_summary_simple.csv"
TEMP_FILE="temp_extraction.txt"

# Initialize output file
echo "Hierarchy_Type,Hierarchy_Name,Insert_Ratio,Find_Ratio,Update_Ratio,Fairness_Index,Throughput_Variation" > "$OUTPUT_SUMMARY"

# Hierarchy type mappings
declare -A HIERARCHY_NAMES=(
    [0]="FLAT"
    [1]="BALANCED"
    [2]="SKEWED"
    [3]="DEEP"
    [4]="GROUPED"
)

echo "Extracting fairness metrics from comprehensive test results..."

# Read the input file and extract metrics using simple grep/sed
current_hierarchy=""
current_insert=""
current_find=""
current_update=""

while IFS= read -r line; do
    # Check if this is a test configuration line
    if [[ $line =~ ^[0-4],[0-9]\.[0-9],[0-9]\.[0-9] ]]; then
        current_hierarchy=$(echo "$line" | cut -d',' -f1)
        current_insert=$(echo "$line" | cut -d',' -f2)
        current_find=$(echo "$line" | cut -d',' -f3)
        current_update=$(echo "$line" | cut -d',' -f4)
    fi
    
    # Extract fairness index
    if [[ $line =~ "Fairness Index:" ]]; then
        fairness_index=$(echo "$line" | grep -o "Fairness Index: [0-9.]*" | grep -o "[0-9.]*")
    fi
    
    # Extract throughput variation and write the complete record
    if [[ $line =~ "Throughput Variation:" ]]; then
        throughput_variation=$(echo "$line" | grep -o "Throughput Variation: [0-9.]*%" | grep -o "[0-9.]*")
        
        # Write complete record if we have all required data
        if [[ -n "$current_hierarchy" && -n "$fairness_index" && -n "$throughput_variation" ]]; then
            hierarchy_name="${HIERARCHY_NAMES[$current_hierarchy]}"
            echo "$current_hierarchy,$hierarchy_name,$current_insert,$current_find,$current_update,$fairness_index,$throughput_variation" >> "$OUTPUT_SUMMARY"
        fi
    fi
    
done < "$INPUT_FILE"

# Generate summary statistics
echo ""
echo "Summary generation completed!"
echo "Results saved to: $OUTPUT_SUMMARY"

# Show quick statistics
total_configs=$(tail -n +2 "$OUTPUT_SUMMARY" | wc -l)
echo "Total test configurations processed: $total_configs"

# Show sample results
echo ""
echo "Sample results (first 10 entries):"
head -11 "$OUTPUT_SUMMARY"

echo ""
echo "Summary by hierarchy type:"
for hierarchy in "${!HIERARCHY_NAMES[@]}"; do
    hierarchy_name="${HIERARCHY_NAMES[$hierarchy]}"
    count=$(awk -F',' -v h="$hierarchy" '$1==h {count++} END {print count+0}' "$OUTPUT_SUMMARY")
    if [ $count -gt 0 ]; then
        avg_fairness=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$6; count++} END {if(count>0) printf "%.4f", sum/count}' "$OUTPUT_SUMMARY")
        avg_variation=$(awk -F',' -v h="$hierarchy" '$1==h {sum+=$7; count++} END {if(count>0) printf "%.2f", sum/count}' "$OUTPUT_SUMMARY")
        echo "  $hierarchy_name: $count tests, Avg Fairness: $avg_fairness, Avg Variation: $avg_variation%"
    fi
done
