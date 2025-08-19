#!/bin/bash

# Constants
THREADS=10
DURATION=15
DB_FILE="./test_db"
OUTPUT_FILE="fairness_results_comprehensive.csv"

# Ensure the output file exists
echo "Hierarchy,Insert Ratio,Find Ratio,Update Ratio,Output" > "$OUTPUT_FILE"

# Hierarchy types: 0=FLAT, 1=BALANCED, 2=SKEWED, 3=DEEP, 4=GROUPED
HIERARCHY_TYPES=(0 1 2 3 4)

# Run the tests
for hierarchy in "${HIERARCHY_TYPES[@]}"; do
    for insert_ratio in $(seq 0.1 0.1 0.9); do
        for find_ratio in $(seq 0.1 0.1 $(echo "1.0 - $insert_ratio - 0.1" | bc -l)); do
            update_ratio=$(echo "1.0 - $insert_ratio - $find_ratio" | bc -l)
            if (( $(echo "$update_ratio >= 0.1" | bc -l) )); then
                echo "Running test with Hierarchy=$hierarchy, Insert Ratio=$insert_ratio, Find Ratio=$find_ratio, Update Ratio=$update_ratio"
                result=$(./upscale_fairness_test $THREADS $DURATION $DB_FILE $insert_ratio $find_ratio $hierarchy) 
                echo "$hierarchy,$insert_ratio,$find_ratio,$update_ratio,$result" >> "$OUTPUT_FILE"
            fi
        done
    done
done

echo "Completed tests. Results saved to '$OUTPUT_FILE'."