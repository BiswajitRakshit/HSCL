#!/bin/bash

# Parameters
THREADS=10
DURATION=15
DB_FILE="/tmp/test_db"

# Ensure the database file exists
if [ ! -f "$DB_FILE" ]; then
    touch "$DB_FILE"
fi

# Lock hierarchies
LOCK_HIERARCHIES=(0 1 2 3 4)

# Operation ratios
RATIOS=(0.1 0.2 0.3 0.4)

# Output CSV
OUTPUT_FILE="fairness_results_comprehensive.csv"
echo "Hierarchy,Insert Ratio,Find Ratio,Update Ratio,Output" > "$OUTPUT_FILE"

# Iterate over the hierarchy types and ratios
for HIERARCHY in "${LOCK_HIERARCHIES[@]}"; do
    for INSERT_RATIO in "${RATIOS[@]}"; do
        for FIND_RATIO in "${RATIOS[@]}"; do
            if (( $(echo "$INSERT_RATIO + $FIND_RATIO < 1.0" | bc -l) )); then
                UPDATE_RATIO=$(echo "1.0 - $INSERT_RATIO - $FIND_RATIO" | bc -l)
                OUTPUT=$(./upscale_fairness_test $THREADS $DURATION $DB_FILE $INSERT_RATIO $FIND_RATIO $HIERARCHY)
                echo "$HIERARCHY,$INSERT_RATIO,$FIND_RATIO,$UPDATE_RATIO,$OUTPUT" >> "$OUTPUT_FILE"
            fi
        done
    done
done

echo "Tests complete. Results saved in $OUTPUT_FILE."
