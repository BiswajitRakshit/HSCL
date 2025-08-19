#!/bin/bash

# Quick Hierarchical Lock Fairness Testing Script
# Tests hierarchical lock configurations with selected operation ratios

set -e

PROGRAM="./upscale_fairness_test"
THREADS=8
DURATION=10
OUTPUT_FILE="fairness_results_hierarchical_quick.csv"
LOG_FILE="fairness_test_hierarchical_quick.log"

# Hierarchical lock types (assuming these are available in your program)
LOCK_TYPES=("HIERARCHICAL_MUTEX" "HIERARCHICAL_SPINLOCK" "HIERARCHICAL_RWLOCK" "HIERARCHICAL_ADAPTIVE")

# Selected operation ratios for quick testing
RATIOS=(
    "0.1 0.8 0.1"
    "0.2 0.7 0.1"
    "0.3 0.6 0.1"
    "0.4 0.5 0.1"
    "0.1 0.7 0.2"
    "0.2 0.6 0.2"
    "0.3 0.5 0.2"
    "0.1 0.6 0.3"
)

echo "Starting hierarchical lock fairness testing..."
echo "Program: $PROGRAM"
echo "Threads: $THREADS, Duration: ${DURATION}s"
echo "Lock types: ${LOCK_TYPES[*]}"
echo "Output: $OUTPUT_FILE"
echo ""

# Initialize CSV header
echo "Lock_Type,Insert_Ratio,Find_Ratio,Update_Ratio,Jains_Index,Coefficient_of_Variation,Gini_Coefficient,Min_Ops,Max_Ops,Avg_Ops,Total_Ops,Throughput_Spread,Test_Duration" > "$OUTPUT_FILE"

# Initialize log
echo "=== Hierarchical Lock Fairness Test Log ===" > "$LOG_FILE"
echo "Started: $(date)" >> "$LOG_FILE"
echo "" >> "$LOG_FILE"

TOTAL_TESTS=$((${#LOCK_TYPES[@]} * ${#RATIOS[@]}))
CURRENT_TEST=0

for lock_type in "${LOCK_TYPES[@]}"; do
    echo "Testing $lock_type..."
    
    for ratio in "${RATIOS[@]}"; do
        CURRENT_TEST=$((CURRENT_TEST + 1))
        read -r insert_ratio find_ratio update_ratio <<< "$ratio"
        
        echo "  [$CURRENT_TEST/$TOTAL_TESTS] Insert:$insert_ratio Find:$find_ratio Update:$update_ratio"
        
        # Log test start
        echo "Test $CURRENT_TEST: $lock_type I:$insert_ratio F:$find_ratio U:$update_ratio" >> "$LOG_FILE"
        
        # Run the test with timeout
        timeout $((DURATION + 5))s "$PROGRAM" \
            --lock-type "$lock_type" \
            --threads "$THREADS" \
            --duration "$DURATION" \
            --insert-ratio "$insert_ratio" \
            --find-ratio "$find_ratio" \
            --update-ratio "$update_ratio" \
            --fairness-metrics \
            2>&1 | tee -a "$LOG_FILE" | {
            
            # Parse output for fairness metrics
            jains_index=""
            coefficient_of_variation=""
            gini_coefficient=""
            min_ops=""
            max_ops=""
            avg_ops=""
            total_ops=""
            throughput_spread=""
            
            while IFS= read -r line; do
                if [[ $line =~ "Jain's Fairness Index:"[[:space:]]*([0-9]+\.[0-9]+) ]]; then
                    jains_index="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Coefficient of Variation:"[[:space:]]*([0-9]+\.[0-9]+) ]]; then
                    coefficient_of_variation="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Gini Coefficient:"[[:space:]]*([0-9]+\.[0-9]+) ]]; then
                    gini_coefficient="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Min operations per thread:"[[:space:]]*([0-9]+) ]]; then
                    min_ops="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Max operations per thread:"[[:space:]]*([0-9]+) ]]; then
                    max_ops="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Average operations per thread:"[[:space:]]*([0-9]+\.[0-9]+) ]]; then
                    avg_ops="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Total operations:"[[:space:]]*([0-9]+) ]]; then
                    total_ops="${BASH_REMATCH[1]}"
                elif [[ $line =~ "Throughput spread:"[[:space:]]*([0-9]+\.[0-9]+) ]]; then
                    throughput_spread="${BASH_REMATCH[1]}"
                fi
            done
            
            # Default values for missing metrics
            jains_index="${jains_index:-ERROR}"
            coefficient_of_variation="${coefficient_of_variation:-ERROR}"
            gini_coefficient="${gini_coefficient:-ERROR}"
            min_ops="${min_ops:-0}"
            max_ops="${max_ops:-0}"
            avg_ops="${avg_ops:-0}"
            total_ops="${total_ops:-0}"
            throughput_spread="${throughput_spread:-ERROR}"
            
            # Write to CSV
            echo "$lock_type,$insert_ratio,$find_ratio,$update_ratio,$jains_index,$coefficient_of_variation,$gini_coefficient,$min_ops,$max_ops,$avg_ops,$total_ops,$throughput_spread,$DURATION" >> "$OUTPUT_FILE"
            
            # Brief progress feedback
            if [[ "$jains_index" != "ERROR" ]]; then
                fairness_assessment="GOOD"
                if (( $(echo "$jains_index < 0.8" | bc -l) )); then
                    fairness_assessment="MODERATE"
                fi
                if (( $(echo "$jains_index < 0.6" | bc -l) )); then
                    fairness_assessment="POOR"
                fi
                echo "    Result: Jain's Index = $jains_index ($fairness_assessment)"
            else
                echo "    Result: ERROR or TIMEOUT"
            fi
        }
        
        echo "" >> "$LOG_FILE"
    done
    echo ""
done

# Generate quick summary
echo "=== HIERARCHICAL TEST SUMMARY ===" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "Total tests: $TOTAL_TESTS" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Count successful tests per lock type
for lock_type in "${LOCK_TYPES[@]}"; do
    successful=$(grep "^$lock_type," "$OUTPUT_FILE" | grep -v "ERROR" | wc -l)
    total=$(grep "^$lock_type," "$OUTPUT_FILE" | wc -l)
    echo "$lock_type: $successful/$total successful tests" | tee -a "$LOG_FILE"
    
    if [ "$successful" -gt 0 ]; then
        avg_fairness=$(grep "^$lock_type," "$OUTPUT_FILE" | grep -v "ERROR" | cut -d',' -f5 | awk '{sum+=$1; count++} END {if(count>0) print sum/count; else print "N/A"}')
        if [[ "$avg_fairness" != "N/A" ]]; then
            printf "  Average Jain's Index: %.4f\n" "$avg_fairness" | tee -a "$LOG_FILE"
        fi
    fi
done

echo "" | tee -a "$LOG_FILE"
echo "Results saved to: $OUTPUT_FILE" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
echo ""
echo "Quick hierarchical fairness test completed!"
echo "Run 'python3 plot_fairness_results.py $OUTPUT_FILE' to generate graphs."
