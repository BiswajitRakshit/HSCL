#!/bin/bash

# Quick Lock Fairness Testing Script
# Tests all lock types with selected operation ratios for faster results

# Configuration
NUM_THREADS=8
DURATION=10
DB_FILE="./test_fairness_quick.db"
OUTPUT_FILE="fairness_results_quick.csv"
LOG_DIR="./test_logs_quick"

# Lock types: 0=MUTEX, 1=SPINLOCK, 2=RWLOCK, 3=ADAPTIVE_MUTEX
LOCK_TYPES=(0 1 2 3)
LOCK_NAMES=("MUTEX" "SPINLOCK" "RWLOCK" "ADAPTIVE_MUTEX")

# Test ratios - selected combinations for quick testing
TEST_COMBINATIONS=(
    "0.1 0.8 0.1"  # Low insert, high find, low update
    "0.2 0.6 0.2"  # Balanced
    "0.3 0.4 0.3"  # Even more balanced
    "0.5 0.3 0.2"  # High insert, medium find, low update
    "0.8 0.1 0.1"  # Very high insert, low find/update
    "0.4 0.4 0.2"  # Equal insert/find, low update
    "0.6 0.2 0.2"  # High insert, low find/update
    "0.2 0.7 0.1"  # Low insert, very high find, low update
)

# Create output directory for logs
mkdir -p "$LOG_DIR"

# Initialize CSV output file with headers
echo "Lock_Type,Insert_Ratio,Find_Ratio,Update_Ratio,Threads,Duration,Jains_Index,CoV,Gini_Coeff,Throughput_Spread,Min_Ops,Max_Ops,Avg_Ops,Total_Ops,Critical_Avg,High_Avg,Normal_Avg,Low_Avg,Background_Avg,Hierarchy_Switches,Assessment" > "$OUTPUT_FILE"

echo "=== QUICK LOCK FAIRNESS TESTING ==="
echo "Testing all lock types with selected operation ratios"
echo "Results will be saved to: $OUTPUT_FILE"
echo "Logs will be saved to: $LOG_DIR/"
echo ""

# Function to extract fairness metrics from output
extract_metrics() {
    local output_file="$1"
    local jains_index=$(grep "Jain's Fairness Index:" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local cov=$(grep "Coefficient of Variation:" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local gini=$(grep "Gini Coefficient:" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local throughput_spread=$(grep "Throughput Spread:" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local min_ops=$(grep "Min ops:" "$output_file" | grep -o 'Min ops: [0-9]*' | grep -o '[0-9]*')
    local max_ops=$(grep "Max ops:" "$output_file" | grep -o 'Max ops: [0-9]*' | grep -o '[0-9]*')
    local avg_ops=$(grep "Avg ops:" "$output_file" | grep -o 'Avg ops: [0-9]*\.[0-9]*' | grep -o '[0-9]*\.[0-9]*')
    local hierarchy_switches=$(grep "Total hierarchy switches:" "$output_file" | grep -o '[0-9]*')
    local assessment=$(grep "Overall Fairness Assessment:" "$output_file" | sed 's/.*Assessment: //' | sed 's/ (.*//')
    
    # Extract hierarchy level averages
    local critical_avg=$(grep "CRITICAL" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local high_avg=$(grep "HIGH" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local normal_avg=$(grep "NORMAL" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local low_avg=$(grep "LOW" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    local background_avg=$(grep "BACKGROUND" "$output_file" | grep -o '[0-9]*\.[0-9]*' | head -1)
    
    # Calculate total operations
    local total_ops=$(echo "$avg_ops * $NUM_THREADS" | bc -l 2>/dev/null || echo "0")
    
    # Return all metrics as comma-separated values
    echo "$jains_index,$cov,$gini,$throughput_spread,$min_ops,$max_ops,$avg_ops,$total_ops,$critical_avg,$high_avg,$normal_avg,$low_avg,$background_avg,$hierarchy_switches,$assessment"
}

# Function to run a single test
run_test() {
    local lock_type=$1
    local lock_name=$2
    local insert_ratio=$3
    local find_ratio=$4
    local update_ratio=$5
    
    local test_name="${lock_name}_i${insert_ratio}_f${find_ratio}_u${update_ratio}"
    local log_file="$LOG_DIR/${test_name}.log"
    
    echo "  Testing: $test_name"
    
    # Clean up previous test files
    rm -f ${DB_FILE}*
    
    # Run the test with timeout
    timeout 20s ./upscale_mutes $NUM_THREADS $DURATION $DB_FILE $insert_ratio $find_ratio $lock_type > "$log_file" 2>&1
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        # Extract metrics from log file
        local metrics=$(extract_metrics "$log_file")
        
        # Write to CSV
        echo "$lock_name,$insert_ratio,$find_ratio,$update_ratio,$NUM_THREADS,$DURATION,$metrics" >> "$OUTPUT_FILE"
        
        # Show summary
        local jains=$(echo "$metrics" | cut -d',' -f1)
        local assessment=$(echo "$metrics" | cut -d',' -f15)
        echo "    Result: Jain's Index=$jains, Assessment=$assessment"
        
    elif [ $exit_code -eq 124 ]; then
        echo "    TIMEOUT: Test exceeded 20 seconds"
        echo "$lock_name,$insert_ratio,$find_ratio,$update_ratio,$NUM_THREADS,$DURATION,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT" >> "$OUTPUT_FILE"
    else
        echo "    ERROR: Test failed with exit code $exit_code"
        echo "$lock_name,$insert_ratio,$find_ratio,$update_ratio,$NUM_THREADS,$DURATION,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR" >> "$OUTPUT_FILE"
    fi
}

# Calculate total tests
total_tests=$((${#LOCK_TYPES[@]} * ${#TEST_COMBINATIONS[@]}))
completed_tests=0

echo "Total tests to run: $total_tests"
echo ""

# Run all test combinations
for lock_idx in "${!LOCK_TYPES[@]}"; do
    lock_type=${LOCK_TYPES[$lock_idx]}
    lock_name=${LOCK_NAMES[$lock_idx]}
    
    echo "=== Testing $lock_name (Type $lock_type) ==="
    
    for combination in "${TEST_COMBINATIONS[@]}"; do
        read -r insert find update <<< "$combination"
        
        run_test "$lock_type" "$lock_name" "$insert" "$find" "$update"
        ((completed_tests++))
        
        # Show progress
        progress=$(echo "scale=1; $completed_tests * 100 / $total_tests" | bc -l)
        echo "    Progress: $completed_tests/$total_tests ($progress%)"
        echo ""
    done
done

echo "=== TESTING COMPLETE ==="
echo "Results saved to: $OUTPUT_FILE"
echo "Logs saved to: $LOG_DIR/"
echo ""

# Generate quick summary
echo "=== QUICK SUMMARY ==="
echo "Lock Type Performance (Average Jain's Fairness Index):"
echo "------------------------------------------------------"

for lock_name in "${LOCK_NAMES[@]}"; do
    avg_jains=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.4f", sum/count; else print "N/A"}')
    test_count=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | wc -l)
    
    # Get best and worst results
    best=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | sort -t',' -k7 -nr | head -1 | cut -d',' -f7)
    worst=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | sort -t',' -k7 -n | head -1 | cut -d',' -f7)
    
    echo "$lock_name: Avg=$avg_jains, Best=$best, Worst=$worst ($test_count tests)"
done

echo ""
echo "Most Fair Configuration:"
most_fair=$(grep -v "ERROR\|TIMEOUT" "$OUTPUT_FILE" | sort -t',' -k7 -nr | head -1)
if [ ! -z "$most_fair" ]; then
    lock=$(echo "$most_fair" | cut -d',' -f1)
    ratios=$(echo "$most_fair" | cut -d',' -f2-4)
    jains=$(echo "$most_fair" | cut -d',' -f7)
    assess=$(echo "$most_fair" | cut -d',' -f21)
    echo "  $lock with ratios $ratios: Jain's Index $jains ($assess)"
fi

echo ""
echo "Least Fair Configuration:"
least_fair=$(grep -v "ERROR\|TIMEOUT" "$OUTPUT_FILE" | sort -t',' -k7 -n | head -1)
if [ ! -z "$least_fair" ]; then
    lock=$(echo "$least_fair" | cut -d',' -f1)
    ratios=$(echo "$least_fair" | cut -d',' -f2-4)
    jains=$(echo "$least_fair" | cut -d',' -f7)
    assess=$(echo "$least_fair" | cut -d',' -f21)
    echo "  $lock with ratios $ratios: Jain's Index $jains ($assess)"
fi

echo ""
echo "Files created:"
echo "  $OUTPUT_FILE - CSV results"
echo "  $LOG_DIR/ - Individual test logs"
echo ""
echo "Use 'cat $OUTPUT_FILE' to see all results"
