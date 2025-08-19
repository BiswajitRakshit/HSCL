#!/bin/bash

# Comprehensive Lock Fairness Testing Script
# Tests all lock types with different operation ratios and thread configurations

# Configuration
NUM_THREADS=10
DURATION=15
DB_FILE="./test_fairness.db"
OUTPUT_FILE="fairness_results.csv"
LOG_DIR="./test_logs"

# Lock types: 0=MUTEX, 1=SPINLOCK, 2=RWLOCK, 3=ADAPTIVE_MUTEX
LOCK_TYPES=(0 1 2 3)
LOCK_NAMES=("MUTEX" "SPINLOCK" "RWLOCK" "ADAPTIVE_MUTEX")

# Create output directory for logs
mkdir -p "$LOG_DIR"

# Initialize CSV output file with headers
echo "Lock_Type,Insert_Ratio,Find_Ratio,Update_Ratio,Threads,Duration,Jains_Index,CoV,Gini_Coeff,Throughput_Spread,Min_Ops,Max_Ops,Avg_Ops,Total_Ops,Critical_Avg,High_Avg,Normal_Avg,Low_Avg,Background_Avg,Hierarchy_Switches,Assessment" > "$OUTPUT_FILE"

echo "=== COMPREHENSIVE LOCK FAIRNESS TESTING ==="
echo "Testing all lock types with varying operation ratios"
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
    timeout 30s ./upscale_mutes $NUM_THREADS $DURATION $DB_FILE $insert_ratio $find_ratio $lock_type > "$log_file" 2>&1
    
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
        echo "    TIMEOUT: Test exceeded 30 seconds"
        echo "$lock_name,$insert_ratio,$find_ratio,$update_ratio,$NUM_THREADS,$DURATION,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT,TIMEOUT" >> "$OUTPUT_FILE"
    else
        echo "    ERROR: Test failed with exit code $exit_code"
        echo "$lock_name,$insert_ratio,$find_ratio,$update_ratio,$NUM_THREADS,$DURATION,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR,ERROR" >> "$OUTPUT_FILE"
    fi
}

# Main testing loop
total_tests=0
completed_tests=0

# Calculate total number of tests
for lock_idx in "${!LOCK_TYPES[@]}"; do
    for insert in $(seq 0.1 0.1 0.8); do
        for find in $(seq 0.1 0.1 0.8); do
            update=$(echo "1.0 - $insert - $find" | bc -l)
            # Only test valid combinations where ratios sum to <= 1.0
            if (( $(echo "$update >= 0.1" | bc -l) )); then
                ((total_tests++))
            fi
        done
    done
done

echo "Total tests to run: $total_tests"
echo ""

# Run all test combinations
for lock_idx in "${!LOCK_TYPES[@]}"; do
    lock_type=${LOCK_TYPES[$lock_idx]}
    lock_name=${LOCK_NAMES[$lock_idx]}
    
    echo "=== Testing $lock_name (Type $lock_type) ==="
    
    for insert in $(seq 0.1 0.1 0.8); do
        for find in $(seq 0.1 0.1 0.8); do
            update=$(echo "1.0 - $insert - $find" | bc -l)
            
            # Only test valid combinations where update ratio >= 0.1
            if (( $(echo "$update >= 0.1" | bc -l) )); then
                run_test "$lock_type" "$lock_name" "$insert" "$find" "$update"
                ((completed_tests++))
                
                # Show progress
                progress=$(echo "scale=1; $completed_tests * 100 / $total_tests" | bc -l)
                echo "    Progress: $completed_tests/$total_tests ($progress%)"
                echo ""
            fi
        done
    done
done

echo "=== TESTING COMPLETE ==="
echo "Results saved to: $OUTPUT_FILE"
echo "Logs saved to: $LOG_DIR/"
echo ""

# Generate summary statistics
echo "=== SUMMARY STATISTICS ==="
echo "Generating summary report..."

# Create summary report
SUMMARY_FILE="fairness_summary.txt"
{
    echo "COMPREHENSIVE LOCK FAIRNESS TEST SUMMARY"
    echo "========================================"
    echo "Test Configuration:"
    echo "  Threads: $NUM_THREADS"
    echo "  Duration: $DURATION seconds"
    echo "  Total Tests: $completed_tests"
    echo ""
    
    echo "LOCK TYPE PERFORMANCE SUMMARY:"
    echo "------------------------------"
    
    for lock_name in "${LOCK_NAMES[@]}"; do
        echo ""
        echo "$lock_name Results:"
        
        # Best fairness (highest Jain's index)
        best_jains=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | sort -t',' -k7 -nr | head -1)
        if [ ! -z "$best_jains" ]; then
            best_ratio=$(echo "$best_jains" | cut -d',' -f2-4)
            best_value=$(echo "$best_jains" | cut -d',' -f7)
            echo "  Best Fairness: Jain's Index $best_value (ratios: $best_ratio)"
        fi
        
        # Worst fairness (lowest Jain's index)
        worst_jains=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | sort -t',' -k7 -n | head -1)
        if [ ! -z "$worst_jains" ]; then
            worst_ratio=$(echo "$worst_jains" | cut -d',' -f2-4)
            worst_value=$(echo "$worst_jains" | cut -d',' -f7)
            echo "  Worst Fairness: Jain's Index $worst_value (ratios: $worst_ratio)"
        fi
        
        # Average fairness
        avg_jains=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.4f", sum/count; else print "N/A"}')
        echo "  Average Fairness: Jain's Index $avg_jains"
        
        # Count of different assessments
        excellent=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -c "EXCELLENT")
        good=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -c "GOOD")
        moderate=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -c "MODERATE")
        poor=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -c "POOR")
        very_poor=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -c "VERY")
        
        echo "  Assessment Distribution:"
        echo "    EXCELLENT: $excellent, GOOD: $good, MODERATE: $moderate, POOR: $poor, VERY POOR: $very_poor"
    done
    
    echo ""
    echo "OPERATION RATIO IMPACT:"
    echo "----------------------"
    echo "Most Fair Configurations (Top 5):"
    grep -v "ERROR\|TIMEOUT" "$OUTPUT_FILE" | sort -t',' -k7 -nr | head -5 | while IFS=',' read -r lock insert find update threads duration jains cov gini spread min max avg total crit high norm low bg switches assess; do
        echo "  $lock: Insert=$insert, Find=$find, Update=$update, Jain's=$jains ($assess)"
    done
    
    echo ""
    echo "Least Fair Configurations (Bottom 5):"
    grep -v "ERROR\|TIMEOUT" "$OUTPUT_FILE" | sort -t',' -k7 -n | head -5 | while IFS=',' read -r lock insert find update threads duration jains cov gini spread min max avg total crit high norm low bg switches assess; do
        echo "  $lock: Insert=$insert, Find=$find, Update=$update, Jain's=$jains ($assess)"
    done
    
} > "$SUMMARY_FILE"

echo "Summary report saved to: $SUMMARY_FILE"
echo ""

# Show quick summary on screen
echo "QUICK RESULTS OVERVIEW:"
echo "----------------------"
for lock_name in "${LOCK_NAMES[@]}"; do
    avg_jains=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | cut -d',' -f7 | awk '{sum+=$1; count++} END {if(count>0) printf "%.4f", sum/count; else print "N/A"}')
    test_count=$(grep "^$lock_name," "$OUTPUT_FILE" | grep -v "ERROR\|TIMEOUT" | wc -l)
    echo "$lock_name: Average Jain's Index = $avg_jains ($test_count successful tests)"
done

echo ""
echo "Files created:"
echo "  $OUTPUT_FILE - Detailed CSV results"
echo "  $SUMMARY_FILE - Summary report"
echo "  $LOG_DIR/ - Individual test logs"
echo ""
echo "Use 'head -20 $OUTPUT_FILE' to see sample results"
echo "Use 'cat $SUMMARY_FILE' to see the full summary"
