#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <inttypes.h>
#include <assert.h>
#include <math.h>
#include <ups/upscaledb.h>
#include "hscl-archived/rdtsc.h"

#define gettid() syscall(SYS_gettid)

// Define cycles per microsecond for timing
#ifndef CYCLE_PER_US
#define CYCLE_PER_US 2400  // Adjust this for your machine
#endif

typedef unsigned long long ull;

// Test configuration
#define MAX_THREADS 16
#define MAX_RECORDS 100000
#define KEY_SIZE 16
#define DATA_SIZE 256

// Thread hierarchy levels
typedef enum {
    HIERARCHY_CRITICAL = 0,     // Highest priority - should dominate
    HIERARCHY_HIGH = 1,         // High priority
    HIERARCHY_NORMAL = 2,       // Normal priority
    HIERARCHY_LOW = 3,          // Low priority
    HIERARCHY_BACKGROUND = 4    // Lowest priority - should be starved
} thread_hierarchy_t;

// Lock types for comparison
typedef enum {
    LOCK_MUTEX = 0,           // Standard pthread_mutex (unfair)
    LOCK_SPIN = 1,            // pthread_spinlock (unfair)
    LOCK_RWLOCK = 2,          // pthread_rwlock (write lock for all operations)
    LOCK_ADAPTIVE_MUTEX = 3   // Adaptive mutex (if available)
} lock_type_t;

// Operation types
typedef enum {
    OP_INSERT = 0,
    OP_FIND = 1,
    OP_UPDATE = 2
} operation_type_t;

// Thread configuration with hierarchy
typedef struct {
    int thread_id;
    thread_hierarchy_t hierarchy_level;
    const char *hierarchy_name;
    int system_priority;        // OS scheduling priority
    int nice_value;            // Nice value for process priority
    cpu_set_t cpu_affinity;    // CPU affinity to increase contention
    
    double insert_ratio;       // Ratio of insert operations (0.0 to 1.0)
    double find_ratio;         // Ratio of find operations
    double update_ratio;       // Ratio of update operations
    int duration_seconds;
    volatile int *stop_flag;
    
    // Database handles
    ups_env_t *env;
    ups_db_t *db;
    
    // Statistics
    ull insert_count;
    ull find_count;
    ull update_count;
    ull insert_time;
    ull find_time;
    ull update_time;
    ull lock_wait_time;
    ull total_operations;
    ull lock_acquisitions;
    
    // Fairness and starvation metrics
    ull lock_contentions;
    ull max_consecutive_acquisitions;
    ull current_consecutive;
    ull starvation_periods;     // Number of times thread was starved
    ull max_starvation_time;    // Longest time without lock access
    ull last_lock_time;        // Last time this thread got the lock
    
    // Hierarchy-specific metrics
    ull dominated_lower_threads; // Times this thread beat lower hierarchy threads
    ull starved_by_higher;      // Times starved by higher hierarchy threads
    
    pthread_t thread;
} thread_config_t;

// Global variables for different lock types
static volatile int global_stop = 0;
static int next_key_id = 1;
static lock_type_t current_lock_type = LOCK_MUTEX;

// Different lock implementations
static pthread_mutex_t global_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_spinlock_t global_spinlock;
static pthread_rwlock_t global_rwlock = PTHREAD_RWLOCK_INITIALIZER;
static pthread_mutex_t global_adaptive_mutex;

// Lock operation tracking for hierarchy analysis
static volatile int last_thread_id = -1;
static volatile thread_hierarchy_t last_hierarchy = HIERARCHY_BACKGROUND;
static volatile ull consecutive_same_thread = 0;
static volatile ull consecutive_same_hierarchy = 0;
static volatile ull hierarchy_switches = 0;

// Thread hierarchy configuration
static const struct {
    const char *name;
    int system_priority;    // Real-time priority (1-99) or 0 for normal
    int nice_value;        // Nice value (-20 to 19)
    int scheduling_policy; // SCHED_FIFO, SCHED_RR, or SCHED_OTHER
    double work_intensity; // Relative amount of work between lock acquisitions
} hierarchy_config[] = {
    {"CRITICAL",    50, -20, SCHED_FIFO, 0.1},    // Highest RT priority, minimal work
    {"HIGH",        30, -10, SCHED_FIFO, 0.3},    // High RT priority, light work  
    {"NORMAL",       0,   0, SCHED_OTHER, 1.0},   // Normal priority, normal work
    {"LOW",          0,   5, SCHED_OTHER, 2.0},   // Lower nice, more work
    {"BACKGROUND",   0,  19, SCHED_OTHER, 5.0}    // Lowest nice, heavy work
};

// Generate a random key
void generate_key(char *key, int thread_id, int key_id) {
    snprintf(key, KEY_SIZE, "T%02d_K%08d", thread_id, key_id);
}

// Generate random data
void generate_data(char *data, int size) {
    for (int i = 0; i < size - 1; i++) {
        data[i] = 'A' + (rand() % 26);
    }
    data[size - 1] = '\0';
}

// Get next key ID atomically
int get_next_key_id() {
    return __sync_fetch_and_add(&next_key_id, 1);
}

// Simulate work based on hierarchy level (higher hierarchy = less work)
void simulate_work(thread_hierarchy_t hierarchy) {
    double work_factor = hierarchy_config[hierarchy].work_intensity;
    int work_cycles = (int)(1000 * work_factor); // Base work unit
    
    // Busy work to consume CPU time
    volatile int sum = 0;
    for (int i = 0; i < work_cycles; i++) {
        sum += i * i;
    }
}

// Set thread scheduling and priority based on hierarchy
int setup_thread_hierarchy(thread_config_t *config) {
    thread_hierarchy_t level = config->hierarchy_level;
    
    // Set CPU affinity to increase contention (all threads on same cores)
    CPU_ZERO(&config->cpu_affinity);
    CPU_SET(0, &config->cpu_affinity);  // Force all threads to compete on CPU 0
    CPU_SET(1, &config->cpu_affinity);  // And CPU 1
    
    if (pthread_setaffinity_np(config->thread, sizeof(cpu_set_t), &config->cpu_affinity) != 0) {
        perror("pthread_setaffinity_np");
        // Continue anyway - not critical
    }
    
    // Set scheduling policy and priority
    struct sched_param param;
    param.sched_priority = hierarchy_config[level].system_priority;
    
    int policy = hierarchy_config[level].scheduling_policy;
    if (pthread_setschedparam(config->thread, policy, &param) != 0) {
        // If we can't set RT priority, fall back to nice values
        perror("pthread_setschedparam (trying nice instead)");
        
        pid_t tid = gettid();
        int nice_val = hierarchy_config[level].nice_value;
        if (setpriority(PRIO_PROCESS, tid, nice_val) != 0) {
            perror("setpriority");
            // Don't return error - continue with default priority
        }
    }
    
    // Always set these values, even if priority setup failed
    config->system_priority = (param.sched_priority > 0) ? param.sched_priority : 0;
    config->nice_value = hierarchy_config[level].nice_value;
    config->hierarchy_name = hierarchy_config[level].name;
    
    return 0;  // Always return success - priority is just a hint
}

// Initialize the selected lock type
int init_lock(lock_type_t type) {
    current_lock_type = type;
    
    switch (type) {
        case LOCK_MUTEX:
            // Already initialized statically
            printf("Using standard pthread_mutex (UNFAIR - hierarchy should matter)\n");
            return 0;
            
        case LOCK_SPIN:
            if (pthread_spin_init(&global_spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
                perror("pthread_spin_init");
                return -1;
            }
            printf("Using pthread_spinlock (UNFAIR - hierarchy should matter)\n");
            return 0;
            
        case LOCK_RWLOCK:
            // Already initialized statically
            printf("Using pthread_rwlock (write locks) (hierarchy should matter)\n");
            return 0;
            
        case LOCK_ADAPTIVE_MUTEX:
            {
                pthread_mutexattr_t attr;
                if (pthread_mutexattr_init(&attr) != 0) {
                    perror("pthread_mutexattr_init");
                    return -1;
                }
                
                // Try to set adaptive mutex (may not be available on all systems)
                #ifdef PTHREAD_MUTEX_ADAPTIVE_NP
                if (pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ADAPTIVE_NP) != 0) {
                    printf("Adaptive mutex not supported, using normal mutex\n");
                    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);
                }
                #else
                printf("Adaptive mutex not supported, using normal mutex\n");
                pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL);
                #endif
                
                if (pthread_mutex_init(&global_adaptive_mutex, &attr) != 0) {
                    perror("pthread_mutex_init");
                    pthread_mutexattr_destroy(&attr);
                    return -1;
                }
                
                pthread_mutexattr_destroy(&attr);
                printf("Using adaptive pthread_mutex (hierarchy should matter)\n");
                return 0;
            }
            
        default:
            printf("Unknown lock type\n");
            return -1;
    }
}

// Destroy the selected lock type
void destroy_lock() {
    switch (current_lock_type) {
        case LOCK_MUTEX:
            pthread_mutex_destroy(&global_mutex);
            break;
            
        case LOCK_SPIN:
            pthread_spin_destroy(&global_spinlock);
            break;
            
        case LOCK_RWLOCK:
            pthread_rwlock_destroy(&global_rwlock);
            break;
            
        case LOCK_ADAPTIVE_MUTEX:
            pthread_mutex_destroy(&global_adaptive_mutex);
            break;
    }
}

// Acquire lock based on type and track hierarchy statistics
ull acquire_lock(thread_config_t *config) {
    ull start_time = rdtsc();
    
    switch (current_lock_type) {
        case LOCK_MUTEX:
            pthread_mutex_lock(&global_mutex);
            break;
            
        case LOCK_SPIN:
            pthread_spin_lock(&global_spinlock);
            break;
            
        case LOCK_RWLOCK:
            pthread_rwlock_wrlock(&global_rwlock);
            break;
            
        case LOCK_ADAPTIVE_MUTEX:
            pthread_mutex_lock(&global_adaptive_mutex);
            break;
    }
    
    ull end_time = rdtsc();
    ull current_time = end_time;
    
    // Track hierarchy-based statistics
    thread_hierarchy_t current_hierarchy = config->hierarchy_level;
    int current_thread = config->thread_id;
    
    // Check for starvation (if too much time passed since last lock)
    if (config->last_lock_time > 0) {
        ull time_since_last = current_time - config->last_lock_time;
        if (time_since_last > config->max_starvation_time) {
            config->max_starvation_time = time_since_last;
        }
        
        // Count as starvation if more than 100ms passed (approximate)
        if (time_since_last > (100 * CYCLE_PER_US * 1000)) {
            config->starvation_periods++;
        }
    }
    config->last_lock_time = current_time;
    
    // Track consecutive acquisitions and hierarchy dominance
    if (last_thread_id == current_thread) {
        consecutive_same_thread++;
    } else {
        consecutive_same_thread = 1;
        
        // Check hierarchy dominance patterns
        if (last_thread_id >= 0) {
            thread_hierarchy_t last_hier = last_hierarchy;
            
            // If current thread has higher hierarchy (lower enum value), it dominated
            if (current_hierarchy < last_hier) {
                config->dominated_lower_threads++;
            }
            // If current thread has lower hierarchy, it was potentially starved
            else if (current_hierarchy > last_hier) {
                config->starved_by_higher++;
            }
            
            if (last_hier != current_hierarchy) {
                hierarchy_switches++;
                consecutive_same_hierarchy = 1;
            } else {
                consecutive_same_hierarchy++;
            }
        }
        
        last_thread_id = current_thread;
        last_hierarchy = current_hierarchy;
    }
    
    return end_time - start_time;
}

// Release lock based on type
void release_lock() {
    switch (current_lock_type) {
        case LOCK_MUTEX:
            pthread_mutex_unlock(&global_mutex);
            break;
            
        case LOCK_SPIN:
            pthread_spin_unlock(&global_spinlock);
            break;
            
        case LOCK_RWLOCK:
            pthread_rwlock_unlock(&global_rwlock);
            break;
            
        case LOCK_ADAPTIVE_MUTEX:
            pthread_mutex_unlock(&global_adaptive_mutex);
            break;
    }
}

// Perform insert operation
ull perform_insert(thread_config_t *config) {
    ull start_time = rdtsc();
    
    char key[KEY_SIZE];
    char data[DATA_SIZE];
    int key_id = get_next_key_id();
    
    generate_key(key, config->thread_id, key_id);
    generate_data(data, DATA_SIZE);
    
    ups_key_t ups_key = {0};
    ups_record_t ups_record = {0};
    
    ups_key.data = key;
    ups_key.size = strlen(key) + 1;
    ups_record.data = data;
    ups_record.size = strlen(data) + 1;
    
    ups_status_t st = ups_db_insert(config->db, 0, &ups_key, &ups_record, 0);
    
    ull end_time = rdtsc();
    
    if (st != UPS_SUCCESS && st != UPS_DUPLICATE_KEY) {
        printf("Thread %d: Insert failed with status %d\n", config->thread_id, st);
    }
    
    return end_time - start_time;
}

// Perform find operation
ull perform_find(thread_config_t *config) {
    ull start_time = rdtsc();
    
    char key[KEY_SIZE];
    if (next_key_id <= 1) { 
        return 0; 
    }
    
    // Use existing key (probabilistic)
    int key_id = 1 + (rand() % (next_key_id - 1));
    int target_thread = rand() % MAX_THREADS;
    
    generate_key(key, target_thread, key_id);
    
    ups_key_t ups_key = {0};
    ups_record_t ups_record = {0};
    
    ups_key.data = key;
    ups_key.size = strlen(key) + 1;
    
    ups_status_t st = ups_db_find(config->db, 0, &ups_key, &ups_record, 0);
    
    ull end_time = rdtsc();
    
    return end_time - start_time;
}

// Perform update operation
ull perform_update(thread_config_t *config) {
    ull start_time = rdtsc();
    
    char key[KEY_SIZE];
    char new_data[DATA_SIZE];
    
    if (next_key_id <= 1) {
        return 0;
    }
    
    int key_id = 1 + (rand() % (next_key_id - 1));
    int target_thread = rand() % MAX_THREADS;
    
    generate_key(key, target_thread, key_id);
    generate_data(new_data, DATA_SIZE);
    
    ups_key_t ups_key = {0};
    ups_record_t ups_record = {0};
    
    ups_key.data = key;
    ups_key.size = strlen(key) + 1;
    ups_record.data = new_data;
    ups_record.size = strlen(new_data) + 1;
    
    ups_status_t st = ups_db_find(config->db, 0, &ups_key, &ups_record, 0);
    if (st == UPS_SUCCESS) {
        st = ups_db_insert(config->db, 0, &ups_key, &ups_record, UPS_OVERWRITE);
    }
    
    ull end_time = rdtsc();
    
    return end_time - start_time;
}

// Worker thread function
void *worker_thread(void *arg) {
    thread_config_t *config = (thread_config_t *)arg;
    
    // Setup thread hierarchy (priority, affinity, etc.)
    if (setup_thread_hierarchy(config) != 0) {
        printf("Warning: Thread %d failed to set hierarchy properly\n", config->thread_id);
    }
    
    printf("Thread %d (%s) starting...\n", config->thread_id, config->hierarchy_name);
    
    // Seed random number generator per thread
    srand(time(NULL) + config->thread_id);
    
    printf("Thread %d (%s): sys_priority=%d, nice=%d\n", 
           config->thread_id, config->hierarchy_name, 
           config->system_priority, config->nice_value);
    
    ull operation_time;
    ull lock_wait_time;
    
    while (!*config->stop_flag) {
        // Determine operation type based on configured ratios
        double op_rand = (double)rand() / RAND_MAX;
        operation_type_t op_type;
        
        if (op_rand < config->insert_ratio) {
            op_type = OP_INSERT;
        } else if (op_rand < config->insert_ratio + config->find_ratio) {
            op_type = OP_FIND;
        } else {
            op_type = OP_UPDATE;
        }
        
        // Acquire lock and measure wait time
        lock_wait_time = acquire_lock(config);
        config->lock_wait_time += lock_wait_time;
        config->lock_acquisitions++;
        
        // Track consecutive acquisitions
        if (consecutive_same_thread > config->max_consecutive_acquisitions) {
            config->max_consecutive_acquisitions = consecutive_same_thread;
        }
        
        // Perform database operation
        switch (op_type) {
            case OP_INSERT:
                operation_time = perform_insert(config);
                config->insert_time += operation_time;
                config->insert_count++;
                break;
                
            case OP_FIND:
                operation_time = perform_find(config);
                config->find_time += operation_time;
                config->find_count++;
                break;
                
            case OP_UPDATE:
                operation_time = perform_update(config);
                config->update_time += operation_time;
                config->update_count++;
                break;
        }
        
        config->total_operations++;
        
        // Release lock
        release_lock();
        
        // Do hierarchy-appropriate work between lock acquisitions
        simulate_work(config->hierarchy_level);
        
        // Occasional yield to allow scheduler to make decisions
        if (config->total_operations % 10 == 0) {
            sched_yield();
        }
    }
    
    printf("Thread %d (%s) finished: total_ops=%llu, max_consecutive=%llu, dominated=%llu, starved=%llu\n",
           config->thread_id, config->hierarchy_name, config->total_operations, 
           config->max_consecutive_acquisitions, config->dominated_lower_threads, 
           config->starved_by_higher);
    
    return NULL;
}

// Print hierarchical fairness statistics
void print_hierarchy_stats(thread_config_t *threads, int num_threads, int duration, lock_type_t lock_type) {
    const char* lock_names[] = {"MUTEX", "SPINLOCK", "RWLOCK", "ADAPTIVE_MUTEX"};
    
    printf("\n=== HIERARCHICAL LOCK FAIRNESS ANALYSIS ===\n");
    printf("Lock Type: %s\n", lock_names[lock_type]);
    printf("Expected Behavior: Higher hierarchy threads should dominate (unfair scheduling)\n\n");
    
    // Group threads by hierarchy level
    printf("Thread | Hierarchy  | Ops/sec | Lock Wait(ms) | Max Consec | Dominated | Starved | Starvation\n");
    printf("       |            |         |               |            | Lower     | By High | Periods   \n");
    printf("-------|------------|---------|---------------|------------|-----------|---------|----------\n");
    
    ull total_ops_by_hierarchy[5] = {0};
    int threads_by_hierarchy[5] = {0};
    
    for (int i = 0; i < num_threads; i++) {
        thread_config_t *t = &threads[i];
        
        double ops_per_sec = (double)t->total_operations / duration;
        double lock_wait_ms = (double)t->lock_wait_time / (CYCLE_PER_US * 1000);
        
        printf("  %2d   | %-10s | %7.1f | %11.2f | %8llu | %7llu | %7llu | %8llu\n",
               t->thread_id, t->hierarchy_name, ops_per_sec, lock_wait_ms,
               t->max_consecutive_acquisitions, t->dominated_lower_threads,
               t->starved_by_higher, t->starvation_periods);
        
        total_ops_by_hierarchy[t->hierarchy_level] += t->total_operations;
        threads_by_hierarchy[t->hierarchy_level]++;
    }
    
    printf("-------|------------|---------|---------------|------------|-----------|---------|----------\n");
    
    // Show hierarchy-level statistics with fairness indices
    printf("\nHierarchy Level Performance:\n");
    printf("Level      | Avg Ops/sec | Threads | Fairness Index | CoV    | Expected | Assessment\n");
    printf("-----------|-------------|---------|----------------|--------|----------|------------------\n");
    
    // Calculate per-level fairness indices
    double level_avg_ops[5] = {0};
    double level_fairness_index[5] = {0};
    double level_cov[5] = {0};
    
    for (int level = 0; level < 5; level++) {
        if (threads_by_hierarchy[level] > 0) {
            level_avg_ops[level] = (double)total_ops_by_hierarchy[level] / 
                                 (threads_by_hierarchy[level] * duration);
            
            // Calculate Jain's Fairness Index for threads within this hierarchy level
            double sum_ops = 0.0;
            double sum_ops_squared = 0.0;
            int count = 0;
            
            for (int i = 0; i < num_threads; i++) {
                if (threads[i].hierarchy_level == level) {
                    double ops = (double)threads[i].total_operations / duration;
                    sum_ops += ops;
                    sum_ops_squared += ops * ops;
                    count++;
                }
            }
            
            if (count > 1) {
                level_fairness_index[level] = (sum_ops * sum_ops) / (count * sum_ops_squared);
                
                // Calculate Coefficient of Variation (standard deviation / mean)
                double mean = sum_ops / count;
                double variance = 0.0;
                for (int i = 0; i < num_threads; i++) {
                    if (threads[i].hierarchy_level == level) {
                        double ops = (double)threads[i].total_operations / duration;
                        double diff = ops - mean;
                        variance += diff * diff;
                    }
                }
                variance /= count;
                double std_dev = sqrt(variance);
                level_cov[level] = (mean > 0) ? (std_dev / mean) : 0.0;
            } else {
                level_fairness_index[level] = 1.0;  // Perfect fairness for single thread
                level_cov[level] = 0.0;
            }
            
            const char *expectation;
            const char *assessment;
            
            switch (level) {
                case HIERARCHY_CRITICAL:
                    expectation = "HIGHEST";
                    assessment = (level_avg_ops[level] > 50) ? "GOOD (Dominating)" : "POOR (Not dominating)";
                    break;
                case HIERARCHY_HIGH:
                    expectation = "HIGH";
                    assessment = (level_avg_ops[level] > 30) ? "GOOD" : "POOR";
                    break;
                case HIERARCHY_NORMAL:
                    expectation = "MEDIUM";
                    assessment = (level_avg_ops[level] > 15) ? "GOOD" : "FAIR";
                    break;
                case HIERARCHY_LOW:
                    expectation = "LOW";
                    assessment = (level_avg_ops[level] < 20) ? "GOOD (Being starved)" : "POOR (Too much access)";
                    break;
                case HIERARCHY_BACKGROUND:
                    expectation = "LOWEST";
                    assessment = (level_avg_ops[level] < 10) ? "GOOD (Properly starved)" : "POOR (Getting too much)";
                    break;
                default:
                    expectation = "UNKNOWN";
                    assessment = "UNKNOWN";
            }
            
            printf("%-10s | %9.1f | %7d | %12.4f | %6.3f | %-8s | %s\n", 
                   hierarchy_config[level].name, level_avg_ops[level], threads_by_hierarchy[level],
                   level_fairness_index[level], level_cov[level], expectation, assessment);
        }
    }
    
    // Calculate comprehensive fairness metrics
    ull total_ops = 0;
    double min_ops = threads[0].total_operations;
    double max_ops = threads[0].total_operations;
    
    for (int i = 0; i < num_threads; i++) {
        total_ops += threads[i].total_operations;
        if (threads[i].total_operations < min_ops) min_ops = threads[i].total_operations;
        if (threads[i].total_operations > max_ops) max_ops = threads[i].total_operations;
    }
    
    double avg_ops = (double)total_ops / num_threads;
    
    // Calculate overall Jain's Fairness Index
    double sum_ops = 0.0;
    double sum_ops_squared = 0.0;
    for (int i = 0; i < num_threads; i++) {
        double ops = (double)threads[i].total_operations;
        sum_ops += ops;
        sum_ops_squared += ops * ops;
    }
    double overall_fairness_index = (sum_ops * sum_ops) / (num_threads * sum_ops_squared);
    
    // Calculate Coefficient of Variation
    double variance = 0.0;
    for (int i = 0; i < num_threads; i++) {
        double ops = (double)threads[i].total_operations;
        double diff = ops - (sum_ops / num_threads);
        variance += diff * diff;
    }
    variance /= num_threads;
    double std_dev = sqrt(variance);
    double cov = (avg_ops > 0) ? (std_dev / avg_ops) : 0.0;
    
    // Calculate Gini coefficient (inequality measure)
    double gini = 0.0;
    for (int i = 0; i < num_threads; i++) {
        for (int j = 0; j < num_threads; j++) {
            gini += fabs((double)threads[i].total_operations - (double)threads[j].total_operations);
        }
    }
    gini = gini / (2.0 * num_threads * sum_ops);
    
    printf("\n=== COMPREHENSIVE FAIRNESS ANALYSIS ===\n");
    printf("Overall Fairness Indices:\n");
    printf("  Jain's Fairness Index:     %.4f  (1.0 = perfect fair, 0.0 = completely unfair)\n", overall_fairness_index);
    printf("  Coefficient of Variation:  %.4f  (0.0 = equal, higher = more variable)\n", cov);
    printf("  Gini Coefficient:          %.4f  (0.0 = equal, 1.0 = maximum inequality)\n", gini);
    printf("  Throughput Spread:         %.1f%% (max-min)/avg\n", 
           avg_ops > 0 ? ((max_ops - min_ops) / avg_ops) * 100 : 0);
    
    printf("\nOperational Metrics:\n");
    printf("  Total hierarchy switches: %llu\n", hierarchy_switches);
    printf("  Min ops: %.0f, Max ops: %.0f, Avg ops: %.1f\n", min_ops, max_ops, avg_ops);
    
    // Fairness classification based on Jain's index
    printf("\nOverall Fairness Assessment: ");
    if (overall_fairness_index >= 0.95) {
        printf("EXCELLENT (Very Fair)\n");
    } else if (overall_fairness_index >= 0.80) {
        printf("GOOD (Mostly Fair)\n");
    } else if (overall_fairness_index >= 0.60) {
        printf("MODERATE (Some Unfairness)\n");
    } else if (overall_fairness_index >= 0.40) {
        printf("POOR (Significant Unfairness)\n");
    } else {
        printf("VERY POOR (Highly Unfair)\n");
    }
    
    // Check if hierarchy is working as expected
    ull critical_ops = total_ops_by_hierarchy[HIERARCHY_CRITICAL];
    ull background_ops = total_ops_by_hierarchy[HIERARCHY_BACKGROUND];
    
    if (threads_by_hierarchy[HIERARCHY_CRITICAL] > 0 && threads_by_hierarchy[HIERARCHY_BACKGROUND] > 0) {
        double critical_avg = (double)critical_ops / threads_by_hierarchy[HIERARCHY_CRITICAL];
        double background_avg = (double)background_ops / threads_by_hierarchy[HIERARCHY_BACKGROUND];
        
        printf("  Critical vs Background ratio: %.2f:1 ", 
               background_avg > 0 ? critical_avg / background_avg : critical_avg);
        
        if (critical_avg > background_avg * 2) {
            printf("(GOOD - hierarchy working)\n");
        } else if (critical_avg > background_avg) {
            printf("(FAIR - some hierarchy effect)\n");
        } else {
            printf("(POOR - hierarchy not working)\n");
        }
    }
    
    printf("\nNOTE: For mutex locks, we EXPECT unfairness where higher hierarchy threads\n");
    printf("      dominate lower hierarchy threads. This demonstrates why fair locks\n");
    printf("      might be needed in some applications.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <num_threads> <duration_seconds> <db_file> [insert_ratio] [find_ratio] [lock_type]\n", argv[0]);
        printf("  insert_ratio: 0.0-1.0 (default 0.3)\n");
        printf("  find_ratio: 0.0-1.0 (default 0.6, update_ratio = 1.0-insert-find)\n");
        printf("  lock_type: 0=MUTEX, 1=SPINLOCK, 2=RWLOCK, 3=ADAPTIVE_MUTEX (default 0)\n");
        printf("  Threads will be assigned to hierarchy levels automatically.\n");
        return 1;
    }
    
    int num_threads = atoi(argv[1]);
    int duration = atoi(argv[2]);
    const char *db_file = argv[3];
    double insert_ratio = argc > 4 ? atof(argv[4]) : 0.3;
    double find_ratio = argc > 5 ? atof(argv[5]) : 0.6;
    double update_ratio = 1.0 - insert_ratio - find_ratio;
    lock_type_t lock_type = argc > 6 ? (lock_type_t)atoi(argv[6]) : LOCK_MUTEX;
    
    if (num_threads > MAX_THREADS || num_threads < 1) {
        printf("Number of threads must be between 1 and %d\n", MAX_THREADS);
        return 1;
    }
    
    if (insert_ratio + find_ratio > 1.0) {
        printf("insert_ratio + find_ratio must be <= 1.0\n");
        return 1;
    }
    
    printf("Starting hierarchical lock fairness test with %d threads for %d seconds\n", num_threads, duration);
    printf("Operation ratios: Insert=%.2f, Find=%.2f, Update=%.2f\n", 
           insert_ratio, find_ratio, update_ratio);
    
    // Initialize the selected lock type
    if (init_lock(lock_type) != 0) {
        return 1;
    }
    
    // Create upscaleDB environment
    ups_env_t *env;
    ups_db_t *db;
    
    ups_status_t st = ups_env_create(&env, db_file, UPS_ENABLE_TRANSACTIONS, 0664, 0);
    if (st != UPS_SUCCESS) {
        printf("Failed to create environment: %s\n", ups_strerror(st));
        return 1;
    }
    
    st = ups_env_create_db(env, &db, 1, 0, 0);
    if (st != UPS_SUCCESS) {
        printf("Failed to create database: %s\n", ups_strerror(st));
        ups_env_close(env, 0);
        return 1;
    }
    
    // Configure threads with hierarchical levels
    thread_config_t threads[MAX_THREADS];
    
    for (int i = 0; i < num_threads; i++) {
        threads[i].thread_id = i;
        
        // Assign hierarchy level cyclically to ensure we have representatives at each level
        threads[i].hierarchy_level = (thread_hierarchy_t)(i % 5);
        
        threads[i].insert_ratio = insert_ratio;
        threads[i].find_ratio = find_ratio;
        threads[i].update_ratio = update_ratio;
        threads[i].duration_seconds = duration;
        threads[i].stop_flag = &global_stop;
        threads[i].env = env;
        threads[i].db = db;
        
        // Initialize statistics
        threads[i].insert_count = 0;
        threads[i].find_count = 0;
        threads[i].update_count = 0;
        threads[i].insert_time = 0;
        threads[i].find_time = 0;
        threads[i].update_time = 0;
        threads[i].lock_wait_time = 0;
        threads[i].total_operations = 0;
        threads[i].lock_acquisitions = 0;
        threads[i].lock_contentions = 0;
        threads[i].max_consecutive_acquisitions = 0;
        threads[i].current_consecutive = 0;
        threads[i].starvation_periods = 0;
        threads[i].max_starvation_time = 0;
        threads[i].last_lock_time = 0;
        threads[i].dominated_lower_threads = 0;
        threads[i].starved_by_higher = 0;
    }
    
    // Print thread hierarchy assignment
    printf("\nThread Hierarchy Assignment:\n");
    for (int i = 0; i < num_threads; i++) {
        printf("Thread %2d: %s (sys_prio=%d, nice=%d)\n", 
               i, hierarchy_config[threads[i].hierarchy_level].name,
               hierarchy_config[threads[i].hierarchy_level].system_priority,
               hierarchy_config[threads[i].hierarchy_level].nice_value);
    }
    printf("\n");
    
    // Start threads
    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i].thread, NULL, worker_thread, &threads[i]) != 0) {
            printf("Failed to create thread %d\n", i);
            global_stop = 1;
            break;
        }
        // Small delay to allow thread setup
        usleep(10000); // 10ms
    }
    
    // Run for specified duration
    printf("Running test for %d seconds...\n", duration);
    sleep(duration);
    global_stop = 1;
    printf("Stopping threads...\n");
    
    // Wait for threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i].thread, NULL);
    }
    
    // Print results
    print_hierarchy_stats(threads, num_threads, duration, lock_type);
    
    // Cleanup
    ups_db_close(db, 0);
    ups_env_close(env, 0);
    destroy_lock();
    
    printf("\nTest completed successfully!\n");
    printf("\nTo run with different lock types:\n");
    printf("  0 = MUTEX (should show hierarchy effects)\n");
    printf("  1 = SPINLOCK (should show hierarchy effects)\n");
    printf("  2 = RWLOCK (may show hierarchy effects)\n");
    printf("  3 = ADAPTIVE_MUTEX (may show hierarchy effects)\n");
    
    return 0;
}