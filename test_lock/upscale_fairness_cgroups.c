// gcc -g -o upscale_fairness_test upscale_fairness_test.c hscl-archived/locks/hfairlock.o -lupscaledb -lpthread -lm -lubsan -DCYCLE_PER_US=2400L

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/time.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <inttypes.h>
#include <assert.h>
#include <errno.h>
#include <ups/upscaledb.h>
#include "hscl-archived/rdtsc.h"
#include "hscl-archived/locks/hfairlock.h"

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
#define MAX_CGROUPS 8

// Operation types
typedef enum {
    OP_INSERT = 0,
    OP_FIND = 1,
    OP_UPDATE = 2
} operation_type_t;

// CGroup types (similar to Linux cgroups)
typedef enum {
    CGROUP_ROOT = 0,
    CGROUP_SYSTEM = 1,     // System/kernel threads
    CGROUP_USER = 2,       // Regular user processes
    CGROUP_REALTIME = 3,   // Real-time processes
    CGROUP_BATCH = 4,      // Batch/background processes
    CGROUP_INTERACTIVE = 5, // Interactive/UI processes
    CGROUP_NETWORK = 6,    // Network-intensive processes
    CGROUP_IO = 7          // I/O intensive processes
} cgroup_type_t;

// CGroup configuration structure
typedef struct {
    int cgroup_id;
    cgroup_type_t type;
    char name[32];
    int weight;            // Relative weight for scheduling
    int cpu_shares;        // CPU shares (like Linux cgroups)
    int memory_limit_mb;   // Memory limit in MB
    int io_weight;         // I/O scheduling weight
    int rt_priority;       // Real-time priority
    int nice_value;        // Nice value for non-RT processes
    int throttle_quota;    // CPU throttling quota
    int thread_count;      // Number of threads in this cgroup
    int max_threads;       // Maximum threads allowed
} cgroup_config_t;

// Thread configuration
typedef struct {
    int thread_id;
    int priority;
    int weight;
    int parent_node;
    int cgroup_id;         // Which cgroup this thread belongs to
    cgroup_type_t cgroup_type; // Type of cgroup
    double insert_ratio;    // Ratio of insert operations (0.0 to 1.0)
    double find_ratio;      // Ratio of find operations
    double update_ratio;    // Ratio of update operations
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
    
    // Fairness metrics
    ull slice_violations;
    ull ban_time_total;
    ull reacquire_count;
    
    // CGroup-specific metrics
    ull cgroup_preemptions;
    ull cgroup_throttle_time;
    
    pthread_t thread;
} thread_config_t;

// Global variables - SINGLE GLOBAL LOCK
static volatile int global_stop = 0;
static int next_key_id = 1;
static hfairlock_t global_fairlock;  // Single global lock
static node_t *hierarchy;
static cgroup_config_t cgroups[MAX_CGROUPS];
static int num_cgroups = 0;

// Initialize CGroup configurations
void init_cgroup_configs() {
    num_cgroups = 6;  // Using 6 different cgroup types
    
    // Root cgroup
    cgroups[0] = (cgroup_config_t){
        .cgroup_id = 0,
        .type = CGROUP_ROOT,
        .weight = 1024,
        .cpu_shares = 1024,
        .memory_limit_mb = 0,  // Unlimited
        .io_weight = 1000,
        .rt_priority = 0,
        .nice_value = 0,
        .throttle_quota = 0,   // No throttling
        .thread_count = 0,
        .max_threads = MAX_THREADS
    };
    strcpy(cgroups[0].name, "root");
    
    // System cgroup - high priority system tasks
    cgroups[1] = (cgroup_config_t){
        .cgroup_id = 1,
        .type = CGROUP_SYSTEM,
        .weight = 2048,
        .cpu_shares = 2048,
        .memory_limit_mb = 512,
        .io_weight = 1000,
        .rt_priority = 10,
        .nice_value = -10,
        .throttle_quota = 0,
        .thread_count = 0,
        .max_threads = 4
    };
    strcpy(cgroups[1].name, "system");
    
    // Real-time cgroup - real-time processes
    cgroups[2] = (cgroup_config_t){
        .cgroup_id = 2,
        .type = CGROUP_REALTIME,
        .weight = 4096,
        .cpu_shares = 4096,
        .memory_limit_mb = 256,
        .io_weight = 1000,
        .rt_priority = 20,
        .nice_value = -20,
        .throttle_quota = 0,
        .thread_count = 0,
        .max_threads = 2
    };
    strcpy(cgroups[2].name, "realtime");
    
    // Interactive cgroup - user interactive processes
    cgroups[3] = (cgroup_config_t){
        .cgroup_id = 3,
        .type = CGROUP_INTERACTIVE,
        .weight = 1536,
        .cpu_shares = 1536,
        .memory_limit_mb = 1024,
        .io_weight = 800,
        .rt_priority = 0,
        .nice_value = -5,
        .throttle_quota = 0,
        .thread_count = 0,
        .max_threads = 6
    };
    strcpy(cgroups[3].name, "interactive");
    
    // User cgroup - regular user processes
    cgroups[4] = (cgroup_config_t){
        .cgroup_id = 4,
        .type = CGROUP_USER,
        .weight = 1024,
        .cpu_shares = 1024,
        .memory_limit_mb = 2048,
        .io_weight = 500,
        .rt_priority = 0,
        .nice_value = 0,
        .throttle_quota = 0,
        .thread_count = 0,
        .max_threads = 8
    };
    strcpy(cgroups[4].name, "user");
    
    // Batch cgroup - background batch processes
    cgroups[5] = (cgroup_config_t){
        .cgroup_id = 5,
        .type = CGROUP_BATCH,
        .weight = 512,
        .cpu_shares = 512,
        .memory_limit_mb = 4096,
        .io_weight = 200,
        .rt_priority = 0,
        .nice_value = 10,
        .throttle_quota = 50,  // 50% throttling
        .thread_count = 0,
        .max_threads = 4
    };
    strcpy(cgroups[5].name, "batch");
    
    printf("Initialized %d CGroups:\n", num_cgroups);
    for (int i = 0; i < num_cgroups; i++) {
        printf("  %s: weight=%d, cpu_shares=%d, max_threads=%d, nice=%d\n",
               cgroups[i].name, cgroups[i].weight, cgroups[i].cpu_shares,
               cgroups[i].max_threads, cgroups[i].nice_value);
    }
}

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
        printf("Error: No keys available for find operation\n"); 
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
    
    // Don't print error for NOT_FOUND as it's expected in concurrent environment
    if (st != UPS_SUCCESS && st != UPS_KEY_NOT_FOUND) {
        printf("Thread %d: Find failed with status %d\n", config->thread_id, st);
    }
    
    return end_time - start_time;
}

// Perform update operation
ull perform_update(thread_config_t *config) {
    ull start_time = rdtsc();
    
    char key[KEY_SIZE];
    char new_data[DATA_SIZE];
    
    if (next_key_id <= 1) {
        return 0; // No keys to update
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
    
    if (st != UPS_SUCCESS && st != UPS_KEY_NOT_FOUND) {
        printf("Thread %d: Update failed with status %d\n", config->thread_id, st);
    }
    
    return end_time - start_time;
}

// Worker thread function
void *worker_thread(void *arg) {
    thread_config_t *config = (thread_config_t *)arg;
    
    // Set thread priority based on cgroup configuration
    pid_t tid = gettid();
    cgroup_config_t *cgroup = &cgroups[config->cgroup_id];
    
    // Apply cgroup-based scheduling parameters
    int effective_priority = cgroup->nice_value;
    int ret = setpriority(PRIO_PROCESS, tid, effective_priority);
    if (ret != 0 && errno != EPERM && errno != EACCES) {
        perror("setpriority");
        return NULL;
    }
    
    // For real-time cgroups, try to set RT scheduling (if permitted)
    if (cgroup->type == CGROUP_REALTIME && cgroup->rt_priority > 0) {
        struct sched_param param;
        param.sched_priority = cgroup->rt_priority;
        if (sched_setscheduler(tid, SCHED_FIFO, &param) != 0) {
            // Fall back to normal scheduling if RT not available
            printf("Thread %d: RT scheduling not available, using normal priority\n", 
                   config->thread_id);
        }
    }
    
    // Initialize this thread for the GLOBAL fairlock with cgroup-adjusted weight
    int cgroup_adjusted_weight = cgroup->weight * (config->weight / 1024.0);
    hfairlock_thread_init(&global_fairlock, cgroup_adjusted_weight, config->parent_node);

    // Seed random number generator per thread
    srand(time(NULL) + config->thread_id);
    
    printf("Thread %d started: cgroup=%s, priority=%d, weight=%d, parent=%d\n", 
           config->thread_id, cgroup->name, effective_priority, 
           cgroup_adjusted_weight, config->parent_node);
    
    ull operation_time;
    ull lock_start, lock_end;
    ull throttle_start = 0;
    int operations_since_throttle = 0;
    
    while (!*config->stop_flag) {
        // Apply cgroup throttling if configured
        if (cgroup->throttle_quota > 0) {
            operations_since_throttle++;
            if (operations_since_throttle > (100 - cgroup->throttle_quota)) {
                if (throttle_start == 0) {
                    throttle_start = rdtsc();
                }
                // Throttle by sleeping proportionally
                usleep(1000 * cgroup->throttle_quota / 100);
                config->cgroup_throttle_time += rdtsc() - throttle_start;
                operations_since_throttle = 0;
                throttle_start = 0;
            }
        }
        
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
        
        // Acquire the GLOBAL fair lock
        lock_start = rdtsc();
        hfairlock_acquire(&global_fairlock);
        lock_end = rdtsc();
        
        config->lock_wait_time += (lock_end - lock_start);
        config->lock_acquisitions++;
        
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
        
        // Release the GLOBAL fair lock
        ull slice_end = hfairlock_release(&global_fairlock);
        
        // Check for fairness violations (simplified)
        ull current_time = rdtsc();
        if (current_time > slice_end) {
            config->slice_violations++;
        }
        
        // CGroup-specific behavior: Interactive processes get more frequent breaks
        if (cgroup->type == CGROUP_INTERACTIVE && config->total_operations % 50 == 0) {
            usleep(500); // 0.5ms break for responsiveness
        }
        // Batch processes can run longer without breaks
        else if (cgroup->type == CGROUP_BATCH && config->total_operations % 500 == 0) {
            usleep(2000); // 2ms break every 500 operations
        }
        // Regular break for other thread types
        else if (config->total_operations % 100 == 0) {
            usleep(1000); // 1ms delay every 100 operations
        }
    }
    
    printf("Thread %d finished: cgroup=%s, total_ops=%llu, inserts=%llu, finds=%llu, updates=%llu\n",
           config->thread_id, cgroup->name, config->total_operations, 
           config->insert_count, config->find_count, config->update_count);
    
    return NULL;
}

// Initialize CGroups-based hierarchy
void init_cgroups_hierarchy(int num_threads) {
    // Calculate total nodes: root + cgroup nodes + thread nodes
    int cgroup_nodes = num_cgroups - 1;  // Exclude root from count
    int total_nodes = 1 + cgroup_nodes + num_threads;
    
    hierarchy = (node_t *)malloc(total_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    int node_idx = 0;
    
    // Root node (node 0)
    hierarchy[node_idx].id = node_idx;
    hierarchy[node_idx].parent = 0;  // Root is its own parent
    hierarchy[node_idx].weight = cgroups[0].weight;
    hierarchy[node_idx].cs = 0;
    hierarchy[node_idx].banned_until = now;
    hierarchy[node_idx].slice = 0;
    node_idx++;
    
    // CGroup nodes (nodes 1 to num_cgroups-1)
    for (int i = 1; i < num_cgroups; i++) {
        hierarchy[node_idx].id = node_idx;
        hierarchy[node_idx].parent = 0;  // All cgroups are direct children of root
        hierarchy[node_idx].weight = cgroups[i].weight;
        hierarchy[node_idx].cs = 0;
        hierarchy[node_idx].banned_until = now;
        hierarchy[node_idx].slice = 0;
        node_idx++;
    }
    
    // Thread nodes - assign to cgroups based on distribution policy
    for (int i = 0; i < num_threads; i++) {
        hierarchy[node_idx].id = node_idx;
        hierarchy[node_idx].weight = 1024;  // Base weight, will be adjusted by cgroup
        hierarchy[node_idx].cs = 0;
        hierarchy[node_idx].banned_until = now;
        hierarchy[node_idx].slice = 0;
        
        // Assign thread to cgroup based on distribution strategy
        int cgroup_idx;
        if (i < 2) {
            cgroup_idx = 2;  // First 2 threads to realtime
        } else if (i < 4) {
            cgroup_idx = 1;  // Next 2 threads to system
        } else if (i < 8) {
            cgroup_idx = 3;  // Next 4 threads to interactive
        } else if (i < 12) {
            cgroup_idx = 4;  // Next 4 threads to user
        } else {
            cgroup_idx = 5;  // Remaining threads to batch
        }
        
        // Parent is the cgroup node (cgroup nodes start at index 1)
        hierarchy[node_idx].parent = cgroup_idx;
        cgroups[cgroup_idx].thread_count++;
        
        node_idx++;
    }
    
    printf("Initialized CGroups hierarchy: %d total nodes (%d cgroups, %d threads)\n", 
           total_nodes, cgroup_nodes, num_threads);
}

// Configure threads for CGroups hierarchy
void configure_threads_for_cgroups(thread_config_t *threads, int num_threads) {
    for (int i = 0; i < num_threads; i++) {
        threads[i].thread_id = i;
        
        // Determine cgroup assignment (same logic as hierarchy initialization)
        int cgroup_idx;
        if (i < 2) {
            cgroup_idx = 2;  // Realtime
            threads[i].priority = cgroups[cgroup_idx].nice_value;
            threads[i].weight = 2048;
        } else if (i < 4) {
            cgroup_idx = 1;  // System
            threads[i].priority = cgroups[cgroup_idx].nice_value;
            threads[i].weight = 1536;
        } else if (i < 8) {
            cgroup_idx = 3;  // Interactive
            threads[i].priority = cgroups[cgroup_idx].nice_value;
            threads[i].weight = 1280;
        } else if (i < 12) {
            cgroup_idx = 4;  // User
            threads[i].priority = cgroups[cgroup_idx].nice_value;
            threads[i].weight = 1024;
        } else {
            cgroup_idx = 5;  // Batch
            threads[i].priority = cgroups[cgroup_idx].nice_value;
            threads[i].weight = 512;
        }
        
        threads[i].cgroup_id = cgroup_idx;
        threads[i].cgroup_type = cgroups[cgroup_idx].type;
        
        // Parent node is the thread's node in hierarchy (after root + cgroup nodes)
        threads[i].parent_node = (num_cgroups - 1) + 1 + i;
    }
}

// Print CGroups hierarchy structure
void print_cgroups_hierarchy_structure(int num_threads) {
    printf("\n=== CGROUPS HIERARCHY STRUCTURE ===\n");
    
    int cgroup_nodes = num_cgroups - 1;
    int total_nodes = 1 + cgroup_nodes + num_threads;
    
    printf("Node | Parent | Weight | Type\n");
    printf("-----|--------|--------|----------------\n");
    
    // Print root
    printf("%4d | %6d | %6d | Root\n", 
           hierarchy[0].id, hierarchy[0].parent, hierarchy[0].weight);
    
    // Print cgroup nodes
    for (int i = 1; i <= cgroup_nodes; i++) {
        printf("%4d | %6d | %6d | CGroup: %s\n", 
               hierarchy[i].id, hierarchy[i].parent, hierarchy[i].weight,
               cgroups[i].name);
    }
    
    // Print thread nodes
    for (int i = cgroup_nodes + 1; i < total_nodes; i++) {
        int thread_idx = i - cgroup_nodes - 1;
        int cgroup_id = hierarchy[i].parent;
        printf("%4d | %6d | %6d | Thread %d -> %s\n", 
               hierarchy[i].id, hierarchy[i].parent, hierarchy[i].weight,
               thread_idx, cgroups[cgroup_id].name);
    }
    
    printf("\n=== CGROUP SUMMARY ===\n");
    for (int i = 0; i < num_cgroups; i++) {
        printf("%s: %d threads, weight=%d, cpu_shares=%d, nice=%d\n",
               cgroups[i].name, cgroups[i].thread_count, 
               cgroups[i].weight, cgroups[i].cpu_shares, cgroups[i].nice_value);
    }
    printf("\n");
}

// Print fairness statistics with CGroup information
void print_cgroups_fairness_stats(thread_config_t *threads, int num_threads, int duration) {
    printf("\n=== CGROUPS FAIRNESS ANALYSIS ===\n");
    
    ull total_ops = 0;
    ull total_lock_wait = 0;
    ull total_lock_acquisitions = 0;
    
    // Statistics by cgroup
    ull cgroup_ops[MAX_CGROUPS] = {0};
    ull cgroup_threads[MAX_CGROUPS] = {0};
    ull cgroup_throttle_time[MAX_CGROUPS] = {0};
    
    printf("Thread | CGroup      |  Ops/sec | Lock Wait(ms) | Avg Wait(us) | Throttle(ms) | Priority\n");
    printf("-------|-------------|----------|---------------|--------------|--------------|----------\n");
    
    for (int i = 0; i < num_threads; i++) {
        thread_config_t *t = &threads[i];
        cgroup_config_t *cgroup = &cgroups[t->cgroup_id];
        
        double ops_per_sec = (double)t->total_operations / duration;
        double lock_wait_ms = (double)t->lock_wait_time / (CYCLE_PER_US * 1000);
        double avg_wait_us = t->lock_acquisitions > 0 ? 
            (double)t->lock_wait_time / (t->lock_acquisitions * CYCLE_PER_US) : 0;
        double throttle_ms = (double)t->cgroup_throttle_time / (CYCLE_PER_US * 1000);
        
        printf("  %2d   | %-11s | %8.1f | %11.2f | %10.2f | %10.2f | %6d\n",
               t->thread_id, cgroup->name, ops_per_sec, lock_wait_ms, 
               avg_wait_us, throttle_ms, t->priority);
        
        total_ops += t->total_operations;
        total_lock_wait += t->lock_wait_time;
        total_lock_acquisitions += t->lock_acquisitions;
        
        cgroup_ops[t->cgroup_id] += t->total_operations;
        cgroup_threads[t->cgroup_id]++;
        cgroup_throttle_time[t->cgroup_id] += t->cgroup_throttle_time;
    }
    
    printf("-------|-------------|----------|---------------|--------------|--------------|----------\n");
    printf("Total:                %8.1f ops/sec, %.2f ms total lock wait\n", 
           (double)total_ops / duration, 
           (double)total_lock_wait / (CYCLE_PER_US * 1000));
    
    // CGroup summary
    printf("\n=== CGROUP PERFORMANCE SUMMARY ===\n");
    printf("CGroup      | Threads | Total Ops | Avg Ops/Thread | Ops/sec | Throttle(ms)\n");
    printf("------------|---------|-----------|----------------|---------|-------------\n");
    
    for (int i = 1; i < num_cgroups; i++) {  // Skip root cgroup
        if (cgroup_threads[i] > 0) {
            double avg_ops = (double)cgroup_ops[i] / cgroup_threads[i];
            double ops_per_sec = (double)cgroup_ops[i] / duration;
            double throttle_ms = (double)cgroup_throttle_time[i] / (CYCLE_PER_US * 1000);
            
            printf("%-11s | %7llu | %9llu | %14.1f | %7.1f | %9.2f\n",
                   cgroups[i].name, cgroup_threads[i], cgroup_ops[i], 
                   avg_ops, ops_per_sec, throttle_ms);
        }
    }
    
    // Calculate fairness metrics
    double min_ops = threads[0].total_operations;
    double max_ops = threads[0].total_operations;
    double avg_ops = (double)total_ops / num_threads;

    for (int i = 1; i < num_threads; i++) {
        if (threads[i].total_operations < min_ops) min_ops = threads[i].total_operations;
        if (threads[i].total_operations > max_ops) max_ops = threads[i].total_operations;
    }
    
    double fairness_index = 0.0;
    if (total_ops > 0) {
        fairness_index = (avg_ops * avg_ops * num_threads) / 
                         (total_ops * total_ops / num_threads);
    }
    
    printf("\nFairness Metrics:\n");
    printf("  Min ops: %.0f, Max ops: %.0f, Avg ops: %.1f\n", min_ops, max_ops, avg_ops);
    printf("  Fairness Index: %.4f \n", fairness_index);
    printf("  Throughput Variation: ");
    if (avg_ops > 0) {
        printf("%.1f%% (max-min)/avg\n", ((max_ops - min_ops) / avg_ops) * 100);
    } else {
        printf("N/A (insufficient operations)\n");
    }
    
    // CGroup fairness analysis
    printf("\nCGroup Fairness Analysis:\n");
    for (int i = 1; i < num_cgroups; i++) {
        if (cgroup_threads[i] > 0) {
            double expected_share = (double)cgroups[i].cpu_shares / 1024.0;
            double actual_share = (double)cgroup_ops[i] / total_ops;
            double fairness_ratio = actual_share / expected_share;
            
            printf("  %s: Expected %.1f%%, Actual %.1f%%, Ratio %.2f\n",
                   cgroups[i].name, expected_share * 100, actual_share * 100, fairness_ratio);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <num_threads> <duration_seconds> <db_file> [insert_ratio] [find_ratio]\n", argv[0]);
        printf("  insert_ratio: 0.0-1.0 (default 0.3)\n");
        printf("  find_ratio: 0.0-1.0 (default 0.6, update_ratio = 1.0-insert-find)\n");
        printf("  Note: This version uses CGroups-based hierarchy\n");
        return 1;
    }
    
    int num_threads = atoi(argv[1]);
    int duration = atoi(argv[2]);
    const char *db_file = argv[3];
    double insert_ratio = argc > 4 ? atof(argv[4]) : 0.3;
    double find_ratio = argc > 5 ? atof(argv[5]) : 0.6;
    double update_ratio = 1.0 - insert_ratio - find_ratio;
    
    if (num_threads > MAX_THREADS || num_threads < 1) {
        printf("Number of threads must be between 1 and %d\n", MAX_THREADS);
        return 1;
    }
    
    if (insert_ratio + find_ratio > 1.0) {
        printf("insert_ratio + find_ratio must be <= 1.0\n");
        return 1;
    }
    
    printf("Starting CGroups fairness test with %d threads for %d seconds\n", num_threads, duration);
    printf("Operation ratios: Insert=%.2f, Find=%.2f, Update=%.2f\n", 
           insert_ratio, find_ratio, update_ratio);
    
    // Initialize CGroup configurations FIRST
    init_cgroup_configs();
    
    // Initialize CGroups-based hierarchy
    init_cgroups_hierarchy(num_threads);
    
    // Initialize the GLOBAL fairlock with the hierarchy
    if (hfairlock_init(&global_fairlock, hierarchy) != 0) {
        printf("Failed to initialize global fairlock\n");
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
    
    // Configure threads with CGroups-based priorities
    thread_config_t threads[MAX_THREADS];
    
    // Configure threads for the CGroups hierarchy BEFORE creating them
    configure_threads_for_cgroups(threads, num_threads);
    
    for (int i = 0; i < num_threads; i++) {
        // Basic configuration
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
        threads[i].slice_violations = 0;
        threads[i].ban_time_total = 0;
        threads[i].reacquire_count = 0;
        threads[i].cgroup_preemptions = 0;
        threads[i].cgroup_throttle_time = 0;
    }
    
    // Print CGroups hierarchy structure
    print_cgroups_hierarchy_structure(num_threads);
    
    // Start threads
    for (int i = 0; i < num_threads; i++) {
        if (pthread_create(&threads[i].thread, NULL, worker_thread, &threads[i]) != 0) {
            printf("Failed to create thread %d\n", i);
            global_stop = 1;
            break;
        }
    }
    
    // Run for specified duration
    sleep(duration);
    global_stop = 1;
    printf("Stopping threads...\n");
    
    // Wait for threads to complete
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i].thread, NULL);
    }
    
    // Print results with CGroups analysis
    print_cgroups_fairness_stats(threads, num_threads, duration);
    
    // Cleanup
    ups_db_close(db, 0);
    ups_env_close(env, 0);
    hfairlock_destroy(&global_fairlock);
    free(hierarchy);
    
    printf("CGroups fairness test completed successfully!\n");
    return 0;
}