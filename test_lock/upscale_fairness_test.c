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

// Operation types
typedef enum {
    OP_INSERT = 0,
    OP_FIND = 1,
    OP_UPDATE = 2
} operation_type_t;

// Thread configuration
typedef struct {
    int thread_id;
    int priority;
    int weight;
    int parent_node;
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
    
    pthread_t thread;
} thread_config_t;

// Global variables - SINGLE GLOBAL LOCK
static volatile int global_stop = 0;
static int next_key_id = 1;
static hfairlock_t global_fairlock;  // Single global lock
static node_t *hierarchy;

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
    
    // Set thread priority (ignore permission errors)
    pid_t tid = gettid();
    int ret = setpriority(PRIO_PROCESS, tid, config->priority);
    if (ret != 0 && errno != EPERM && errno != EACCES) {
        perror("setpriority");
        return NULL;
    }
    // Continue even if setpriority fails due to permissions
    
    // Initialize this thread for the GLOBAL fairlock
    int priority_adjusted_weight = config->weight * (20 + config->priority);
    hfairlock_thread_init(&global_fairlock, priority_adjusted_weight, config->parent_node);

    // Seed random number generator per thread
    srand(time(NULL) + config->thread_id);
    
    printf("Thread %d started: priority=%d, weight=%d, parent=%d\n", 
           config->thread_id, config->priority, config->weight, config->parent_node);
    
    ull operation_time;
    ull lock_start, lock_end;
    
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
        
        // Small delay to prevent tight spinning
        if (config->total_operations % 100 == 0) {
            usleep(1000); // 1ms delay every 100 operations
        }
    }
    
    printf("Thread %d finished: total_ops=%llu, inserts=%llu, finds=%llu, updates=%llu\n",
           config->thread_id, config->total_operations, 
           config->insert_count, config->find_count, config->update_count);
    
    return NULL;
}

// Type of hierarchy for testing
typedef enum {
    HIERARCHY_FLAT = 0,      // All threads as direct children of root
    HIERARCHY_BALANCED = 1,   // Balanced tree structure
    HIERARCHY_SKEWED = 2,     // Unbalanced tree favoring some branches
    HIERARCHY_DEEP = 3,       // Deep linear hierarchy
    HIERARCHY_GROUPED = 4,    // Threads grouped by priority classes
    HIERARCHY_CUSTOM = 5      // Custom user-defined hierarchy
} hierarchy_type_t;

// Initialize flat hierarchy (original simple version)
void init_flat_hierarchy(int num_threads) {
    int num_nodes = num_threads + 1;
    hierarchy = (node_t *)malloc(num_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    
    // Root node
    hierarchy[0].id = 0;
    hierarchy[0].parent = 0;
    hierarchy[0].weight = 0;
    hierarchy[0].cs = 0;
    hierarchy[0].banned_until = now;
    hierarchy[0].slice = 0;
    
    // All threads as direct children of root
    for (int i = 1; i < num_nodes; i++) {
        hierarchy[i].id = i;
        hierarchy[i].parent = 0;
        hierarchy[i].weight = 0;
        hierarchy[i].cs = 0;
        hierarchy[i].banned_until = now;
        hierarchy[i].slice = 0;
    }
    
    printf("Initialized FLAT hierarchy: %d threads under root\n", num_threads);
}

// Initialize balanced tree hierarchy
void init_balanced_hierarchy(int num_threads) {
    // Create a balanced binary tree structure
    int num_nodes = num_threads + 1;
    hierarchy = (node_t *)malloc(num_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    
    // Root node
    hierarchy[0].id = 0;
    hierarchy[0].parent = 0;
    hierarchy[0].weight = 0;
    hierarchy[0].cs = 0;
    hierarchy[0].banned_until = now;
    hierarchy[0].slice = 0;
    
    // Create balanced tree: each internal node has at most 2 children
    for (int i = 1; i < num_nodes; i++) {
        hierarchy[i].id = i;
        hierarchy[i].parent = (i - 1) / 2;  // Binary tree parent calculation
        hierarchy[i].weight = 0;
        hierarchy[i].cs = 0;
        hierarchy[i].banned_until = now;
        hierarchy[i].slice = 0;
    }
    
    printf("Initialized BALANCED hierarchy: binary tree with %d nodes\n", num_nodes);
}

// Initialize skewed hierarchy (favors certain branches)
void init_skewed_hierarchy(int num_threads) {
    int num_nodes = num_threads + 1;
    hierarchy = (node_t *)malloc(num_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    
    // Root node
    hierarchy[0].id = 0;
    hierarchy[0].parent = 0;
    hierarchy[0].weight = 0;
    hierarchy[0].cs = 0;
    hierarchy[0].banned_until = now;
    hierarchy[0].slice = 0;
    
    // Create skewed tree: first half gets deeper nesting, second half stays shallow
    int mid = num_threads / 2;
    
    for (int i = 1; i < num_nodes; i++) {
        hierarchy[i].id = i;
        hierarchy[i].weight = 0;
        hierarchy[i].cs = 0;
        hierarchy[i].banned_until = now;
        hierarchy[i].slice = 0;
        
        if (i <= mid) {
            // First half: create a linear chain (deep nesting)
            hierarchy[i].parent = i - 1;
        } else {
            // Second half: direct children of root (shallow)
            hierarchy[i].parent = 0;
        }
    }
    
    printf("Initialized SKEWED hierarchy: %d nodes in chain, %d direct children\n", 
           mid, num_threads - mid);
}

// Initialize deep linear hierarchy
void init_deep_hierarchy(int num_threads) {
    int num_nodes = num_threads + 1;
    hierarchy = (node_t *)malloc(num_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    
    // Root node
    hierarchy[0].id = 0;
    hierarchy[0].parent = 0;
    hierarchy[0].weight = 0;
    hierarchy[0].cs = 0;
    hierarchy[0].banned_until = now;
    hierarchy[0].slice = 0;
    
    // Create linear chain: each node is child of previous
    for (int i = 1; i < num_nodes; i++) {
        hierarchy[i].id = i;
        hierarchy[i].parent = i - 1;
        hierarchy[i].weight = 0;
        hierarchy[i].cs = 0;
        hierarchy[i].banned_until = now;
        hierarchy[i].slice = 0;
    }
    
    printf("Initialized DEEP hierarchy: linear chain of %d nodes\n", num_nodes);
}

// Initialize grouped hierarchy (by priority classes)
void init_grouped_hierarchy(int num_threads) {
    int num_groups = 4;  // Number of priority groups
    int num_nodes = num_threads + num_groups + 1;  // threads + group nodes + root
    
    hierarchy = (node_t *)malloc(num_nodes * sizeof(node_t));
    
    ull now = rdtsc();
    
    // Root node
    hierarchy[0].id = 0;
    hierarchy[0].parent = 0;
    hierarchy[0].weight = 0;
    hierarchy[0].cs = 0;
    hierarchy[0].banned_until = now;
    hierarchy[0].slice = 0;
    
    // Create group nodes (intermediate level)
    for (int g = 1; g <= num_groups; g++) {
        hierarchy[g].id = g;
        hierarchy[g].parent = 0;  // Groups are children of root
        hierarchy[g].weight = 0;
        hierarchy[g].cs = 0;
        hierarchy[g].banned_until = now;
        hierarchy[g].slice = 0;
    }
    
    // Assign thread nodes to groups
    for (int i = 0; i < num_threads; i++) {
        int node_id = num_groups + 1 + i;
        int group_id = (i % num_groups) + 1;  // Distribute threads across groups
        
        hierarchy[node_id].id = node_id;
        hierarchy[node_id].parent = group_id;
        hierarchy[node_id].weight = 1024;
        hierarchy[node_id].cs = 0;
        hierarchy[node_id].banned_until = now;
        hierarchy[node_id].slice = 0;
    }
    
    printf("Initialized GROUPED hierarchy: %d groups, %d threads total\n", 
           num_groups, num_threads);
}

void init_hierarchy(int num_threads, hierarchy_type_t type) {
    switch (type) {
        case HIERARCHY_FLAT:
            init_flat_hierarchy(num_threads);
            break;
            
        case HIERARCHY_BALANCED:
            init_balanced_hierarchy(num_threads);
            break;
            
        case HIERARCHY_SKEWED:
            init_skewed_hierarchy(num_threads);
            break;
            
        case HIERARCHY_DEEP:
            init_deep_hierarchy(num_threads);
            break;
            
        case HIERARCHY_GROUPED:
            init_grouped_hierarchy(num_threads);
            break;
            
        default:
            printf("Unknown hierarchy type, using flat hierarchy\n");
            init_flat_hierarchy(num_threads);
            break;
    }
}

// Enhanced thread configuration for different hierarchies
void configure_threads_for_hierarchy(thread_config_t *threads, int num_threads, 
                                   hierarchy_type_t type) {
    for (int i = 0; i < num_threads; i++) {
        threads[i].thread_id = i;
        
        switch (type) {
            case HIERARCHY_FLAT:
                threads[i].priority = -10 + (i % 20);
                threads[i].weight = 1024 >> (i % 4);
                threads[i].parent_node = i + 1;  // Each thread maps to its node (1-based)
                break;
                
            case HIERARCHY_BALANCED:
                threads[i].priority = -5 + (i % 10);
                threads[i].weight = 512 + (i % 3) * 256;
                threads[i].parent_node = i + 1;  // Each thread maps to its node
                break;
                
            case HIERARCHY_SKEWED:
                threads[i].priority = (i < num_threads/2) ? -10 + i : 0;
                threads[i].weight = (i < num_threads/2) ? 2048 >> i : 1024;
                threads[i].parent_node = i + 1;  // Each thread maps to its node
                break;
                
            case HIERARCHY_DEEP:
                threads[i].priority = -15 + i;  // Increasing priority down the chain
                threads[i].weight = 1024 + i * 128;
                threads[i].parent_node = i + 1;  // Each thread maps to its node
                break;
                
            case HIERARCHY_GROUPED:
                {
                    int group = i % 4;
                    threads[i].priority = -10 + group * 5;
                    threads[i].weight = 1024 >> group;
                    threads[i].parent_node = 4 + 1 + i;  // Map to thread's node ID
                }
                break;
                
            default:
                threads[i].priority = 0;
                threads[i].weight = 1024;
                threads[i].parent_node = i + 1;
                break;
        }
    }
}

// Print hierarchy structure for debugging
void print_hierarchy_structure(int num_nodes, hierarchy_type_t type) {
    printf("\n=== HIERARCHY STRUCTURE ===\n");
    printf("Type: %s\n", 
           type == HIERARCHY_FLAT ? "FLAT" :
           type == HIERARCHY_BALANCED ? "BALANCED" :
           type == HIERARCHY_SKEWED ? "SKEWED" :
           type == HIERARCHY_DEEP ? "DEEP" :
           type == HIERARCHY_GROUPED ? "GROUPED" : "CUSTOM");
    
    printf("Node | Parent | Weight | Description\n");
    printf("-----|--------|--------|------------\n");
    
    for (int i = 0; i < num_nodes; i++) {
        printf("%4d | %6d | %6d | ", 
               hierarchy[i].id, hierarchy[i].parent, hierarchy[i].weight);
        
        if (hierarchy[i].id == 0) {
            printf("Root node\n");
        } else if (type == HIERARCHY_GROUPED && hierarchy[i].id <= 4) {
            printf("Group %d\n", hierarchy[i].id);
        } else {
            printf("Thread node\n");
        }
    }
    printf("\n");
}

// Print fairness statistics
void print_fairness_stats(thread_config_t *threads, int num_threads, int duration) {
    printf("\n=== FAIRNESS ANALYSIS ===\n");
    
    ull total_ops = 0;
    ull total_lock_wait = 0;
    ull total_lock_acquisitions = 0;
    
    printf("Thread |  Ops/sec | Lock Wait(ms) | Avg Wait(us) | Slice Violations | Priority | Weight\n");
    printf("-------|----------|---------------|--------------|------------------|----------|-------\n");
    
    for (int i = 0; i < num_threads; i++) {
        thread_config_t *t = &threads[i];
        
        double ops_per_sec = (double)t->total_operations / duration;
        double lock_wait_ms = (double)t->lock_wait_time / (CYCLE_PER_US * 1000);
        double avg_wait_us = t->lock_acquisitions > 0 ? 
            (double)t->lock_wait_time / (t->lock_acquisitions * CYCLE_PER_US) : 0;
        
        printf("  %2d   | %8.1f | %11.2f | %10.2f | %14llu | %6d | %6d\n",
               t->thread_id, ops_per_sec, lock_wait_ms, avg_wait_us, 
               t->slice_violations, t->priority, t->weight);
        
        total_ops += t->total_operations;
        total_lock_wait += t->lock_wait_time;
        total_lock_acquisitions += t->lock_acquisitions;
    }
    
    printf("-------|----------|---------------|--------------|------------------|----------|-------\n");
    printf("Total: %8.1f ops/sec, %.2f ms total lock wait\n", 
           (double)total_ops / duration, 
           (double)total_lock_wait / (CYCLE_PER_US * 1000));
    
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
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <num_threads> <duration_seconds> <db_file> [insert_ratio] [find_ratio] [hierarchy_type]\n", argv[0]);
        printf("  insert_ratio: 0.0-1.0 (default 0.3)\n");
        printf("  find_ratio: 0.0-1.0 (default 0.6, update_ratio = 1.0-insert-find)\n");
        printf("  hierarchy_type: 0=FLAT, 1=BALANCED, 2=SKEWED, 3=DEEP, 4=GROUPED (default 0)\n");
        return 1;
    }
    
    int num_threads = atoi(argv[1]);
    int duration = atoi(argv[2]);
    const char *db_file = argv[3];
    double insert_ratio = argc > 4 ? atof(argv[4]) : 0.3;
    double find_ratio = argc > 5 ? atof(argv[5]) : 0.6;
    double update_ratio = 1.0 - insert_ratio - find_ratio;
    hierarchy_type_t hierarchy_type = HIERARCHY_FLAT;

    // Parse hierarchy type from command line
    if (argc > 6) {
        int type_num = atoi(argv[6]);
        if (type_num >= 0 && type_num <= 4) {
            hierarchy_type = (hierarchy_type_t)type_num;
        }
    }
    
    if (num_threads > MAX_THREADS || num_threads < 1) {
        printf("Number of threads must be between 1 and %d\n", MAX_THREADS);
        return 1;
    }
    
    if (insert_ratio + find_ratio > 1.0) {
        printf("insert_ratio + find_ratio must be <= 1.0\n");
        return 1;
    }
    
    printf("Starting fairness test with %d threads for %d seconds\n", num_threads, duration);
    printf("Operation ratios: Insert=%.2f, Find=%.2f, Update=%.2f\n", 
           insert_ratio, find_ratio, update_ratio);
    
    // Initialize hierarchy FIRST
    init_hierarchy(num_threads, hierarchy_type);
    
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
    
    // Configure threads with different priorities for fairness testing
    thread_config_t threads[MAX_THREADS];
    
    // Configure threads for the hierarchy BEFORE creating them
    configure_threads_for_hierarchy(threads, num_threads, hierarchy_type);
    
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
    }
    
    // Print hierarchy structure
    int total_nodes = (hierarchy_type == HIERARCHY_GROUPED) ? num_threads + 5 : num_threads + 1;
    print_hierarchy_structure(total_nodes, hierarchy_type);
    
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
    
    // Print results
    print_fairness_stats(threads, num_threads, duration);
    
    // Cleanup
    ups_db_close(db, 0);
    ups_env_close(env, 0);
    hfairlock_destroy(&global_fairlock);
    free(hierarchy);
    
    printf("Test completed successfully!\n");
    return 0;
}