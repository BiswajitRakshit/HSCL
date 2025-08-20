/*
 * upscaledb_hfairlock_test.c
 *
 * This sample demonstrates how to use Hierarchical Fair Locks (hfairlock)
 * for synchronization when performing database operations with upscaledb.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdint.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <inttypes.h>
#include <ups/upscaledb.h>

// Include hfairlock headers
// NOTE: Make sure to adjust these paths to match your hfairlock implementation
#include "../hscl-archived/locks/hfairlock.h"
#include "../hscl-archived/rdtsc.h"
// common.h is already included by hfairlock.h

#define DATABASE_NAME 1
#define NUM_THREADS 4
#define NUM_OPERATIONS 1000
#define CS_SIZE_US 50  // Critical section size in microseconds

#define gettid() syscall(SYS_gettid)

// hfairlock for database operations
hfairlock_t hfair_lock;
// Hierarchy for the hfairlock
node_t hierarchy[10];  // Array to hold node hierarchy

// Error handling function
void 
error_handler(const char *function, ups_status_t status) {
    printf("%s() returned error %d: %s\n", function, status, ups_strerror(status));
    exit(-1);
}

// Structure to pass data to worker threads
typedef struct {
    int thread_id;
    int priority;
    int weight;    // Weight for hfairlock
    int parent;    // Parent node for hfairlock
    ups_env_t *env;
    ups_db_t *db;
    int num_operations;
    int *stop;
    uint64_t lock_acquires;
    uint64_t lock_hold;
} thread_data_t;

// Function to insert a key-value pair into the database with hfairlock protection
void 
insert_with_lock(ups_db_t *db, int key_val, const char *value_str) {
    ups_status_t st;
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint64_t start, end;
    
    // Prepare key and record
    key.data = &key_val;
    key.size = sizeof(key_val);
    
    record.data = (void*)value_str;
    record.size = strlen(value_str) + 1;
    
    // Acquire hfairlock before database operation
    hfairlock_acquire(&hfair_lock);
    start = rdtsc();
    
    // Perform database operation (insert)
    st = ups_db_insert(db, NULL, &key, &record, 0);
    if (st != UPS_SUCCESS && st != UPS_DUPLICATE_KEY) {
        error_handler("ups_db_insert", st);
    }
    
    // Simulate some work inside critical section
    uint64_t then = start + (CYCLE_PER_US * CS_SIZE_US);
    uint64_t now;
    do {
        now = rdtsc();
    } while (now < then);
    
    // Release hfairlock
    end = rdtsc();
    hfairlock_release(&hfair_lock);
}

// Function to read a key-value pair from the database with hfairlock protection
void 
read_with_lock(ups_db_t *db, int key_val, char *buffer, size_t buffer_size) {
    ups_status_t st;
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint64_t start, end;
    
    // Prepare key
    key.data = &key_val;
    key.size = sizeof(key_val);
    
    // Acquire hfairlock before database operation
    hfairlock_acquire(&hfair_lock);
    start = rdtsc();
    
    // Perform database operation (find)
    st = ups_db_find(db, NULL, &key, &record, 0);
    if (st != UPS_SUCCESS && st != UPS_KEY_NOT_FOUND) {
        error_handler("ups_db_find", st);
    }
    
    if (st == UPS_SUCCESS && record.data && buffer) {
        strncpy(buffer, (char*)record.data, buffer_size);
        buffer[buffer_size - 1] = '\0';
    } else if (buffer) {
        buffer[0] = '\0';
    }
    
    // Simulate some work inside critical section
    uint64_t then = start + (CYCLE_PER_US * CS_SIZE_US);
    uint64_t now;
    do {
        now = rdtsc();
    } while (now < then);
    
    // Release hfairlock
    end = rdtsc();
    hfairlock_release(&hfair_lock);
}

// Worker thread function
void* 
worker_thread(void *arg) {
    thread_data_t *data = (thread_data_t*)arg;
    char value_buffer[100];
    char read_buffer[100];
    pid_t tid = gettid();
    int ret;
    
    // Set thread priority
    ret = setpriority(PRIO_PROCESS, tid, data->priority);
    if (ret != 0) {
        perror("setpriority");
        return NULL;
    }
    
    // Initialize hfairlock for this thread
    hfairlock_thread_init(&hfair_lock, data->weight, data->parent);
    
    printf("Thread %d (tid: %ld) started with priority %d\n", 
           data->thread_id, (long)tid, data->priority);
    
    uint64_t start_time, end_time;
    uint64_t total_lock_hold = 0;
    uint64_t lock_acquires = 0;
    
    // Perform database operations
    for (int i = 0; i < data->num_operations && !(*data->stop); i++) {
        int key_val = (data->thread_id * 10000) + i;
        
        // Create value string
        snprintf(value_buffer, sizeof(value_buffer), 
                 "Value from thread %d, operation %d", data->thread_id, i);
        
        // Insert with hfairlock protection
        start_time = rdtsc();
        insert_with_lock(data->db, key_val, value_buffer);
        end_time = rdtsc();
        
        total_lock_hold += (end_time - start_time);
        lock_acquires++;
        
        // Read with hfairlock protection
        start_time = rdtsc();
        read_with_lock(data->db, key_val, read_buffer, sizeof(read_buffer));
        end_time = rdtsc();
        
        total_lock_hold += (end_time - start_time);
        lock_acquires++;
        
        // Optional: Sleep between operations to reduce contention
        if (i % 10 == 0) {
            usleep(1000);  // 1ms
        }
    }
    
    data->lock_acquires = lock_acquires;
    data->lock_hold = total_lock_hold;
    
    printf("Thread %d completed: lock_acquires=%lu, lock_hold=%lu cycles (%.2f ms)\n", 
           data->thread_id, lock_acquires, total_lock_hold, 
           total_lock_hold / (float)(CYCLE_PER_US * 1000));
    
    return NULL;
}

int 
main(int argc, char **argv) {
    ups_status_t st;
    ups_env_t *env;
    ups_db_t *db;
    pthread_t threads[NUM_THREADS];
    thread_data_t thread_data[NUM_THREADS];
    int stop_flag = 0;
    int duration = 10;  // Default test duration in seconds
    
    if (argc > 1) {
        duration = atoi(argv[1]);
        if (duration <= 0) duration = 10;
    }
    
    printf("UPSCALEDB with hfairlock Test\n");
    printf("----------------------------\n");
    printf("Running test for %d seconds with %d threads\n", duration, NUM_THREADS);
    
    // Initialize hierarchy for hfairlock
    // Setup a simple hierarchy with one root node and all threads as children
    memset(hierarchy, 0, sizeof(hierarchy));
    hierarchy[0].id = 0;
    hierarchy[0].parent = -1;  // Root node
    hierarchy[0].weight = 100;
    hierarchy[0].banned_until = 0;
    
    // Initialize hfairlock
    hfairlock_init(&hfair_lock, hierarchy);
    printf("hfairlock initialized\n");
    
    // Create a new upscaledb environment
    st = ups_env_create(&env, "hfair_test.db", 0, 0664, 0);
    if (st != UPS_SUCCESS)
        error_handler("ups_env_create", st);
    
    // Create a database in the environment
    st = ups_env_create_db(env, &db, DATABASE_NAME, 0, 0);
    if (st != UPS_SUCCESS)
        error_handler("ups_env_create_db", st);
    
    printf("Database created\n");
    
    // Create worker threads
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_data[i].thread_id = i;
        thread_data[i].priority = i % 20 - 10;  // Priorities from -10 to +9
        thread_data[i].weight = prio_to_weight[thread_data[i].priority + 20];  // Convert priority to weight
        thread_data[i].parent = 0;  // All threads are children of the root node
        thread_data[i].env = env;
        thread_data[i].db = db;
        thread_data[i].num_operations = NUM_OPERATIONS;
        thread_data[i].stop = &stop_flag;
        thread_data[i].lock_acquires = 0;
        thread_data[i].lock_hold = 0;
        
        pthread_create(&threads[i], NULL, worker_thread, &thread_data[i]);
    }
    
    // Run the test for specified duration
    sleep(duration);
    stop_flag = 1;
    printf("Test completed. Waiting for threads to finish...\n");
    
    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    // Print summary
    printf("\nTest Summary:\n");
    uint64_t total_acquires = 0;
    uint64_t total_hold = 0;
    
    for (int i = 0; i < NUM_THREADS; i++) {
        printf("Thread %d: acquires=%lu, hold=%.2f ms\n", 
               i, thread_data[i].lock_acquires, 
               thread_data[i].lock_hold / (float)(CYCLE_PER_US * 1000));
        
        total_acquires += thread_data[i].lock_acquires;
        total_hold += thread_data[i].lock_hold;
    }
    
    printf("\nTotal: acquires=%lu, total hold=%.2f ms\n", 
           total_acquires, total_hold / (float)(CYCLE_PER_US * 1000));
    
    // Cleanup
    st = ups_db_close(db, 0);
    if (st != UPS_SUCCESS)
        error_handler("ups_db_close", st);
    
    st = ups_env_close(env, 0);
    if (st != UPS_SUCCESS)
        error_handler("ups_env_close", st);
    
    printf("Database closed. Test complete.\n");
    
    return 0;
}

