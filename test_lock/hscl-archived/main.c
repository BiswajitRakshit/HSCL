#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <inttypes.h>
#define gettid() syscall(SYS_gettid)
#include "rdtsc.h"
#include "lock.h"

#ifndef CYCLE_PER_US
#error Must define CYCLE_PER_US for the current machine in the Makefile or elsewhere
#endif

typedef unsigned long long ull;
typedef struct {
    volatile int *stop;
    pthread_t thread;
    int priority;
#if defined(HSCL) || defined(HFAIRLOCK)
    int parent;
    int weight;
#endif
    int id;
    double cs;
    int ncpu;
// outputs
    ull loop_in_cs;
    ull lock_acquires;
    ull lock_hold;
#if defined (HSCL) || defined (HFAIRLOCK)
    ull prev_slice;
    ull lock_opp;
#elif HNODE
    lock_context_t *context;
#endif
} task_t __attribute__ ((aligned (64)));

int ithread;
ull idle_start;
ull lock_idle;

#if defined (HSCL) || defined (HFAIRLOCK)
lock_t lock;
node_t *hierarchy;
void read_hierarchy(const char *str, int nthreads, task_t *tasks);
void print_path(int id);
#else
lock_t *lock;
#endif


void *worker(void *arg) {
    int ret;
    task_t *task = (task_t *) arg;
// #if !defined (HSCL) && !defined(HFAIRLOCK)
// #endif //HSCL

    if (task->ncpu != 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for (int i = 0; i < task->ncpu; i++) {
            if (i < 8 || i >= 24)
                CPU_SET(i, &cpuset);
            else if (i < 16)
                CPU_SET(i+8, &cpuset);
            else
                CPU_SET(i-8, &cpuset);
        }
        ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (ret != 0) {
            perror("pthread_set_affinity_np");
            exit(-1);
        }
    }

    pid_t tid = gettid();
    ret = setpriority(PRIO_PROCESS, tid, task->priority);
    if (ret != 0) {
        perror("setpriority");
        exit(-1);
    }

#if defined (HSCL)
    thread_init(&lock, task->id);
#elif defined (HFAIRLOCK)
    thread_init(&lock, task->weight, task->parent);
#else
    thread_init(lock);
#endif

    // loop
    ull now, start, then;
    ull lock_acquires = 0;
    ull lock_hold = 0;
    ull loop_in_cs = 0;
#if defined (HSCL) || defined (HFAIRLOCK)
    ull slice = 0;
#endif
    const ull delta = CYCLE_PER_US * task->cs;
    int i;
    while (!*task->stop) {

        i = __atomic_add_fetch(&ithread, 1, __ATOMIC_RELAXED);
        if (i == 1) {
            if (idle_start != 0) {
                lock_idle += rdtsc() - idle_start;
            }
        }
#if defined (HSCL) || defined (HFAIRLOCK)
        lock_acquire(&lock);
#elif HNODE
        lock_acquire(lock, task->context);
#else
        lock_acquire(lock);
#endif
        now = rdtscp();

        lock_acquires++;
        start = now;
        then = now + delta;

        do {
            loop_in_cs++;
        } while ((now = rdtscp()) < then);

        lock_hold += now - start;
        i = __atomic_sub_fetch(&ithread, 1, __ATOMIC_RELAXED);
        if (i == 0) {
            idle_start = rdtsc();
        }
#if defined (HSCL) || defined (HFAIRLOCK)
        slice = lock_release(&lock);
#elif HNODE
        lock_release(lock, task->context);
#else
        lock_release(lock);
#endif
    }
    task->lock_acquires = lock_acquires;
    task->loop_in_cs = loop_in_cs;
    task->lock_hold = lock_hold;

#if defined (HSCL) || defined (HFAIRLOCK)
    if (task->prev_slice == slice) {
        ull now = rdtsc();
        if (now > slice) {
            task->lock_opp += now - slice;
        }
    } else {
        task->prev_slice = slice;
    }
#endif

    pid_t pid = getpid();
    char path[256];
    char buffer[1024] = { 0 };
    snprintf(path, 256, "/proc/%d/task/%d/schedstat", pid, tid);
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        perror("open");
        exit(-1);
    }
    if (read(fd, buffer, 1024) <= 0) {
        perror("read");
        exit(-1);
    }
#if defined (HSCL) || defined (HFAIRLOCK)
    // printf("Lock %d\n", lock.hierarchy[0].weight);
    // printf("thread %d\n", prio_to_weight[getpriority(PRIO_PROCESS, tid)+20]);
    print_path(task->id);
#endif
    printf("id %02d "
            "loop %10llu "
            "lock_acquires %8llu "
            "lock_hold(ms) %10.3f "
            "weight %d "
#if defined (HSCL) || defined (HFAIRLOCK)
            "lock_opp %llu "
#endif
            "schedstat %s",
            task->id,
            task->loop_in_cs,
            task->lock_acquires,
            task->lock_hold / (float) (CYCLE_PER_US * 1000),
            getpriority(PRIO_PROCESS, tid),
#if defined (HSCL) || defined (HFAIRLOCK)
            task->lock_opp,
#endif
            buffer);
#if defined(HSCL) && defined(DEBUG)
    flthread_info_t *info = pthread_getspecific(lock.flthread_info_key);
    printf("  slice %llu\n"
            "  own_slice_wait %llu\n"
            "  prev_slice_wait %llu\n"
            "  runnable_wait %llu\n"
            "  next_runnable_wait %llu\n"
            "  succ_wait %llu\n"
            "  reenter %llu\n"
            "  banned(actual) %llu\n"
            "  banned %llu\n"
            "  elapse %llu\n",
            task->lock_acquires - info->stat.reenter,
            info->stat.own_slice_wait,
            info->stat.prev_slice_wait,
            info->stat.runnable_wait,
            info->stat.next_runnable_wait,
            info->stat.succ_wait,
            info->stat.reenter,
            info->stat.banned_time,
            info->banned_until-info->stat.start,
            info->start_ticks-info->stat.start);
#endif
    return 0;
}

#if defined(HSCL) || defined(HFAIRLOCK)
void read_hierarchy(const char *str, int nthreads, task_t *tasks) {
    FILE* fd = fopen(str, "r");
    if (fd == NULL) {
        perror("No file hierarchy passed\n");
        exit(-1);
    }
    int nnodes;
    fscanf(fd, "%d", &nnodes);
    hierarchy = (node_t*)malloc(nnodes * sizeof(node_t));
    ull now = rdtsc();
    for (int i = 1; i < nnodes; ++i)
    {
        hierarchy[i].id = i;
        hierarchy[i].banned_until = now;
        hierarchy[i].cs = 0;
        fscanf(fd, "%d", &hierarchy[i].parent);
    }
    for (int i = 0; i < nthreads; ++i)
    {
        fscanf(fd, "%d", &tasks[i].parent);
    }
    fclose(fd);
}

void print_path(int id) {
    flthread_info_t *info = (flthread_info_t *)pthread_getspecific(lock.flthread_info_key);
    int *path = info->path;
    printf("Thread-id %d path: ", id);
    for (int i = 0; i < MAX_DEPTH; ++i) {
        printf(" %d", path[i]);
    }
    printf("\n");

    printf("Weight of Thread-id %d path: ", id);
    for (int i = 0; i < MAX_DEPTH; ++i) {
        printf(" %d", hierarchy[path[i]].weight);
    }
    printf("\n");
    printf("Critical Section of Thread-id %d path: ", id);
    for (int i = 0; i < MAX_DEPTH; ++i) {
        printf(" %llu", hierarchy[path[i]].cs);
    }
    printf("\n");
}
#endif //HSCL


int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("usage: %s <nthreads> <duration> <<cs prio> <..n>> [NCPU]\n", argv[0]);
        printf("nthreads - no. of threads to be used for experimentation\n");
        printf("duration - the duration of the experiment\n");
        printf("cs - critical section size in us(microseconds)\n");
        printf("prio - priority of the thread\n");
        printf("NCPU - no. of CPUs to be used for the experimentation\n");
        return 1;
    }
    int nthreads = atoi(argv[1]);
    int duration = atoi(argv[2]);
    task_t *tasks = malloc(sizeof(task_t) * nthreads);
    if (argc < 3+nthreads*2) {
        printf("usage: %s <nthreads> <duration> <<cs prio> <..n>> [NCPU]\n", argv[0]);
        return 1;
    }

    int stop = 0;
#if defined (HSCL) || defined (HFAIRLOCK)
    int tot_weight = 0;
#endif
    int ncpu = argc > 3 + nthreads*2 ? atoi(argv[3+nthreads*2]) : 0;

#if defined(HNODE)
    lock_context_t context[nthreads];
#endif
    for (int i = 0; i < nthreads; i++) {
        tasks[i].stop = &stop;
        tasks[i].cs = atof(argv[3+i*2]);

        int priority = atoi(argv[4+i*2]);
        tasks[i].priority = priority;
#if defined (HSCL) || defined (HFAIRLOCK)
        int weight = prio_to_weight[priority+20];
        tasks[i].weight = weight;
        tot_weight += weight;
#endif

        tasks[i].ncpu = ncpu;
        tasks[i].id = i;

        tasks[i].loop_in_cs = 0;
        tasks[i].lock_acquires = 0;
        tasks[i].lock_hold = 0;
#if defined(HNODE)
        tasks[i].context = &context[i]; 
#endif
    }

#if defined (HSCL) 
    lock_init(&lock);
#elif defined (HFAIRLOCK)
    read_hierarchy("./hierarchy/hierarchy.txt", nthreads, tasks);
    lock_init(&lock, hierarchy);
#elif defined (HNODE)
    lock = (lock_t *)lock_init();
    lock_init_context(lock, context, nthreads);
#elif defined (MUTEX) || defined(SPIN)
    lock = (lock_t *)malloc(sizeof(lock_t));
    lock_init(lock);
#else
    lock = (lock_t *)lock_init();
#endif
    for (int i = 0; i < nthreads; i++) {
        pthread_create(&tasks[i].thread, NULL, worker, &tasks[i]);
    }
    sleep(duration);
    stop = 1;
    printf("Stop!\n");
    for (int i = 0; i < nthreads; i++) {
        pthread_join(tasks[i].thread, NULL);
    }
    printf("lock_idle: %llu\n", lock_idle);
    return 0;
}
