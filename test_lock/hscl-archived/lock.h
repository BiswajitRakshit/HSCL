#ifndef __LOCK_H__
#define __LOCK_H__

// #ifndef HSCL
// void create_context(lock_t *lock, lock_context_t *context, int nthreads);
// #endif

#ifdef MUTEX
#include <pthread.h>
#include "rdtsc.h"
typedef pthread_mutex_t lock_t;
#define lock_init(plock) pthread_mutex_init(plock, NULL)
#define lock_acquire(plock) pthread_mutex_lock(plock)
#define lock_release(plock) pthread_mutex_unlock(plock)
#define thread_init(plock)
#define lock_destroy(plock)

#elif SPIN
#include <pthread.h>
typedef pthread_spinlock_t lock_t;
#define lock_init(plock) pthread_spin_init(plock, PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock) pthread_spin_lock(plock)
#define lock_release(plock) pthread_spin_unlock(plock)
#define thread_init(plock)
#define lock_destroy(plock)

#elif CPTKT
#include "cptltkt.h"
typedef cpt_mutex_t lock_t;
typedef cpt_context_t lock_context_t;
#define lock_init() cpt_mutex_create(PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock) cpt_mutex_lock(plock, NULL)
#define lock_release(plock) cpt_mutex_unlock(plock, NULL)
#define thread_init(plock) cpt_thread_start()
#define lock_init_context(plock, pnode, nthread) cpt_init_context(plock, pnode, nthread)

#elif CTTKT
#include "ctkttkt.h"
typedef ctkt_mutex_t lock_t;
typedef ctkt_context_t lock_context_t;
#define lock_init() ctkt_mutex_create(PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock) ctkt_mutex_lock(plock, NULL)
#define lock_release(plock) ctkt_mutex_unlock(plock, NULL)
#define thread_init(plock) ctkt_thread_start()
#define lock_init_context ctkt_init_context

#elif HMCS
#include "hmcs.h"
typedef hmcs_mutex_t lock_t;
typedef hmcs_context_t lock_context_t;
#define lock_init() hmcs_mutex_create(PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock, pcontext) hmcs_mutex_lock(plock, pcontext)
#define lock_release(plock, pcontext) hmcs_mutex_unlock(plock, pcontext)
#define thread_init(plock) hmcs_thread_start()
#define lock_init_context hmcs_init_context

#elif HTEPFL
#include "htlockepfl.h"
typedef htlockepfl_mutex_t lock_t;
typedef htlockepfl_context_t lock_context_t;
#define lock_init() htlockepfl_mutex_create(PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock, pcontext) htlockepfl_mutex_lock(plock, pcontext)
#define lock_release(plock, pcontext) htlockepfl_mutex_unlock(plock, pcontext)
#define thread_init(plock) htlockepfl_thread_start()
#define lock_init_context htlockepfl_init_context

#elif HYMCS
#include "hyshmcs.h"
typedef hyshmcs_mutex_t lock_t;
typedef hyshmcs_context_t lock_context_t;
#define lock_init() hyshmcs_mutex_create(PTHREAD_PROCESS_PRIVATE)
#define lock_acquire(plock, pcontext) hyshmcs_mutex_lock(plock, pcontext)
#define lock_release(plock, pcontext) hyshmcs_mutex_unlock(plock, pcontext)
#define thread_init(plock) hyshmcs_thread_start()
#define lock_init_context hyshmcs_init_context

#elif HNODE
// Generic hierarchical node-based lock - define context type
typedef void lock_context_t;

#elif HFAIRLOCK
#include "hfairlock.h"
typedef hfairlock_t lock_t;
typedef struct node node_t;
extern const int prio_to_weight[40];  // Forward declaration
#define lock_init(plock, hr) hfairlock_init(plock, hr)
#define lock_acquire(plock) hfairlock_acquire(plock)
#define lock_release(plock) hfairlock_release(plock)
#define thread_init(plock, wt, parent) hfairlock_thread_init(plock, wt, parent) 

#elif HSCL
// Hierarchical SCL lock
typedef struct hscl_lock hscl_lock_t;
typedef hscl_lock_t lock_t;
typedef struct node node_t;
typedef struct flthread_info flthread_info_t;
extern const int prio_to_weight[40];  // Forward declaration
#define MAX_DEPTH 8  // Define if not already defined
#define lock_init(plock) hscl_init(plock)
#define lock_acquire(plock) hscl_acquire(plock)
#define lock_release(plock) hscl_release(plock)
#define thread_init(plock, id) hscl_thread_init(plock, id)

#endif // type of Mutex imported

#endif // __LOCK_H__