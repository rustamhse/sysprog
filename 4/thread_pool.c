#include "thread_pool.h"
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <errno.h>

#define MAX_QUEUE_SIZE TPOOL_MAX_TASKS

struct thread_task {
    thread_task_f function;
    void *arg;
    void *result;

    bool is_running;
    bool is_finished;
    bool is_pushed;
	bool is_detached;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
};


struct thread_pool {
    int max_threads;
    int active_threads;
	int idle_threads;
	int running_tasks_count;

    pthread_t threads[TPOOL_MAX_THREADS];

    struct thread_task *task_queue[MAX_QUEUE_SIZE];
    int queue_front;
    int queue_back;
    int queue_size;

    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool is_shutdown;
};


int
thread_pool_new(int max_thread_count, struct thread_pool **pool) {
    if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_pool *p = malloc(sizeof(struct thread_pool));
    if (!p) return -1;

    p->max_threads = max_thread_count;
    p->active_threads = 0;
    p->idle_threads = 0;
    p->running_tasks_count = 0;
    p->queue_front = 0;
    p->queue_back = 0;
    p->queue_size = 0;
    p->is_shutdown = false;

    pthread_mutex_init(&p->mutex, NULL);
    pthread_cond_init(&p->cond, NULL);

    *pool = p;

	if (pthread_mutex_init(&p->mutex, NULL) != 0) {
		free(p);
		return -1;
	}
	
	if (pthread_cond_init(&p->cond, NULL) != 0) {
		pthread_mutex_destroy(&p->mutex);
		free(p);
		return -1;
	}

    return 0;
}


int 
thread_pool_thread_count(const struct thread_pool *pool) {
    return pool ? pool->active_threads : 0;
}


int 
thread_pool_delete(struct thread_pool *pool) {
    if (!pool) return -1;

    pthread_mutex_lock(&pool->mutex);

    if (pool->queue_size > 0 || pool->running_tasks_count > 0) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_HAS_TASKS;
    }

    pool->is_shutdown = true;
    pthread_cond_broadcast(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);

    for (int i = 0; i < pool->active_threads; ++i) {
        pthread_join(pool->threads[i], NULL);
    }

    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->cond);
    free(pool);
    return 0;
}



static void *worker_thread_func(void *arg) {
    struct thread_pool *pool = (struct thread_pool *)arg;

    while (true) {
        pthread_mutex_lock(&pool->mutex);
        pool->idle_threads++;

        while (pool->queue_size == 0 && !pool->is_shutdown) {
            pthread_cond_wait(&pool->cond, &pool->mutex);
        }

        pool->idle_threads--;

        if (pool->is_shutdown && pool->queue_size == 0) {
            pthread_mutex_unlock(&pool->mutex);
            break;
        }

        struct thread_task *task = pool->task_queue[pool->queue_front];
        pool->queue_front = (pool->queue_front + 1) % MAX_QUEUE_SIZE;
        pool->queue_size--;
        pool->running_tasks_count++;

        pthread_mutex_unlock(&pool->mutex);

        pthread_mutex_lock(&task->mutex);
        task->is_running = true;
        pthread_mutex_unlock(&task->mutex);

        void *res = NULL;
        if (task && task->function) {
            res = task->function(task->arg);
        }

        pthread_mutex_lock(&task->mutex);
        task->result = res;
        task->is_finished = true;
        task->is_running = false;
        task->is_pushed = false;
        pthread_cond_broadcast(&task->cond);
        pthread_mutex_unlock(&task->mutex);

        pthread_mutex_lock(&pool->mutex);
        pool->running_tasks_count--;
        pthread_mutex_unlock(&pool->mutex);

		bool need_free = false;

		pthread_mutex_lock(&task->mutex);
		if (task->is_detached) {
			task->is_pushed = false;
			need_free = true;
		}
		pthread_mutex_unlock(&task->mutex);
		if (need_free) {
			pthread_mutex_destroy(&task->mutex);
			pthread_cond_destroy(&task->cond);
			free(task);
		}
    }

    return NULL;
}




int 
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
    if (!pool || !task) return -1;

    pthread_mutex_lock(&pool->mutex);

    if (pool->queue_size >= MAX_QUEUE_SIZE) {
        pthread_mutex_unlock(&pool->mutex);
        return TPOOL_ERR_TOO_MANY_TASKS;
    }

    pthread_mutex_lock(&task->mutex);
    task->is_finished = false;
    task->is_running = false;
    pthread_mutex_unlock(&task->mutex);

    pool->task_queue[pool->queue_back] = task;
    pool->queue_back = (pool->queue_back + 1) % MAX_QUEUE_SIZE;
    pool->queue_size++;

    task->is_pushed = true;

    if (pool->active_threads < pool->max_threads && pool->idle_threads == 0) {
        if (pthread_create(&pool->threads[pool->active_threads], NULL, worker_thread_func, pool) != 0) {
            // откат очереди
            pool->queue_back = (pool->queue_back - 1 + MAX_QUEUE_SIZE) % MAX_QUEUE_SIZE;
            pool->queue_size--;
            task->is_pushed = false;
            pthread_mutex_unlock(&pool->mutex);
            return -1;
        }
        pool->active_threads++;
    }

    pthread_cond_signal(&pool->cond);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}



int 
thread_task_new(struct thread_task **task, thread_task_f function, void *arg) {
    if (!task || !function)
        return TPOOL_ERR_INVALID_ARGUMENT;

    struct thread_task *t = malloc(sizeof(struct thread_task));
    if (!t) return -1;

    t->function = function;
    t->arg = arg;
    t->result = NULL;

    t->is_running = false;
    t->is_finished = false;
    t->is_pushed = false;
	t->is_detached = false;

    pthread_mutex_init(&t->mutex, NULL);
    pthread_cond_init(&t->cond, NULL);

    *task = t;

	if (pthread_mutex_init(&t->mutex, NULL) != 0) {
		free(t);
		return -1;
	}
	if (pthread_cond_init(&t->cond, NULL) != 0) {
		pthread_mutex_destroy(&t->mutex);
		free(t);
		return -1;
	}	

    return 0;
}

bool 
thread_task_is_finished(const struct thread_task *task) {
    return task && task->is_finished;
}

bool 
thread_task_is_running(const struct thread_task *task) {
    return task && task->is_running;
}


int 
thread_task_join(struct thread_task *task, void **result) {
    pthread_mutex_lock(&task->mutex);

    if (!task->is_pushed && !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    while (!task->is_finished) {
        pthread_cond_wait(&task->cond, &task->mutex);
    }

    if (result)
        *result = task->result;

    pthread_mutex_unlock(&task->mutex);
    return 0;
}



#if NEED_TIMED_JOIN

int 
thread_task_timed_join(struct thread_task *task, double timeout, void **result) {
    if (!task) return -1;

    pthread_mutex_lock(&task->mutex);

    if (!task->is_pushed && !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->is_finished) {
        if (result)
            *result = task->result;
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    time_t sec = (time_t)timeout;
    long nsec = (long)((timeout - sec) * 1e9);

    ts.tv_sec += sec;
    ts.tv_nsec += nsec;
    if (ts.tv_nsec >= 1e9) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1e9;
    }

    int rc = 0;
    while (!task->is_finished && rc != ETIMEDOUT) {
        rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
    }

    if (task->is_finished) {
        if (result)
            *result = task->result;
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    pthread_mutex_unlock(&task->mutex);
    return TPOOL_ERR_TIMEOUT;
}


#endif

int 
thread_task_delete(struct thread_task *task) {
    if (!task) return -1;

    pthread_mutex_lock(&task->mutex);
    if (task->is_pushed && !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_IN_POOL;
    }
    pthread_mutex_unlock(&task->mutex);

    pthread_mutex_destroy(&task->mutex);
    pthread_cond_destroy(&task->cond);
    free(task);
    return 0;
}

#if NEED_DETACH

int 
thread_task_detach(struct thread_task *task) {
    if (!task) return -1;

    pthread_mutex_lock(&task->mutex);

    if (!task->is_pushed && !task->is_finished) {
        pthread_mutex_unlock(&task->mutex);
        return TPOOL_ERR_TASK_NOT_PUSHED;
    }

    if (task->is_detached) {
        pthread_mutex_unlock(&task->mutex);
        return 0;
    }

    task->is_detached = true;

    pthread_mutex_unlock(&task->mutex);
    return 0;
}



#endif
