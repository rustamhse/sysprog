#include "thread_pool.h"
#include "rlist.h"
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>

enum task_status { TASK_NEW, TASK_QUEUED, TASK_RUNNING, TASK_FINISHED };

struct thread_task {
  thread_task_f function;
  void *arg;
  void *result;

  enum task_status status;
  bool is_detached;
  bool is_joined;

  struct thread_pool *pool;

  struct rlist list;
};

struct thread_pool {
  pthread_t *threads;
  int max_threads_count;
  int active_threads_count;
  int idle_threads_count;

  struct rlist task_list;
  int task_list_size;

  pthread_mutex_t mutex;
  pthread_cond_t task_cond;
  pthread_cond_t done_cond;
  bool shutdown;
};

static void *worker_thread(void *arg) {
  struct thread_pool *pool = arg;

  pthread_mutex_lock(&pool->mutex);
  while (!pool->shutdown) {

    while (rlist_empty(&pool->task_list) && !pool->shutdown) {
      pthread_cond_wait(&pool->task_cond, &pool->mutex);
    }

    if (pool->shutdown) {
      break;
    }

    struct thread_task *task =
        rlist_shift_entry(&pool->task_list, struct thread_task, list);

    pool->task_list_size--;
    pool->idle_threads_count--;
    pthread_mutex_unlock(&pool->mutex);

    task->status = TASK_RUNNING;
    task->result = task->function(task->arg);

    pthread_mutex_lock(&pool->mutex);
    task->status = TASK_FINISHED;
    pool->idle_threads_count++;

    if (task->is_detached) {
      free(task);
    } else {
      pthread_cond_broadcast(&pool->done_cond);
    }
  }
  pthread_mutex_unlock(&pool->mutex);

  return NULL;
}

int thread_pool_new(int max_thread_count, struct thread_pool **pool) {
  if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  *pool = calloc(1, sizeof(struct thread_pool));
  (*pool)->threads = calloc(max_thread_count, sizeof(pthread_t));
  (*pool)->max_threads_count = max_thread_count;
  rlist_create(&(*pool)->task_list);
  pthread_mutex_init(&(*pool)->mutex, NULL);
  pthread_cond_init(&(*pool)->task_cond, NULL);
  pthread_cond_init(&(*pool)->done_cond, NULL);

  return 0;
}

int thread_pool_thread_count(const struct thread_pool *pool) {
  if (!pool) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  return pool->active_threads_count;
}

int thread_pool_delete(struct thread_pool *pool) {
  if (!pool) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  if (!rlist_empty(&pool->task_list) ||
      pool->idle_threads_count != pool->active_threads_count) {
    return TPOOL_ERR_HAS_TASKS;
  }

  pool->shutdown = true;
  pthread_cond_broadcast(&pool->task_cond);

  for (int i = 0; i < pool->active_threads_count; ++i) {
    pthread_join(pool->threads[i], NULL);
  }

  pthread_mutex_destroy(&pool->mutex);
  pthread_cond_destroy(&pool->task_cond);
  pthread_cond_destroy(&pool->done_cond);
  free(pool->threads);
  free(pool);
  return 0;
}

int thread_pool_push_task(struct thread_pool *pool, struct thread_task *task) {
  if (!pool || !task || pool->shutdown) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  pthread_mutex_lock(&pool->mutex);
  if (pool->task_list_size >= TPOOL_MAX_TASKS) {
    pthread_mutex_unlock(&pool->mutex);
    return TPOOL_ERR_TOO_MANY_TASKS;
  }

  task->pool = pool;
  task->status = TASK_QUEUED;
  rlist_add_tail(&pool->task_list, &task->list);
  pool->task_list_size++;

  if (pool->idle_threads_count == 0 &&
      pool->active_threads_count < pool->max_threads_count) {
    if (pthread_create(&pool->threads[pool->active_threads_count++], NULL,
                       worker_thread, pool) == 0) {
      pool->idle_threads_count++;
    }
  }
  pthread_cond_signal(&pool->task_cond);
  pthread_mutex_unlock(&pool->mutex);

  return 0;
}

int thread_task_new(struct thread_task **task, thread_task_f function,
                    void *arg) {
  *task = calloc(1, sizeof(struct thread_task));
  (*task)->function = function;
  (*task)->arg = arg;
  (*task)->status = TASK_NEW;

  return 0;
}

bool thread_task_is_finished(const struct thread_task *task) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  return task->status == TASK_FINISHED;
}

bool thread_task_is_running(const struct thread_task *task) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  return task->status == TASK_RUNNING;
}

int thread_task_join(struct thread_task *task, void **result) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  if (task->status == TASK_NEW) {
    return TPOOL_ERR_TASK_NOT_PUSHED;
  }

  pthread_mutex_lock(&task->pool->mutex);
  while (task->status != TASK_FINISHED) {
    pthread_cond_wait(&task->pool->done_cond, &task->pool->mutex);
  }
  *result = task->result;
  task->is_joined = true;
  pthread_mutex_unlock(&task->pool->mutex);

  return 0;
}

#if NEED_TIMED_JOIN

int thread_task_timed_join(struct thread_task *task, double timeout,
                           void **result) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  if (task->status == TASK_NEW) {
    return TPOOL_ERR_TASK_NOT_PUSHED;
  }

  struct timespec timeout_time;
  if (clock_gettime(CLOCK_MONOTONIC, &timeout_time) != 0) {
    return -1;
  }

  timeout_time.tv_sec += (time_t)timeout;
  timeout_time.tv_nsec += (long)((timeout - (time_t)timeout) * 1e9);

  pthread_mutex_lock(&task->pool->mutex);
  int wait_result = 0;
  while (task->status != TASK_FINISHED && wait_result != ETIMEDOUT) {
    pthread_cond_timedwait(&task->pool->done_cond, &task->pool->mutex,
                           &timeout_time);

    if (wait_result != ETIMEDOUT) {
      struct timespec now;
      clock_gettime(CLOCK_MONOTONIC, &now);
      if (now.tv_sec > timeout_time.tv_sec ||
          (now.tv_sec == timeout_time.tv_sec &&
           now.tv_nsec >= timeout_time.tv_nsec)) {
        wait_result = ETIMEDOUT;
      }
    }
  }

  if (task->status == TASK_FINISHED) {
    *result = task->result;
    task->is_joined = true;

    pthread_mutex_unlock(&task->pool->mutex);
    return 0;
  }
  pthread_mutex_unlock(&task->pool->mutex);

  return TPOOL_ERR_TIMEOUT;
}

#endif

int thread_task_delete(struct thread_task *task) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  if (task->status != TASK_NEW && !task->is_joined) {
    return TPOOL_ERR_TASK_IN_POOL;
  }

  free(task);
  return 0;
}

#if NEED_DETACH

int thread_task_detach(struct thread_task *task) {
  if (!task) {
    return TPOOL_ERR_INVALID_ARGUMENT;
  }

  if (task->status == TASK_NEW) {
    return TPOOL_ERR_TASK_NOT_PUSHED;
  }

  pthread_mutex_t *mutex = &task->pool->mutex;

  pthread_mutex_lock(mutex);
  if (task->status == TASK_FINISHED) {
    free(task);
  } else {
    task->is_detached = true;
  }
  pthread_mutex_unlock(mutex);

  return 0;
}

#endif