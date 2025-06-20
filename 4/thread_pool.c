#include "thread_pool.h"

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>

enum task_status {
	TASK_STATUS_NEW,
	TASK_STATUS_IN_POOL,
	TASK_STATUS_RUNNING,
	TASK_STATUS_FINISHED,
};

struct thread_task {
	thread_task_f function;
	void *arg;
	void *result;

	enum task_status status;
	bool detached;

	pthread_mutex_t mutex;
	pthread_cond_t finished_cond;

	struct thread_pool *pool;
};

struct thread_pool {
	pthread_t *threads;
	int max_thread_count;
	int thread_count;

	struct thread_task **task_queue;
	int head;
	int tail;
	int task_queue_count;
	int running_task_count;

	pthread_mutex_t mutex;
	pthread_cond_t task_added_cond;
	pthread_cond_t all_tasks_done_cond;
	bool shutdown;
};

static void *
worker_thread(void *arg)
{
	struct thread_pool *pool = arg;

	while (true) {
		pthread_mutex_lock(&pool->mutex);

		while (pool->task_queue_count == 0 && !pool->shutdown) {
			pthread_cond_wait(&pool->task_added_cond, &pool->mutex);
		}

		if (pool->shutdown && pool->task_queue_count == 0) {
			pthread_mutex_unlock(&pool->mutex);
			break;
		}

		struct thread_task *task = pool->task_queue[pool->head];
		pool->head = (pool->head + 1) % TPOOL_MAX_TASKS;
		pool->task_queue_count--;
		pool->running_task_count++;

		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->status = TASK_STATUS_RUNNING;
		pthread_mutex_unlock(&task->mutex);

		void *result = task->function(task->arg);

		pthread_mutex_lock(&pool->mutex);
		pool->running_task_count--;
		if (pool->shutdown && pool->running_task_count == 0 &&
		    pool->task_queue_count == 0) {
			pthread_cond_broadcast(&pool->all_tasks_done_cond);
		}
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->status = TASK_STATUS_FINISHED;
		task->result = result;
		pthread_cond_broadcast(&task->finished_cond);
		bool detached = task->detached;
		pthread_mutex_unlock(&task->mutex);

		if (detached) {
			thread_task_delete(task);
		}
	}
	return NULL;
}

int
thread_pool_new(int max_thread_count, struct thread_pool **pool)
{
	if (max_thread_count <= 0 || max_thread_count > TPOOL_MAX_THREADS) {
		return TPOOL_ERR_INVALID_ARGUMENT;
	}

	struct thread_pool *new_pool = calloc(1, sizeof(*new_pool));
	if (!new_pool) {
		return TPOOL_ERR_NOT_IMPLEMENTED;
	}

	new_pool->threads = calloc(max_thread_count, sizeof(pthread_t));
	new_pool->task_queue =
		calloc(TPOOL_MAX_TASKS, sizeof(struct thread_task *));

	if (!new_pool->threads || !new_pool->task_queue) {
		free(new_pool->threads);
		free(new_pool->task_queue);
		free(new_pool);
		return TPOOL_ERR_NOT_IMPLEMENTED; 
	}
	new_pool->max_thread_count = max_thread_count;
	pthread_mutex_init(&new_pool->mutex, NULL);
	pthread_cond_init(&new_pool->task_added_cond, NULL);
	pthread_cond_init(&new_pool->all_tasks_done_cond, NULL);

	*pool = new_pool;
	return 0;
}

int
thread_pool_thread_count(const struct thread_pool *pool)
{
	return pool->thread_count;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	if (!pool)
		return TPOOL_ERR_INVALID_ARGUMENT;

	pthread_mutex_lock(&pool->mutex);

	if (pool->task_queue_count > 0 || pool->running_task_count > 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}

	pool->shutdown = true;
	pthread_cond_broadcast(&pool->task_added_cond);

	pthread_mutex_unlock(&pool->mutex);

	for (int i = 0; i < pool->thread_count; ++i) {
		pthread_join(pool->threads[i], NULL);
	}

	pthread_mutex_destroy(&pool->mutex);
	pthread_cond_destroy(&pool->task_added_cond);
	pthread_cond_destroy(&pool->all_tasks_done_cond);
	free(pool->threads);
	free(pool->task_queue);
	free(pool);

	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->mutex);

	if (pool->task_queue_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	if (task->status == TASK_STATUS_IN_POOL ||
	    task->status == TASK_STATUS_RUNNING) {
		pthread_mutex_unlock(&task->mutex);
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	task->status = TASK_STATUS_IN_POOL;
	task->pool = pool;
	pthread_mutex_unlock(&task->mutex);

	pool->task_queue[pool->tail] = task;
	pool->tail = (pool->tail + 1) % TPOOL_MAX_TASKS;
	pool->task_queue_count++;

	if (pool->thread_count < pool->max_thread_count &&
	    pool->thread_count < pool->task_queue_count) {
		pthread_create(&pool->threads[pool->thread_count], NULL,
			       worker_thread, pool);
		pool->thread_count++;
	}

	pthread_cond_signal(&pool->task_added_cond);
	pthread_mutex_unlock(&pool->mutex);

	return 0;
}

int
thread_task_new(struct thread_task **task, thread_task_f function, void *arg)
{
	struct thread_task *new_task = calloc(1, sizeof(*new_task));
	if (!new_task) {
		return TPOOL_ERR_NOT_IMPLEMENTED;
	}
	new_task->function = function;
	new_task->arg = arg;
	new_task->status = TASK_STATUS_NEW;
	pthread_mutex_init(&new_task->mutex, NULL);
	pthread_cond_init(&new_task->finished_cond, NULL);
	*task = new_task;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_t *mutex = (pthread_mutex_t *)&task->mutex;
	pthread_mutex_lock(mutex);
	bool finished = (task->status == TASK_STATUS_FINISHED);
	pthread_mutex_unlock(mutex);
	return finished;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_t *mutex = (pthread_mutex_t *)&task->mutex;
	pthread_mutex_lock(mutex);
	bool running = (task->status == TASK_STATUS_RUNNING);
	pthread_mutex_unlock(mutex);
	return running;
}

int
thread_task_join(struct thread_task *task, void **result)
{
	pthread_mutex_lock(&task->mutex);
	if (task->status == TASK_STATUS_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	while (task->status != TASK_STATUS_FINISHED) {
		pthread_cond_wait(&task->finished_cond, &task->mutex);
	}
	if (result) {
		*result = task->result;
	}
	task->status = TASK_STATUS_NEW;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout, void **result)
{
	struct timespec ts;
	struct timeval tv;
	gettimeofday(&tv, NULL);

	ts.tv_sec = tv.tv_sec + (long)timeout;
	ts.tv_nsec = tv.tv_usec * 1000 + (long)((timeout - (long)timeout) * 1e9);
	if (ts.tv_nsec >= 1000000000) {
		ts.tv_sec++;
		ts.tv_nsec -= 1000000000;
	}

	pthread_mutex_lock(&task->mutex);
	if (task->status == TASK_STATUS_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}

	int ret = 0;
	while (task->status != TASK_STATUS_FINISHED && ret != ETIMEDOUT) {
		ret = pthread_cond_timedwait(&task->finished_cond, &task->mutex,
					       &ts);
	}

	if (ret == ETIMEDOUT) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TIMEOUT;
	}

	if (result) {
		*result = task->result;
	}
	task->status = TASK_STATUS_NEW;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	if (!task) {
		return 0;
	}
	pthread_mutex_lock(&task->mutex);
	if (task->status == TASK_STATUS_IN_POOL ||
	    task->status == TASK_STATUS_RUNNING) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_destroy(&task->mutex);
	pthread_cond_destroy(&task->finished_cond);
	free(task);
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (task->status == TASK_STATUS_NEW) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (task->status == TASK_STATUS_FINISHED) {
		pthread_mutex_unlock(&task->mutex);
		thread_task_delete(task);
		return 0;
	}
	task->detached = true;
	pthread_mutex_unlock(&task->mutex);
	return 0;
}

#endif
