#pragma once

#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <stdint.h>

#define BG_PROC_ARR_INIT_SIZE 10
#define BG_PROC_ARR_GROW_COEFF 2

struct pid_array {
	size_t pa_size;
	size_t pa_capacity;
	pid_t *pa_children;
};

struct parser;

static inline int
pid_array_init(struct pid_array *arr)
{
	assert(arr != NULL);
	arr->pa_size = 0;
	arr->pa_capacity = BG_PROC_ARR_INIT_SIZE;
	arr->pa_children = calloc(arr->pa_capacity, sizeof(pid_t));
	if (arr->pa_children == NULL) {
		return 1;
	}

	return 0;
}

static inline void
pid_array_free(struct pid_array *arr)
{
	assert(arr != NULL);
	free(arr->pa_children);
}

static inline int
pid_array_realloc(struct pid_array *arr)
{
	assert(arr != NULL);

	size_t new_capacity = 0;

	if (arr->pa_size * BG_PROC_ARR_GROW_COEFF < arr->pa_capacity && arr->pa_size > BG_PROC_ARR_INIT_SIZE) {
		new_capacity = arr->pa_capacity / BG_PROC_ARR_GROW_COEFF;
	}

	if (arr->pa_size == arr->pa_capacity) {
		new_capacity = arr->pa_capacity * BG_PROC_ARR_GROW_COEFF;
	}

	if (new_capacity != 0) {
		pid_t *new_children = realloc(arr->pa_children, sizeof(pid_t) * new_capacity);
		if (new_children == NULL) {
			return 1;
		}

		arr->pa_children = new_children;
		arr->pa_capacity = new_capacity;
		return 0;
	}

	return 0;
}

static inline int
pid_array_wait_nonblock(struct pid_array *arr)
{
	assert(arr != NULL);

	for (size_t i = 0; i < arr->pa_size;) {
		if (waitpid(arr->pa_children[i], NULL, WNOHANG) > 0) {
			--arr->pa_size;

			if (i < arr->pa_size) {
				memmove(arr->pa_children + i, arr->pa_children + i + 1, sizeof(pid_t) * (arr->pa_size - i));
			}
		}
		else {
			++i;
		}
	}

	return pid_array_realloc(arr);
}

static inline int
pid_array_wait_and_free(struct pid_array *arr)
{
	assert(arr != NULL);

	int last_exitcode = 0;

	for (size_t child_idx = 0; child_idx < arr->pa_size; ++child_idx) {
		int status;
		waitpid(arr->pa_children[child_idx], &status, 0);

		if (WIFEXITED(status)) {
			last_exitcode = WEXITSTATUS(status);
		}
	}

	pid_array_free(arr);

	return last_exitcode;
}

static inline int
pid_array_push(struct pid_array *arr, pid_t child)
{
	assert(arr != NULL);
	arr->pa_children[arr->pa_size++] = child;
	return pid_array_realloc(arr);
}

enum parser_error {
	PARSER_ERR_NONE,
	PARSER_ERR_PIPE_WITH_NO_LEFT_ARG,
	PARSER_ERR_PIPE_WITH_LEFT_ARG_NOT_A_COMMAND,
	PARSER_ERR_AND_WITH_NO_LEFT_ARG,
	PARSER_ERR_AND_WITH_LEFT_ARG_NOT_A_COMMAND,
	PARSER_ERR_OR_WITH_NO_LEFT_ARG,
	PARSER_ERR_OR_WITH_LEFT_ARG_NOT_A_COMMAND,
	PARSER_ERR_OUTOUT_REDIRECT_BAD_ARG,
	PARSER_ERR_TOO_LATE_ARGUMENTS,
	PARSER_ERR_ENDS_NOT_WITH_A_COMMAND,
};

struct command {
	char *exe;
	char** args;
	uint32_t arg_count;
	uint32_t arg_capacity;
};

enum expr_type {
	EXPR_TYPE_COMMAND,
	EXPR_TYPE_PIPE,
	EXPR_TYPE_AND,
	EXPR_TYPE_OR,
};

struct expr {
	enum expr_type type;
	struct command cmd;
	struct expr *next;
};

enum output_type {
	OUTPUT_TYPE_STDOUT,
	OUTPUT_TYPE_FILE_NEW,
	OUTPUT_TYPE_FILE_APPEND,
};

struct command_line {
	struct expr *head;
	struct expr *tail;
	enum output_type out_type;
	/** Valid if the out type is FILE. */
	char *out_file;
	bool is_background;
};

void
command_line_delete(struct command_line *line);

struct parser *
parser_new(void);

void
parser_feed(struct parser *p, const char *str, uint32_t len);

enum parser_error
parser_pop_next(struct parser *p, struct command_line **out);

void
parser_delete(struct parser *p);