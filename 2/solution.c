#include "parser.h"

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

struct exec_result {
    int need_exit;
    int return_code;
    pid_t *bg_pids;
    size_t bg_count;
};

static struct exec_result make_result(int need_exit, int return_code, pid_t *bg_pids, size_t bg_count) {
    struct exec_result res = {need_exit, return_code, bg_pids, bg_count};
    return res;
}

static int handle_cd_command(const struct expr *expr) {
    assert(expr);
    if (expr->cmd.arg_count != 1) return 1;
    return chdir(expr->cmd.args[0]);
}

static void execute_cmd(const struct expr *expr) {
    assert(expr);
    char **args = calloc(expr->cmd.arg_count + 2, sizeof(char *));
    args[0] = expr->cmd.exe;
    memcpy(args + 1, expr->cmd.args, sizeof(char *) * expr->cmd.arg_count);
    execvp(expr->cmd.exe, args);
}

static int is_logical(const struct expr *e) {
    return e->type == EXPR_TYPE_AND || e->type == EXPR_TYPE_OR;
}

static int is_last_in_pipeline(const struct expr *e) {
    return e->next == NULL || is_logical(e->next);
}

static struct exec_result execute_pipeline(struct expr *start, const char *outfile, enum output_type outtype, int wait_children) {
    struct pid_array pids;
    if (pid_array_init(&pids) != 0) return make_result(0, 1, NULL, 0);

    int pipefds[3] = {STDIN_FILENO, STDOUT_FILENO, -1};
    struct expr *e = start;

    while (e && !is_logical(e)) {
        if (e->type != EXPR_TYPE_COMMAND) { e = e->next; continue; }

        if (!is_last_in_pipeline(e)) {
            if (pipe(pipefds + 1) != 0) return make_result(0, 1, NULL, 0);
            int tmp = pipefds[1]; pipefds[1] = pipefds[2]; pipefds[2] = tmp;
        }

        if (strcmp(e->cmd.exe, "cd") == 0 && pids.pa_size == 0 && is_last_in_pipeline(e)) {
            if (handle_cd_command(e) != 0) {
                pid_array_free(&pids);
                if (pipefds[0] != STDIN_FILENO) close(pipefds[0]);
                if (pipefds[1] != STDOUT_FILENO) close(pipefds[1]);
                return make_result(0, -1, NULL, 0);
            }
        } else if (strcmp(e->cmd.exe, "exit") == 0) {
            if (e->next == NULL || is_logical(e->next)) {
                int is_one_cmd = (pids.pa_size == 0);
                pid_array_wait_and_free(&pids);
                if (pipefds[0] != STDIN_FILENO) close(pipefds[0]);
                if (pipefds[1] != STDOUT_FILENO) close(pipefds[1]);
                if (e->cmd.arg_count != 0) {
                    int code = atoi(e->cmd.args[0]);
                    return make_result(is_one_cmd, code, NULL, 0);
                }
                return make_result(is_one_cmd, 0, NULL, 0);
            }
        } else {
            pid_t pid = fork();
            if (pid == -1) {
                pid_array_wait_and_free(&pids);
                return make_result(1, 1, NULL, 0);
            }
            if (pid == 0) {
                pid_array_free(&pids);
                if (wait_children || pids.pa_size > 0) {
                    if (dup2(pipefds[0], STDIN_FILENO) != STDIN_FILENO) exit(1);
                } else close(pipefds[0]);

                int outfd = pipefds[1];
                if (is_last_in_pipeline(e)) {
                    if (outfd != STDOUT_FILENO) close(outfd);
                    if (outtype != OUTPUT_TYPE_STDOUT) {
                        outfd = open(outfile, O_CREAT | O_WRONLY | (outtype == OUTPUT_TYPE_FILE_NEW ? O_TRUNC : O_APPEND), 0666);
                        if (outfd == -1) exit(1);
                    } else outfd = STDOUT_FILENO;
                }

                if (dup2(outfd, STDOUT_FILENO) != STDOUT_FILENO) exit(1);
                if (pipefds[2] != -1) close(pipefds[2]);
                execute_cmd(e);
                exit(1);
            }
            pid_array_push(&pids, pid);
        }

        if (pipefds[0] != STDIN_FILENO) close(pipefds[0]);
        if (pipefds[1] != STDOUT_FILENO) close(pipefds[1]);
        pipefds[0] = pipefds[2];
        e = e->next;
    }

    if (pipefds[0] != STDIN_FILENO) close(pipefds[0]);

    if (wait_children) return make_result(0, pid_array_wait_and_free(&pids), NULL, 0);
    return make_result(0, 0, pids.pa_children, pids.pa_size);
}

static struct exec_result execute_command_line(const struct command_line *line) {
    struct expr *iter = line->head;
    struct expr *start = iter;
    while (iter && !is_logical(iter)) iter = iter->next;

    int last = (iter == NULL);
    struct exec_result result = execute_pipeline(start, last ? line->out_file : NULL, last ? line->out_type : OUTPUT_TYPE_STDOUT, last ? !line->is_background : 1);
    if (result.need_exit) return result;

    while (iter) {
        enum expr_type op = iter->type;
        iter = iter->next;

        if ((op == EXPR_TYPE_AND && result.return_code == 0) || (op == EXPR_TYPE_OR && result.return_code != 0)) {
            start = iter;
            while (iter && !is_logical(iter)) iter = iter->next;
            last = (iter == NULL);
            result = execute_pipeline(start, last ? line->out_file : NULL, last ? line->out_type : OUTPUT_TYPE_STDOUT, last ? !line->is_background : 1);
            if (result.need_exit) return result;
        }
    }
    return result;
}

int main(void) {
    const size_t BUF_SIZE = 1024;
    char buf[BUF_SIZE];
    ssize_t rc;
    struct parser *p = parser_new();
    int last_retcode = 0;

    struct pid_array bg_proc;
    if (pid_array_init(&bg_proc) != 0) {
        parser_delete(p);
        return 1;
    }

    while (1) {
        rc = read(STDIN_FILENO, buf, BUF_SIZE);
        if (rc == 0) break;
        if (rc < 0) {
            perror("read");
            break;
        }
        parser_feed(p, buf, rc);

        struct command_line *line = NULL;
        while (parser_pop_next(p, &line) == PARSER_ERR_NONE && line != NULL) {
            struct exec_result res = execute_command_line(line);
            last_retcode = res.return_code;
            command_line_delete(line);

            if (res.bg_pids) {
                for (size_t i = 0; i < res.bg_count; i++)
                    pid_array_push(&bg_proc, res.bg_pids[i]);
                free(res.bg_pids);
            }
            if (res.need_exit) {
                pid_array_wait_and_free(&bg_proc);
                parser_delete(p);
                return res.return_code;
            }
        }
        pid_array_wait_nonblock(&bg_proc);
    }

    pid_array_wait_and_free(&bg_proc);
    parser_delete(p);
    return last_retcode;
}

