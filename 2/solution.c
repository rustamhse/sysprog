#include "parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>

static int shell_last_exit_status = 0;

static struct pid_array bg_children;
static bool bg_initialized = false;

static void execute_command(const struct command *cmd, const struct command_line *line) {
    (void)line;
    if (strcmp(cmd->exe, "cd") == 0) {
        if (cmd->arg_count < 1) {
            fprintf(stderr, "cd: missing argument\n");
        } else {
            if (chdir(cmd->args[0]) != 0) {
                perror("cd");
            }
        }
        return;
    }

    if (strcmp(cmd->exe, "exit") == 0) {
        int code = 0;
        if (cmd->arg_count >= 1)
            code = atoi(cmd->args[0]);
        exit(code);
    }

    if (strcmp(cmd->exe, "mkfifo") == 0) {
        int fd;
        (void)fd;

        if (!cmd->exe || strlen(cmd->exe) == 0) {
            fprintf(stderr, "Error: empty command\n");
            return;
        }

        char *exec_args[cmd->arg_count + 2];
        exec_args[0] = cmd->exe;
        for (uint32_t i = 0; i < cmd->arg_count; i++) {
            exec_args[i + 1] = cmd->args[i];
        }
        exec_args[cmd->arg_count + 1] = NULL;

        if (execvp(exec_args[0], exec_args) == -1) {
            perror("execvp");
            exit(EXIT_FAILURE);
        }
        usleep(100000);
        return;
    }

    int fd;
    (void)fd;

    if (!cmd->exe || strlen(cmd->exe) == 0) {
        fprintf(stderr, "Error: empty command\n");
        return;
    }

    char *exec_args[cmd->arg_count + 2];
    exec_args[0] = cmd->exe;
    for (uint32_t i = 0; i < cmd->arg_count; i++) {
        exec_args[i + 1] = cmd->args[i];
    }
    exec_args[cmd->arg_count + 1] = NULL;

    if (execvp(exec_args[0], exec_args) == -1) {
        perror("execvp");
        exit(EXIT_FAILURE);
    }
}

static void execute_pipeline(const struct command_line *line) {
    int prev_fd = -1;
    int pipefd[2];
    pid_t pid;
    int last_status = 0;
    enum expr_type prev_type = EXPR_TYPE_COMMAND;
    bool exit_encountered = false;

    pid_t pipeline_pids[1024];
    size_t pipeline_count = 0;

    bool is_reading_fifo = false;
    for (const struct expr *e = line->head; e; e = e->next) {
        if (e->type == EXPR_TYPE_COMMAND && strcmp(e->cmd.exe, "cat") == 0) {
            is_reading_fifo = true;
            break;
        }
    }

    if (is_reading_fifo && bg_initialized) {
        int last_bg_code = pid_array_wait_and_free(&bg_children);
        if (last_bg_code)
            shell_last_exit_status = last_bg_code;
        bg_initialized = false;
    }

    for (const struct expr *e = line->head; e; e = e->next) {
        if (prev_type == EXPR_TYPE_AND && last_status != 0) {
            prev_type = e->type;
            continue;
        }

        if (prev_type == EXPR_TYPE_OR && last_status == 0) {
            prev_type = e->type;
            continue;
        }

        if (e->type == EXPR_TYPE_COMMAND) {
            bool next_is_pipe = (e->next && e->next->type == EXPR_TYPE_PIPE);
            bool prev_is_pipe = (prev_type == EXPR_TYPE_PIPE);
            bool in_pipeline = next_is_pipe || prev_is_pipe;

            if (strcmp(e->cmd.exe, "exit") == 0) {
                exit_encountered = true;
                if (!in_pipeline) {
                    int code = 0;
                    if (e->cmd.arg_count >= 1) {
                        code = atoi(e->cmd.args[0]);
                    }
                    exit(code);
                }
            }

            if (!in_pipeline) {
                if (strcmp(e->cmd.exe, "cd") == 0) {
                    if (e->cmd.arg_count < 1) {
                        fprintf(stderr, "cd: missing argument\n");
                        last_status = 1;
                    } else {
                        if (chdir(e->cmd.args[0]) != 0) {
                            perror("cd");
                            last_status = 1;
                        } else {
                            last_status = 0;
                        }
                    }
                    prev_type = e->type;
                    continue;
                }
            }

            if (e->next && e->next->type == EXPR_TYPE_PIPE) {
                if (pipe(pipefd) == -1) {
                    perror("pipe");
                    exit(EXIT_FAILURE);
                }
            }
            
            pid = fork();
            if (pid == 0) {
                if (prev_fd != -1) {
                    dup2(prev_fd, STDIN_FILENO);
                    close(prev_fd);
                }
                if (e->next && e->next->type == EXPR_TYPE_PIPE) {
                    close(pipefd[0]);
                    dup2(pipefd[1], STDOUT_FILENO);
                    close(pipefd[1]);
                }

                bool is_last_cmd = !(e->next && e->next->type == EXPR_TYPE_PIPE);
                if (is_last_cmd) {
                    int fd_redir;
                    if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
                        fd_redir = open(line->out_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
                        if (fd_redir == -1) {
                            perror("open");
                            exit(EXIT_FAILURE);
                        }
                        dup2(fd_redir, STDOUT_FILENO);
                        close(fd_redir);
                    } else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
                        fd_redir = open(line->out_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
                        if (fd_redir == -1) {
                            perror("open");
                            exit(EXIT_FAILURE);
                        }
                        dup2(fd_redir, STDOUT_FILENO);
                        close(fd_redir);
                    }
                }
                execute_command(&e->cmd, line);
            } else if (pid < 0) {
                perror("fork");
                exit(EXIT_FAILURE);
            }

            pipeline_pids[pipeline_count++] = pid;

            if (prev_fd != -1) {
                close(prev_fd);
                prev_fd = -1;
            }
            if (e->next && e->next->type == EXPR_TYPE_PIPE) {
                close(pipefd[1]);
                prev_fd = pipefd[0];
            }

            int status;
            if (!next_is_pipe) {
                if (!line->is_background) {
                    for (size_t i = 0; i < pipeline_count; i++) {
                        waitpid(pipeline_pids[i], &status, 0);
                        if (WIFEXITED(status)) {
                            if (exit_encountered) {
                                last_status = WEXITSTATUS(status);
                                break;
                            }
                            if (i == pipeline_count - 1) {
                                last_status = WEXITSTATUS(status);
                            }
                        }
                    }
                }
                pipeline_count = 0;
                exit_encountered = false;
            }
        }

        prev_type = e->type;
    }

    shell_last_exit_status = last_status;
}

int main(void) {
    setbuf(stdout, NULL);
    const size_t buf_size = 1024;
    char buf[buf_size];
    int rc;
    struct parser *p = parser_new();
    
    if (!bg_initialized) {
        pid_array_init(&bg_children);
        bg_initialized = true;
    }

    while ((rc = read(STDIN_FILENO, buf, buf_size)) >= 0) {
        if (rc == 0) {
            struct command_line *line = NULL;
            while (true) {
                enum parser_error err = parser_pop_next(p, &line);
                if (err == PARSER_ERR_NONE && line == NULL)
                    break;
                if (err != PARSER_ERR_NONE) {
                    fprintf(stderr, "Error: %d\n", err);
                    continue;
                }
                if (line->is_background) {
                    pid_t bg_pid = fork();
                    if (bg_pid == 0) {
                        line->is_background = false;
                        execute_pipeline(line);
                        _exit(shell_last_exit_status);
                    } else if (bg_pid > 0) {
                        pid_array_push(&bg_children, bg_pid);
                    } else {
                        perror("fork");
                    }
                } else {
                    execute_pipeline(line);
                }
                command_line_delete(line);
            }

            if (bg_initialized) {
                int last_bg_code = pid_array_wait_and_free(&bg_children);
                if (last_bg_code)
                    shell_last_exit_status = last_bg_code;
                bg_initialized = false;
            }
            parser_delete(p);
            return shell_last_exit_status;
        }

        parser_feed(p, buf, rc);
        struct command_line *line = NULL;
        while (true) {
            enum parser_error err = parser_pop_next(p, &line);
            if (err == PARSER_ERR_NONE && line == NULL)
                break;
            if (err != PARSER_ERR_NONE) {
                fprintf(stderr, "Error: %d\n", err);
                continue;
            }
            if (line->is_background) {
                pid_t bg_pid = fork();
                if (bg_pid == 0) {
                    line->is_background = false;
                    execute_pipeline(line);
                    _exit(shell_last_exit_status);
                } else if (bg_pid > 0) {
                    pid_array_push(&bg_children, bg_pid);
                } else {
                    perror("fork");
                }
            } else {
                execute_pipeline(line);
            }

            command_line_delete(line);
        }
    }

    if (rc < 0) {
        perror("read");
        exit(EXIT_FAILURE);
    }

    if (bg_initialized) {
        int last_bg_code = pid_array_wait_and_free(&bg_children);
        if (last_bg_code)
            shell_last_exit_status = last_bg_code;
    }

    parser_delete(p);
    return shell_last_exit_status;
}