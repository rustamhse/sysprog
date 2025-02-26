#include "parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>

static void execute_command(const struct command *cmd, const struct command_line *line) {
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
        exit(0);
    }

    int fd;
    if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
        fd = open(line->out_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);
        }
        dup2(fd, STDOUT_FILENO);
        close(fd);
    } else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
        fd = open(line->out_file, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd == -1) {
            perror("open");
            exit(EXIT_FAILURE);
        }
        dup2(fd, STDOUT_FILENO);
        close(fd);
    }

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
                execute_command(&e->cmd, line);
            } else if (pid < 0) {
                perror("fork");
                exit(EXIT_FAILURE);
            }

            if (prev_fd != -1) {
                close(prev_fd);
            }
            if (e->next && e->next->type == EXPR_TYPE_PIPE) {
                close(pipefd[1]);
                prev_fd = pipefd[0];
            }

            int status;
            waitpid(pid, &status, 0);
            if (WIFEXITED(status)) {
                last_status = WEXITSTATUS(status);
            }
        }

        prev_type = e->type;
    }
}

int main(void) {
    setbuf(stdout, NULL);
    const size_t buf_size = 1024;
    char buf[buf_size];
    int rc;
    struct parser *p = parser_new();
    
    while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
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
            execute_pipeline(line);
            command_line_delete(line);
        }
    }
    parser_delete(p);
    return 0;
}