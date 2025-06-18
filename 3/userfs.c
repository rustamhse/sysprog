#include "userfs.h"

#include <string.h>
#include <stdlib.h>

#define FD_INIT_CAP 10
#define FD_GROWTH 2

enum {
    BLOCK_SIZE = 4 * 1024,
    MAX_FILE_SIZE = 1024 * 1024 * 100,
};

static enum ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
    char *memory;
    int occupied;
    struct block *next;
    struct block *prev;
};

struct file {
    struct block *blocks;
    struct block *last_block;
    int refs;
    char *name;
    struct file *next;
    struct file *prev;
    int deleted;
};

static struct file *files_head = NULL;

struct filedesc {
    struct file *file;
    int block_num;
    int offset_in_block;
    enum open_flags flags;
};

static struct filedesc **fds = NULL;
static int fds_count = 0;
static int fds_capacity = 0;

enum ufs_error_code ufs_errno() {
    return ufs_error_code;
}

static enum ufs_error_code init_fds() {
    fds = calloc(FD_INIT_CAP, sizeof(struct filedesc *));
    if (!fds) return UFS_ERR_NO_MEM;
    fds_count = 0;
    fds_capacity = FD_INIT_CAP;
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code resize_fds() {
    int new_cap = fds_capacity;
    if (fds_count == fds_capacity)
        new_cap *= FD_GROWTH;

    if (new_cap == fds_capacity)
        return UFS_ERR_NO_ERR;

    struct filedesc **tmp = realloc(fds, sizeof(struct filedesc *) * new_cap);
    if (!tmp) return UFS_ERR_NO_MEM;
    memset(tmp + fds_count, 0, sizeof(struct filedesc *) * (new_cap - fds_count));
    fds = tmp;
    fds_capacity = new_cap;
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code add_block(struct file *f) {
    struct block *blk = calloc(1, sizeof(struct block));
    if (!blk) return UFS_ERR_NO_MEM;
    blk->memory = calloc(BLOCK_SIZE, 1);
    if (!blk->memory) { free(blk); return UFS_ERR_NO_MEM; }
    if (!f->blocks)
        f->blocks = f->last_block = blk;
    else {
        f->last_block->next = blk;
        blk->prev = f->last_block;
        f->last_block = blk;
    }
    return UFS_ERR_NO_ERR;
}

static void free_blocks(struct block *blk) {
    while (blk) {
        struct block *next = blk->next;
        free(blk->memory);
        free(blk);
        blk = next;
    }
}

static struct file *create_file(const char *name) {
    struct file *f = calloc(1, sizeof(struct file));
    if (!f) return NULL;
    f->name = strdup(name);
    if (!f->name) { free(f); return NULL; }
    if (add_block(f) != UFS_ERR_NO_ERR) { free(f->name); free(f); return NULL; }
    if (files_head) { f->next = files_head; files_head->prev = f; }
    files_head = f;
    return f;
}

static void remove_file(struct file *f) {
    if (f->prev) f->prev->next = f->next;
    if (f->next) f->next->prev = f->prev;
    if (f == files_head) files_head = f->next;
    free_blocks(f->blocks);
    free(f->name);
    free(f);
}

static struct file *find_file(const char *name) {
    for (struct file *f = files_head; f; f = f->next)
        if (strcmp(f->name, name) == 0 && !f->deleted)
            return f;
    return NULL;
}

static struct filedesc *alloc_fd(struct file *f, enum open_flags flags) {
    struct filedesc *desc = calloc(1, sizeof(struct filedesc));
    if (!desc) return NULL;
    desc->file = f;
    desc->flags = flags;
    return desc;
}

static int next_fd_index() {
    if (!fds) return -1;
    for (int i = 0; i < fds_capacity; i++)
        if (!fds[i]) return i;
    if (resize_fds() != UFS_ERR_NO_ERR) return -1;
    return fds_capacity / FD_GROWTH;
}

static struct filedesc *get_fd(int fd) {
    if (fd < 0 || fd >= fds_count) return NULL;
    return fds[fd];
}

static int can_write(struct filedesc *d) {
    return d->flags == 0 || (d->flags & (UFS_CREATE | UFS_WRITE_ONLY | UFS_READ_WRITE));
}

static int can_read(struct filedesc *d) {
    return d->flags == 0 || (d->flags & (UFS_CREATE | UFS_READ_ONLY | UFS_READ_WRITE));
}

int ufs_open(const char *name, int flags) {
    if (!fds && init_fds() != UFS_ERR_NO_ERR)
        return -1;
    struct file *f = find_file(name);
    if (!f) {
        if (!(flags & UFS_CREATE)) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
        f = create_file(name);
        if (!f) { ufs_error_code = UFS_ERR_NO_MEM; return -1; }
    }
    int idx = next_fd_index();
    if (idx == -1) return -1;
    struct filedesc *desc = alloc_fd(f, flags);
    if (!desc) { ufs_error_code = UFS_ERR_NO_MEM; return -1; }
    ++f->refs;
    fds[idx] = desc;
    if (idx == fds_count) ++fds_count;
    ufs_error_code = UFS_ERR_NO_ERR;
    return idx;
}

ssize_t ufs_write(int fd, const char *buf, size_t sz) {
    struct filedesc *desc = get_fd(fd);
    if (!desc) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
    if (!can_write(desc)) { ufs_error_code = UFS_ERR_NO_PERMISSION; return -1; }
    struct file *f = desc->file;
    struct block *blk = f->blocks;
    for (int i = 0; i < desc->block_num; i++) blk = blk->next;
    if ((size_t)(blk->occupied + desc->block_num * BLOCK_SIZE + sz) > MAX_FILE_SIZE) {
        ufs_error_code = UFS_ERR_NO_MEM; return -1;
    }

    ssize_t written = 0;
    while (written < (ssize_t)sz) {
        if (desc->offset_in_block == BLOCK_SIZE) {
            blk = blk->next;
            if (!blk) {
                if (add_block(f) != UFS_ERR_NO_ERR) return written;
                blk = f->last_block;
            }
            desc->offset_in_block = 0;
            ++desc->block_num;
        }
        size_t to_write = BLOCK_SIZE - desc->offset_in_block;
        if (sz - written < to_write) to_write = sz - written;
        memcpy(blk->memory + desc->offset_in_block, buf + written, to_write);
        desc->offset_in_block += to_write;
        written += to_write;
        if (desc->offset_in_block > blk->occupied)
            blk->occupied = desc->offset_in_block;
    }
    ufs_error_code = UFS_ERR_NO_ERR;
    return written;
}

ssize_t ufs_read(int fd, char *buf, size_t sz) {
    struct filedesc *desc = get_fd(fd);
    if (!desc) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
    if (!can_read(desc)) { ufs_error_code = UFS_ERR_NO_PERMISSION; return -1; }
    struct block *blk = desc->file->blocks;
    for (int i = 0; i < desc->block_num; i++) blk = blk->next;
    ssize_t read_bytes = 0;
    while (read_bytes < (ssize_t)sz) {
        if (desc->offset_in_block == BLOCK_SIZE) {
            blk = blk->next;
            if (!blk) return read_bytes;
            desc->offset_in_block = 0;
            ++desc->block_num;
        }
        size_t to_read = blk->occupied - desc->offset_in_block;
        if (sz - read_bytes < to_read) to_read = sz - read_bytes;
        if (to_read == 0) return read_bytes;
        memcpy(buf + read_bytes, blk->memory + desc->offset_in_block, to_read);
        desc->offset_in_block += to_read;
        read_bytes += to_read;
    }
    return read_bytes;
}

int ufs_close(int fd) {
    struct filedesc *desc = get_fd(fd);
    if (!desc) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
    struct file *f = desc->file;
    --f->refs;
    if (f->deleted && f->refs == 0)
        remove_file(f);
    free(desc);
    fds[fd] = NULL;
    if (fds_count - 1 == fd)
        while (fds_count > 0 && !fds[fds_count - 1]) --fds_count;
    resize_fds();
    return 0;
}

int ufs_delete(const char *name) {
    struct file *f = find_file(name);
    if (!f) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
    if (f->refs != 0)
        f->deleted = 1;
    else
        remove_file(f);
    return 0;
}

int ufs_resize(int fd, size_t new_size) {
    struct filedesc *desc = get_fd(fd);
    if (!desc) { ufs_error_code = UFS_ERR_NO_FILE; return -1; }
    if (!can_write(desc)) { ufs_error_code = UFS_ERR_NO_PERMISSION; return -1; }
    if (new_size > MAX_FILE_SIZE) { ufs_error_code = UFS_ERR_NO_MEM; return -1; }
    struct file *f = desc->file;
    struct block *blk = f->blocks;
    size_t curr_size = 0;
    int block_count = 0;

    while (blk) {
        curr_size += blk->occupied;
        if (curr_size > new_size) break;
        blk = blk->next;
        ++block_count;
    }

    if (curr_size > new_size) {
        free_blocks(blk->next);
        f->last_block = blk;
        blk->occupied = new_size - block_count * BLOCK_SIZE;

        for (int i = 0; i < fds_count; i++) {
            struct filedesc *d = fds[i];
            if (!d || d->file != f) continue;
            if (d->block_num >= block_count) {
                d->block_num = block_count;
                if (d->offset_in_block > blk->occupied)
                    d->offset_in_block = blk->occupied;
            }
        }
    } else {
        if (blk) {
            curr_size += BLOCK_SIZE - blk->occupied;
            blk->occupied = BLOCK_SIZE;
        }
        while (curr_size < new_size) {
            if (add_block(f) != UFS_ERR_NO_ERR) return -1;
            f->last_block->occupied = BLOCK_SIZE;
            curr_size += BLOCK_SIZE;
            block_count++;
        }
        f->last_block->occupied = new_size - block_count * BLOCK_SIZE;
    }
    return 0;
}

void ufs_destroy(void) {
    for (int i = 0; i < fds_count; i++)
        free(fds[i]);
    free(fds);
    fds = NULL;
    while (files_head)
        remove_file(files_head);
}
