#include "userfs.h"

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#define INITIAL_DESCRIPTORS 16
#define DESCRIPTOR_GROW_FACTOR 2

enum {
    UFS_BLOCK_SIZE = 4 * 1024,
    UFS_MAX_FILE_SIZE = 1024 * 1024 * 100,
};

static enum ufs_error_code g_last_error = UFS_ERR_NO_ERR;

typedef struct ufs_block_t {
    char *data;
    int bytes_used;
    struct ufs_block_t *next;
    struct ufs_block_t *prev;
} ufs_block_t;

typedef struct ufs_file_t {
    ufs_block_t *block_head;
    ufs_block_t *block_tail;
    int ref_count;
    char *name;
    struct ufs_file_t *next;
    struct ufs_file_t *prev;
    bool is_deleted;
} ufs_file_t;

static ufs_file_t *g_file_list = NULL;

typedef struct ufs_desc_t {
    ufs_file_t *file_ptr;
    int block_idx;
    int cursor_in_block;
    enum open_flags flags;
    struct ufs_desc_t *next_recycled;
} ufs_desc_t;

static ufs_desc_t **g_descriptor_table = NULL;
static int g_descriptor_count = 0;
static int g_descriptor_capacity = 0;

static ufs_desc_t *g_recycled_descriptors = NULL;

enum ufs_error_code ufs_errno() {
    return g_last_error;
}

static enum ufs_error_code initialize_descriptor_table() {
    g_descriptor_table = calloc(INITIAL_DESCRIPTORS, sizeof(ufs_desc_t *));
    if (!g_descriptor_table) return UFS_ERR_NO_MEM;
    g_descriptor_count = 0;
    g_descriptor_capacity = INITIAL_DESCRIPTORS;
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code
expand_descriptor_table(void)
{
    if (g_descriptor_count < g_descriptor_capacity)
        return UFS_ERR_NO_ERR;

    int new_capacity = g_descriptor_capacity * DESCRIPTOR_GROW_FACTOR;
    ufs_desc_t **new_table = calloc(new_capacity, sizeof(*new_table));
    if (new_table == NULL)
        return UFS_ERR_NO_MEM;

    memcpy(new_table, g_descriptor_table, sizeof(*g_descriptor_table) * g_descriptor_capacity);
    free(g_descriptor_table);
    g_descriptor_table = new_table;
    g_descriptor_capacity = new_capacity;
    return UFS_ERR_NO_ERR;
}

static enum ufs_error_code append_block_to_file(ufs_file_t *file) {
    ufs_block_t *block = calloc(1, sizeof(ufs_block_t));
    if (!block) return UFS_ERR_NO_MEM;
    block->data = calloc(UFS_BLOCK_SIZE, 1);
    if (!block->data) { free(block); return UFS_ERR_NO_MEM; }
    if (!file->block_head)
        file->block_head = file->block_tail = block;
    else {
        file->block_tail->next = block;
        block->prev = file->block_tail;
        file->block_tail = block;
    }
    return UFS_ERR_NO_ERR;
}

static void release_file_blocks(ufs_block_t *block) {
    while (block) {
        ufs_block_t *next_block = block->next;
        free(block->data);
        free(block);
        block = next_block;
    }
}

static ufs_file_t *create_file_metadata(const char *name) {
    ufs_file_t *file = calloc(1, sizeof(ufs_file_t));
    if (!file) return NULL;
    file->name = strdup(name);
    if (!file->name) { free(file); return NULL; }
    if (append_block_to_file(file) != UFS_ERR_NO_ERR) { free(file->name); free(file); return NULL; }
    if (g_file_list) { file->next = g_file_list; g_file_list->prev = file; }
    g_file_list = file;
    return file;
}

static void destroy_file_metadata(ufs_file_t *file) {
    if (file->prev) file->prev->next = file->next;
    if (file->next) file->next->prev = file->prev;
    if (file == g_file_list) g_file_list = file->next;
    release_file_blocks(file->block_head);
    free(file->name);
    free(file);
}

static ufs_file_t *lookup_file_by_name(const char *name) {
    for (ufs_file_t *file = g_file_list; file; file = file->next)
        if (strcmp(file->name, name) == 0 && !file->is_deleted)
            return file;
    return NULL;
}

static ufs_desc_t *
get_recycled_or_new_descriptor(ufs_file_t *file, enum open_flags flags)
{
    ufs_desc_t *desc;
    if (g_recycled_descriptors) {
        desc = g_recycled_descriptors;
        g_recycled_descriptors = g_recycled_descriptors->next_recycled;
        memset(desc, 0, sizeof(*desc));
    } else {
        desc = calloc(1, sizeof(ufs_desc_t));
        if (!desc)
            return NULL;
    }
    desc->file_ptr = file;
    desc->flags = flags;
    return desc;
}

static int
find_available_descriptor_index(void)
{
    if (!g_descriptor_table)
        return -1;

    while (true) {
        for (int i = 0; i < g_descriptor_capacity; ++i) {
            if (g_descriptor_table[i] == NULL)
                return i;
        }
        if (expand_descriptor_table() != UFS_ERR_NO_ERR)
            return -1;
    }
}

static ufs_desc_t *lookup_descriptor(int fd) {
    if (fd < 0 || fd >= g_descriptor_count) return NULL;
    return g_descriptor_table[fd];
}

static bool check_write_permission(ufs_desc_t *desc) {
    if (desc->flags == 0) return true;
    if (desc->flags & UFS_WRITE_ONLY) return true;
    if (desc->flags & UFS_READ_WRITE) return true;
    if (desc->flags & UFS_CREATE) return true;
    return false;
}

static bool check_read_permission(ufs_desc_t *desc) {
    if (desc->flags == 0) return true;
    if (desc->flags & UFS_READ_ONLY) return true;
    if (desc->flags & UFS_READ_WRITE) return true;
    if (desc->flags & UFS_CREATE) return true;
    return false;
}

int ufs_open(const char *name, int flags) {
    if (!g_descriptor_table && initialize_descriptor_table() != UFS_ERR_NO_ERR)
        return -1;
    ufs_file_t *file = lookup_file_by_name(name);
    if (!file) {
        if (!(flags & UFS_CREATE)) { g_last_error = UFS_ERR_NO_FILE; return -1; }
        file = create_file_metadata(name);
        if (!file) { g_last_error = UFS_ERR_NO_MEM; return -1; }
    }
    int index = find_available_descriptor_index();
    if (index == -1) return -1;
    ufs_desc_t *desc = get_recycled_or_new_descriptor(file, flags);
    if (!desc) { g_last_error = UFS_ERR_NO_MEM; return -1; }
    ++file->ref_count;
    g_descriptor_table[index] = desc;
    if (index == g_descriptor_count) ++g_descriptor_count;
    g_last_error = UFS_ERR_NO_ERR;
    return index;
}

ssize_t ufs_write(int fd, const char *buf, size_t size) {
    ufs_desc_t *desc = lookup_descriptor(fd);
    if (!desc) { g_last_error = UFS_ERR_NO_FILE; return -1; }
    if (!check_write_permission(desc)) { g_last_error = UFS_ERR_NO_PERMISSION; return -1; }
    ufs_file_t *file = desc->file_ptr;
    ufs_block_t *current_block = file->block_head;
    for (int i = 0; i < desc->block_idx; i++) current_block = current_block->next;

    size_t cursor_position = desc->block_idx * UFS_BLOCK_SIZE + desc->cursor_in_block;
    size_t file_size = 0;
    for(ufs_block_t* b = file->block_head; b != NULL; b = b->next) file_size += b->bytes_used;

    if (cursor_position + size > file_size && cursor_position + size > UFS_MAX_FILE_SIZE) {
        size_t available_space = UFS_MAX_FILE_SIZE - cursor_position;
        if (available_space < size) {
            g_last_error = UFS_ERR_NO_MEM;
            return -1;
        }
    }

    ssize_t bytes_written = 0;
    while (bytes_written < (ssize_t)size) {
        if (desc->cursor_in_block == UFS_BLOCK_SIZE) {
            if (!current_block->next) {
                if (append_block_to_file(file) != UFS_ERR_NO_ERR) {
                    g_last_error = UFS_ERR_NO_MEM;
                    return bytes_written > 0 ? bytes_written : -1;
                }
                current_block = file->block_tail;
            } else {
                current_block = current_block->next;
            }
            desc->cursor_in_block = 0;
            ++desc->block_idx;
        }
        size_t bytes_to_write = UFS_BLOCK_SIZE - desc->cursor_in_block;
        if (size - bytes_written < bytes_to_write) bytes_to_write = size - bytes_written;

        memcpy(current_block->data + desc->cursor_in_block, buf + bytes_written, bytes_to_write);
        desc->cursor_in_block += bytes_to_write;
        bytes_written += bytes_to_write;
        if (desc->cursor_in_block > current_block->bytes_used)
            current_block->bytes_used = desc->cursor_in_block;
    }
    g_last_error = UFS_ERR_NO_ERR;
    return bytes_written;
}

ssize_t ufs_read(int fd, char *buf, size_t size) {
    ufs_desc_t *desc = lookup_descriptor(fd);
    if (!desc) { g_last_error = UFS_ERR_NO_FILE; return -1; }
    if (!check_read_permission(desc)) { g_last_error = UFS_ERR_NO_PERMISSION; return -1; }
    ufs_block_t *current_block = desc->file_ptr->block_head;
    for (int i = 0; i < desc->block_idx; i++) current_block = current_block->next;
    ssize_t total_bytes_read = 0;
    while (total_bytes_read < (ssize_t)size) {
        if (desc->cursor_in_block == UFS_BLOCK_SIZE) {
            current_block = current_block->next;
            if (!current_block) return total_bytes_read;
            desc->cursor_in_block = 0;
            ++desc->block_idx;
        }
        size_t bytes_to_read = current_block->bytes_used - desc->cursor_in_block;
        if (size - total_bytes_read < bytes_to_read) bytes_to_read = size - total_bytes_read;
        if (bytes_to_read == 0) return total_bytes_read;
        memcpy(buf + total_bytes_read, current_block->data + desc->cursor_in_block, bytes_to_read);
        desc->cursor_in_block += bytes_to_read;
        total_bytes_read += bytes_to_read;
    }
    return total_bytes_read;
}

int ufs_close(int fd) {
    ufs_desc_t *desc = lookup_descriptor(fd);
    if (!desc) { g_last_error = UFS_ERR_NO_FILE; return -1; }
    ufs_file_t *file = desc->file_ptr;
    --file->ref_count;
    if (file->is_deleted && file->ref_count == 0)
        destroy_file_metadata(file);
    desc->next_recycled = g_recycled_descriptors;
    g_recycled_descriptors = desc;
    g_descriptor_table[fd] = NULL;
    if (g_descriptor_count - 1 == fd)
        while (g_descriptor_count > 0 && !g_descriptor_table[g_descriptor_count - 1]) --g_descriptor_count;
    return 0;
}

int ufs_delete(const char *name) {
    ufs_file_t *file = lookup_file_by_name(name);
    if (!file) { g_last_error = UFS_ERR_NO_FILE; return -1; }
    if (file->ref_count != 0)
        file->is_deleted = true;
    else
        destroy_file_metadata(file);
    return 0;
}

int ufs_resize(int fd, size_t new_size) {
    ufs_desc_t *desc = lookup_descriptor(fd);
    if (!desc) { g_last_error = UFS_ERR_NO_FILE; return -1; }
    if (!check_write_permission(desc)) { g_last_error = UFS_ERR_NO_PERMISSION; return -1; }
    if (new_size > UFS_MAX_FILE_SIZE) { g_last_error = UFS_ERR_NO_MEM; return -1; }
    ufs_file_t *file = desc->file_ptr;

    size_t current_size = 0;
    int block_count = 0;
    ufs_block_t *current_block = file->block_head;
    while (current_block) {
        current_size += current_block->bytes_used;
        if (current_size >= new_size) break;
        current_block = current_block->next;
        block_count++;
    }

    if (current_size > new_size) {
        if (current_block) {
            release_file_blocks(current_block->next);
            file->block_tail = current_block;
            current_block->next = NULL;
            size_t size_in_prev_blocks = block_count * UFS_BLOCK_SIZE;
            if (new_size > size_in_prev_blocks) {
                current_block->bytes_used = new_size - size_in_prev_blocks;
            } else {
                 current_block->bytes_used = 0;
            }
        }

        for (int i = 0; i < g_descriptor_count; i++) {
            ufs_desc_t *d = g_descriptor_table[i];
            if (d && d->file_ptr == file) {
                size_t cursor_pos = d->block_idx * UFS_BLOCK_SIZE + d->cursor_in_block;
                if(cursor_pos > new_size){
                    d->block_idx = block_count;
                    d->cursor_in_block = current_block->bytes_used;
                }
            }
        }
    } else {
        size_t size_to_add = new_size - current_size;
        while(size_to_add > 0) {
            if(!file->block_tail) { 
                if (append_block_to_file(file) != UFS_ERR_NO_ERR) return -1;
            }
            
            size_t space_in_last_block = UFS_BLOCK_SIZE - file->block_tail->bytes_used;
            size_t chunk = (size_to_add < space_in_last_block) ? size_to_add : space_in_last_block;
            
            file->block_tail->bytes_used += chunk;
            current_size += chunk;
            size_to_add -= chunk;

            if (size_to_add > 0) {
                if (append_block_to_file(file) != UFS_ERR_NO_ERR) return -1;
            }
        }
    }

    return 0;
}

void ufs_destroy(void) {
    for (int i = 0; i < g_descriptor_count; i++)
        free(g_descriptor_table[i]);
    free(g_descriptor_table);
    g_descriptor_table = NULL;
    while (g_file_list)
        destroy_file_metadata(g_file_list);

    while (g_recycled_descriptors) {
        ufs_desc_t *next_desc = g_recycled_descriptors->next_recycled;
        free(g_recycled_descriptors);
        g_recycled_descriptors = next_desc;
    }
}

