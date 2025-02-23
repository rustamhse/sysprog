#include "corobus.h"
#include "libcoro.h"
#include "rlist.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

struct data_vector {
    unsigned *data;
    size_t size;
    size_t capacity;
};


/** Append @a count messages in @a data to the end of the vector. */
static void
data_vector_append_many(struct data_vector *vector,
    const unsigned *data, size_t count)
{
    if (vector->size + count > vector->capacity) {
        if (vector->capacity == 0)
            vector->capacity = 4;
        else
            vector->capacity *= 2;
        if (vector->capacity < vector->size + count)
            vector->capacity = vector->size + count;
        vector->data = realloc(vector->data,
            sizeof(vector->data[0]) * vector->capacity);
    }
    memcpy(&vector->data[vector->size], data, sizeof(data[0]) * count);
    vector->size += count;
}

/** Append a single message to the vector. */
static void
data_vector_append(struct data_vector *vector, unsigned data)
{
    data_vector_append_many(vector, &data, 1);
}

/** Pop @a count of messages into @a data from the head of the vector. */
static void
data_vector_pop_first_many(struct data_vector *vector, unsigned *data, size_t count)
{
    assert(count <= vector->size);
    memcpy(data, vector->data, sizeof(data[0]) * count);
    vector->size -= count;
    memmove(vector->data, &vector->data[count], vector->size * sizeof(vector->data[0]));
}

/** Pop a single message from the head of the vector. */
static unsigned
data_vector_pop_first(struct data_vector *vector)
{
    unsigned data = 0;
    data_vector_pop_first_many(vector, &data, 1);
    return data;
}

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
    struct rlist base;
    struct coro *coro;
    int woken;
};


/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
    struct rlist coros;
};

/** Suspend the current coroutine until it is woken up. */
static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
    struct wakeup_entry *entry = malloc(sizeof(*entry));
    entry->coro = coro_this();
    entry->woken = 0;
    rlist_add_tail_entry(&queue->coros, entry, base);
    coro_suspend();
    if (entry->woken == 0) {
         rlist_del_entry(entry, base);
         free(entry);
    } else {
         free(entry);
    }
}


/** Wakeup the first coroutine in the queue. */
static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
    if (rlist_empty(&queue->coros))
        return;
    struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
        struct wakeup_entry, base);
    rlist_del_entry(entry, base);
    entry->woken = 1;
    coro_wakeup(entry->coro);
}


struct coro_bus_channel {
    /** Channel max capacity. */
    size_t size_limit;
    /** Coroutines waiting until the channel is not full. */
    struct wakeup_queue send_queue;
    /** Coroutines waiting until the channel is not empty. */
    struct wakeup_queue recv_queue;
    /** Message queue. */
    struct data_vector data;
};

struct coro_bus {
    struct coro_bus_channel **channels;
    int channel_count;
    struct wakeup_queue broadcast_queue;
};


static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
    return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
    global_error = err;
}

struct coro_bus *
coro_bus_new(void)
{
    struct coro_bus *bus = malloc(sizeof(*bus));
    bus -> channels = NULL;
    bus -> channel_count = 0;

    rlist_create(&bus->broadcast_queue.coros);

    return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
    for (int i = 0; i < bus -> channel_count; i++){
        struct coro_bus_channel *ch = bus -> channels[i];
        if (ch != NULL){
            free(ch->data.data);
            free(ch);
        }
    }
    free(bus->channels);
    free(bus);
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
    int id = -1;
    for (int i = 0; i < bus->channel_count; i++) {
        if (bus->channels[i] == NULL) {
            id = i;
            break;
        }
    }
    if (id < 0) {
        int new_count = bus->channel_count + 1;
        bus->channels = realloc(bus->channels, sizeof(struct coro_bus_channel *) * new_count);
        if (bus->channels == NULL) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }
        bus->channels[new_count - 1] = NULL;
        id = bus->channel_count;
        bus->channel_count = new_count;
    }

    struct coro_bus_channel *ch = malloc(sizeof(*ch));
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    ch->size_limit = size_limit;
    ch->data.size = 0;
    ch->data.capacity = 0;
    ch->data.data = NULL;
    rlist_create(&ch->send_queue.coros);
    rlist_create(&ch->recv_queue.coros);

    bus->channels[id] = ch;
    return id;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return;
    }

    bus->channels[channel] = NULL;

    while (!rlist_empty(&ch->send_queue.coros)) {
        wakeup_queue_wakeup_first(&ch->send_queue);
    }
    while (!rlist_empty(&ch->recv_queue.coros)) {
        wakeup_queue_wakeup_first(&ch->recv_queue);
    }
    while (!rlist_empty(&bus->broadcast_queue.coros)) {
        wakeup_queue_wakeup_first(&bus->broadcast_queue);
    }

    free(ch->data.data);
    free(ch);
}


int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
    while (true) {
        int rc = coro_bus_try_send(bus, channel, data);
        if (rc == 0) {
            return 0;
        }
        enum coro_bus_error_code err = coro_bus_errno();
        if (err == CORO_BUS_ERR_NO_CHANNEL) {
            return -1;
        }
        if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            struct coro_bus_channel *ch = bus->channels[channel];
            if (ch == NULL) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
                return -1;
            }
            wakeup_queue_suspend_this(&ch->send_queue);
            continue;
        }
        return -1;
    }
}


int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    if (ch->data.size == ch->size_limit) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    data_vector_append(&ch->data, data);
    wakeup_queue_wakeup_first(&ch->recv_queue);
    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}


int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    while (true) {
        int rc = coro_bus_try_recv(bus, channel, data);
        if (rc == 0) {
            return 0;
        }
        enum coro_bus_error_code err = coro_bus_errno();
        if (err == CORO_BUS_ERR_NO_CHANNEL) {
            return -1;
        }
        if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            struct coro_bus_channel *ch = bus->channels[channel];
            if (ch == NULL) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
                return -1;
            }
            wakeup_queue_suspend_this(&ch->recv_queue);
            continue;
        }
        return -1;
    }
}


int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }
    unsigned tmp = data_vector_pop_first(&ch->data);
    *data = tmp;
    wakeup_queue_wakeup_first(&ch->send_queue);
    wakeup_queue_wakeup_first(&bus->broadcast_queue);

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}



#if NEED_BROADCAST

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
    bool has_channels = false;
    bool would_block = false;

    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL)
            continue;
        has_channels = true;
        if (ch->data.size >= ch->size_limit) {
            would_block = true;
        }
    }

    if (!has_channels) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    if (would_block) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    // Отправляем данные во все открытые каналы
    for (int i = 0; i < bus->channel_count; i++) {
        struct coro_bus_channel *ch = bus->channels[i];
        if (ch == NULL)
            continue;
        data_vector_append(&ch->data, data);
        wakeup_queue_wakeup_first(&ch->recv_queue);
    }

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return 0;
}

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
    while (true) {
        int rc = coro_bus_try_broadcast(bus, data);
        if (rc == 0)
            return 0;

        enum coro_bus_error_code err = coro_bus_errno();
        if (err != CORO_BUS_ERR_WOULD_BLOCK)
            return -1;

        bool has_channels = false;
        for (int i = 0; i < bus->channel_count; ++i) {
            if (bus->channels[i] != NULL) {
                has_channels = true;
                break;
            }
        }
        if (!has_channels) {
            coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
            return -1;
        }

        wakeup_queue_suspend_this(&bus->broadcast_queue);
    }
}



#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    unsigned sent = 0;
    while (sent < count) {
        int rc = coro_bus_try_send_v(bus, channel, data + sent, count - sent);
        if (rc > 0) {
            sent += rc;
            if (sent == count)
                return sent;
            continue;
        }
        enum coro_bus_error_code err = coro_bus_errno();  // <--- добавляем эту строку
        if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            if (sent > 0)
                return sent;
            struct coro_bus_channel *ch = bus->channels[channel];
            if (ch == NULL) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
                return -1;
            }
            wakeup_queue_suspend_this(&ch->send_queue);
            continue;
        }
        
        return -1;
    }
    return sent;
}


int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    size_t free_space = ch->size_limit - ch->data.size;
    if (free_space == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    size_t n = (count < free_space ? count : free_space);

    data_vector_append_many(&ch->data, data, n);

    wakeup_queue_wakeup_first(&ch->recv_queue);

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return n;
}


int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    unsigned received = 0;
    while (true) {
        int rc = coro_bus_try_recv_v(bus, channel, data + received, capacity - received);
        if (rc > 0) {
            received += rc;
            return received;
        }

        enum coro_bus_error_code err = coro_bus_errno();
        if (err == CORO_BUS_ERR_NO_CHANNEL) {
            return -1;
        }
        if (err == CORO_BUS_ERR_WOULD_BLOCK) {
            struct coro_bus_channel *ch = bus->channels[channel];
            if (ch == NULL) {
                coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
                return -1;
            }
            wakeup_queue_suspend_this(&ch->recv_queue);
            continue;
        }
        return -1;
    }
}


int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
    if (channel < 0 || channel >= bus->channel_count) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }
    struct coro_bus_channel *ch = bus->channels[channel];
    if (ch == NULL) {
        coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
        return -1;
    }

    if (ch->data.size == 0) {
        coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
        return -1;
    }

    unsigned n = ch->data.size;
    if (n > capacity)
        n = capacity;

    data_vector_pop_first_many(&ch->data, data, n);

    wakeup_queue_wakeup_first(&ch->send_queue);
    wakeup_queue_wakeup_first(&bus->broadcast_queue);

    coro_bus_errno_set(CORO_BUS_ERR_NONE);
    return n;
}


#endif