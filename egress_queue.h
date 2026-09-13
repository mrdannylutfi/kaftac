#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#define MAX_EGRESS_QUEUE_SIZE 25000 // Safely holds burst data during spikes

// Complete structure containing everything Thread 4 needs to stream to a client
typedef struct {
    int client_fd;
    char *payload;
    size_t length;
} egress_msg_t;

typedef struct {
    egress_msg_t *data[MAX_EGRESS_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} egress_ring_buffer_t;

static egress_ring_buffer_t egress_buf;

void init_egress_buffer() {
    egress_buf.head = 0;
    egress_buf.tail = 0;
    egress_buf.count = 0;
    pthread_mutex_init(&egress_buf.lock, NULL);
    pthread_cond_init(&egress_buf.not_full, NULL);
    pthread_cond_init(&egress_buf.not_empty, NULL);
}

// Thread 3 calls this to hand off a unified Kafka+DynamoDB payload
void enqueue_egress(int client_fd, char *compiled_data, size_t dynamic_len) {
    pthread_mutex_lock(&egress_buf.lock);

    // If the egress pipeline is clogged (e.g., slow client downstream), drop or block
    while (egress_buf.count >= MAX_EGRESS_QUEUE_SIZE) {
        // Option A: Block Thread 3 (applies backpressure upstream to Kafka consumer)
        pthread_cond_wait(&egress_buf.not_full, &egress_buf.lock);
    }

    egress_msg_t *msg = malloc(sizeof(egress_msg_t));
    msg->client_fd = client_fd;
    msg->payload = compiled_data; // Thread 4 takes absolute heap memory ownership
    msg->length = dynamic_len;

    egress_buf.data[egress_buf.tail] = msg;
    egress_buf.tail = (egress_buf.tail + 1) % MAX_EGRESS_QUEUE_SIZE;
    egress_buf.count++;

    pthread_cond_signal(&egress_buf.not_empty);
    pthread_mutex_unlock(&egress_buf.lock);
}

// Thread 4 calls this to extract payloads and pump them into the client epoll loop
egress_msg_t* dequeue_egress() {
    pthread_mutex_lock(&egress_buf.lock);

    while (egress_buf.count == 0) {
        pthread_cond_wait(&egress_buf.not_empty, &egress_buf.lock);
    }

    egress_msg_t *msg = egress_buf.data[egress_buf.head];
    egress_buf.head = (egress_buf.head + 1) % MAX_EGRESS_QUEUE_SIZE;
    egress_buf.count--;

    pthread_cond_signal(&egress_buf.not_full);
    pthread_mutex_unlock(&egress_buf.lock);
    return msg;
}
