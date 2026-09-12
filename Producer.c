//Prevents OOM Crashes
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <errno.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

#define MAX_EVENTS 64
#define BUFFER_SIZE 8192
#define MAX_QUEUE_SIZE 50000 // Upper bound to protect memory during peak spikes

// High-performance message struct passed between socket thread and Kafka producer thread
typedef struct {
    uint32_t length;
    char *payload;
} msg_t;

// Thread-safe Fixed Bounded Queue (Ring Buffer)
typedef struct {
    msg_t *data[MAX_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} ring_buffer_t;

ring_buffer_t ring_buf;
rd_kafka_t *rk_producer;
rd_kafka_topic_t *rkt_ingress;
int epoll_fd;

// Initialize Ring Buffer
void init_buffer(ring_buffer_t *buf) {
    buf->head = 0;
    buf->tail = 0;
    buf->count = 0;
    pthread_mutex_init(&buf->lock, NULL);
    pthread_cond_init(&buf->not_full, NULL);
    pthread_cond_init(&buf->not_empty, NULL);
}

// Enqueue message (Socket thread pushes here)
void enqueue(ring_buffer_t *buf, msg_t *msg, int fd) {
    pthread_mutex_lock(&buf->lock);
    
    // BACKPRESSURE: If queue is full during a "tens of thousands/sec" burst, 
    // stop listening to the socket temporarily instead of allocating infinitely.
    while (buf->count >= MAX_QUEUE_SIZE) {
        struct epoll_event ev;
        ev.events = EPOLLOUT; // Stop asking for EPOLLIN (reads)
        ev.data.fd = fd;
        epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        
        pthread_cond_wait(&buf->not_full, &buf->lock);
    }
    
    buf->data[buf->tail] = msg;
    buf->tail = (buf->tail + 1) % MAX_QUEUE_SIZE;
    buf->count++;
    
    pthread_cond_signal(&buf->not_empty);
    pthread_mutex_unlock(&buf->lock);
}

// Dequeue message (Kafka producer worker pulls from here)
msg_t* dequeue(ring_buffer_t *buf) {
    pthread_mutex_lock(&buf->lock);
    while (buf->count == 0) {
        pthread_cond_wait(&buf->not_empty, &buf->lock);
    }
    msg_t *msg = buf->data[buf->head];
    buf->head = (buf->head + 1) % MAX_QUEUE_SIZE;
    buf->count--;
    
    pthread_cond_signal(&buf->not_full);
    pthread_mutex_unlock(&buf->lock);
    return msg;
}

// Kafka Delivery Report Callback (Fires asynchronously on background thread)
static void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    if (rkmessage->err) {
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
        // TODO: Route to Dead Letter Queue (DLQ) topic here if error is permanent
    } else {
        // Success: Message is durable in Kafka broker. 
        // Safe to execute DynamoDB Async conditional updates/writes now.
    }
    // librdkafka automatically manages the memory cleanup if using proper flags
}

// Thread 2: Dedicated Kafka Producer Worker
void* kafka_producer_thread(void *arg) {
    while (1) {
        msg_t *msg = dequeue(&ring_buf);
        
        // Asynchronously produce to Kafka using zero-copy flag
        retry_produce:
        if (rd_kafka_produce(rkt_ingress, RD_KAFKA_PARTITION_UA,
                             RD_KAFKA_MSG_F_FREE, // Tells librdkafka to free(msg->payload) when done
                             msg->payload, msg->length,
                             NULL, 0, NULL) == -1) {
            
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                // Internal Kafka broker buffer full. Back off and poll to clear callbacks.
                rd_kafka_poll(rk_producer, 10);
                goto retry_produce;
            }
            fprintf(stderr, "%% Failed to produce to Kafka: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
            free(msg->payload); // Free manually if production failed catastrophically
        }
        
        // Fast event polling to clear delivery tokens
        rd_kafka_poll(rk_producer, 0);
        free(msg); // Free structural wrapper
    }
    return NULL;
}

// Set socket options to non-blocking
int make_socket_non_blocking(int sfd) {
    int flags = fcntl(sfd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(sfd, F_SETFL, flags | O_NONBLOCK);
}

// Thread 1: Ingress Edge-Triggered Epoll Event Loop
void handle_socket_reads(int client_fd) {
    while (1) {
        uint32_t payload_len = 0;
        
        // 1. Read 4-byte structural length prefix
        ssize_t count = recv(client_fd, &payload_len, sizeof(payload_len), 0);
        if (count == -1) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                perror("read error");
                close(client_fd);
            }
            break; // Buffer is dry for this epoll turn
        } else if (count == 0) {
            close(client_fd); // Client disconnected
            break;
        }
        
        // Convert network byte order if your clients use Big Endian
        payload_len = ntohl(payload_len); 
        
        // 2. Linear memory allocation for exact payload size
        char *buffer = malloc(payload_len);
        if (!buffer) {
            fprintf(stderr, "Out of memory allocating payload\n");
            break;
        }
        
        // 3. Extract the exact frame length from raw stream
        ssize_t bytes_read = recv(client_fd, buffer, payload_len, MSG_WAITALL);
        if (bytes_read < (ssize_t)payload_len) {
            free(buffer);
            fprintf(stderr, "Incomplete frame dropped\n");
            break;
        }
        
        // 4. Construct payload packet & ship to decoupled internal ring buffer
        msg_t *msg = malloc(sizeof(msg_t));
        msg->length = payload_len;
        msg->payload = buffer;
        
        enqueue(&ring_buf, msg, client_fd);
    }
}

int main() {
    init_buffer(&ring_buf);
    
    // Initialize Kafka configuration variables
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    char errstr[512];
    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);
    
    // Performance tuning configs for burst absorption (batching)
    rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", "100000", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "linger.ms", "5", errstr, sizeof(errstr)); // 5ms batch grouping
    
    rk_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    rkt_ingress = rd_kafka_topic_new(rk_producer, "ingress_topic", NULL);
    
    // Spawn Background Kafka Execution Thread
    pthread_t prod_tid;
    pthread_create(&prod_tid, NULL, kafka_producer_thread, NULL);
    
    // Setup Epoll Ingress Architecture
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    make_socket_non_blocking(server_fd);
    
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(8080);
    
    bind(server_fd, (struct sockaddr *)&address, sizeof(address));
    listen(server_fd, SOMAXCONN);
    
    epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN | EPOLLET; // Edge-Triggered
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);
    
    while (1) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd == server_fd) {
                // Handle new inbound connection scaling
                struct sockaddr_in client_addr;
                socklen_t client_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
                make_socket_non_blocking(client_fd);
                
                ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                ev.data.fd = client_fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
            } else if (events[i].events & EPOLLIN) {
                handle_socket_reads(events[i].data.fd);
                
                // Rearm epoll one-shot descriptor safety check
                ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
                ev.data.fd = events[i].data.fd;
                epoll_ctl(epoll_fd, EPOLL_CTL_MOD, events[i].data.fd, &ev);
            }
        }
    }
    return 0;
}
