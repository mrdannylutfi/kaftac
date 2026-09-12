#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stat.h>
#include <librdkafka/rdkafka.h>

#define MAX_QUEUE_SIZE 50000
#define PROMETHEUS_PORT 9100

// --- SYSTEM TELEMETRY COUNTERS ---
static _Atomic uint64_t total_ingress_messages = 0;
static _Atomic uint64_t total_burst_events = 0;
static _Atomic uint64_t queue_overflow_backpressure_events = 0;

typedef struct {
    uint32_t length;
    char *payload;
    char *auth_token; // Captured from socket handshake or metadata boundary
} msg_t;

typedef struct {
    msg_t *data[MAX_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} ring_buffer_t;

extern ring_buffer_t ingress_buf;
extern rd_kafka_topic_t *rkt_ingress;
extern rd_kafka_t *rk_producer;

// --- PROMETHEUS SCRAPE TARGET ENGINE (THREAD) ---
void* prometheus_metric_exporter_thread(void *arg) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PROMETHEUS_PORT);
    
    bind(server_fd, (struct sockaddr *)&addr, sizeof(addr));
    listen(server_fd, 5);
    
    char http_response[2048];
    
    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        
        // Read incoming browser/Prometheus scrape header request (discard)
        char dummy_buf[1024];
        recv(client_fd, dummy_buf, sizeof(dummy_buf), 0);
        
        // Dynamically build the open-metrics payload format string
        snprintf(http_response, sizeof(http_response),
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: close\r\n\r\n"
            "# HELP c_engine_ingress_messages_total Total count of raw binary socket frames parsed.\n"
            "# TYPE c_engine_ingress_messages_total counter\n"
            "c_engine_ingress_messages_total %llu\n\n"
            "# HELP c_engine_burst_spikes_total Tracks sudden 5 percent traffic load burst occurrences.\n"
            "# TYPE c_engine_burst_spikes_total counter\n"
            "c_engine_burst_spikes_total %llu\n\n"
            "# HELP c_engine_backpressure_events_total Counts instances socket epoll reads dropped to save memory.\n"
            "# TYPE c_engine_backpressure_events_total counter\n"
            "c_engine_backpressure_events_total %llu\n",
            (unsigned long long)total_ingress_messages,
            (unsigned long long)total_burst_events,
            (unsigned long long)queue_overflow_backpressure_events
        );
        
        send(client_fd, http_response, strlen(http_response), 0);
        close(client_fd);
    }
    return NULL;
}

// --- KAFKA PRODUCER LOOP WITH METADATA INJECTION ---
void* kafka_producer_thread(void *arg) {
    ring_buffer_t *buf = (ring_buffer_t *)arg;
    uint64_t last_batch_count = 0;
    time_t last_check = time(NULL);

    while (1) {
        // Dequeue tracking item logic...
        pthread_mutex_lock(&buf->lock);
        while (buf->count == 0) {
            pthread_cond_wait(&buf->not_empty, &buf->lock);
        }
        msg_t *msg = buf->data[buf->head];
        buf->head = (buf->head + 1) % MAX_QUEUE_SIZE;
        buf->count--;
        pthread_cond_signal(&buf->not_full);
        pthread_mutex_unlock(&buf->lock);

        // 1. Telemetry Monitoring for 5% Spikes
        total_ingress_messages++;
        
        // Detect sudden burst rates inside sliding time frames
        time_t now = time(NULL);
        if (now - last_check >= 1) {
            uint64_t delta = total_ingress_messages - last_batch_count;
            if (delta > 5000) {  // Threshold matching tens of thousands/sec scale
                total_burst_events++;
            }
            last_batch_count = total_ingress_messages;
            last_check = now;
        }

        // 2. Allocate Native Kafka Headers Object Array
        rd_kafka_headers_t *headers = rd_kafka_headers_new(2);
        
        // Inject TLS/Security trace contexts and device signature boundaries
        const char *security_clearance = "CONFIDENTIAL_LEVEL_3";
        rd_kafka_header_add(headers, "X-Security-Clearance", -1, security_clearance, strlen(security_clearance));
        
        if (msg->auth_token) {
            rd_kafka_header_add(headers, "X-Session-Token", -1, msg->auth_token, strlen(msg->auth_token));
        }

        // 3. Hand over ownership directly to the broker execution lane
        retry_kafka:
        if (rd_kafka_producev(rk_producer,
                              RD_KAFKA_V_TOPIC(rd_kafka_topic_name(rkt_ingress)),
                              RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
                              RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_FREE),
                              RD_KAFKA_V_VALUE(msg->payload, msg->length),
                              RD_KAFKA_V_HEADERS(headers), // Takes structural memory ownership
                              RD_KAFKA_V_END) == -1) {
            
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                queue_overflow_backpressure_events++;
                rd_kafka_poll(rk_producer, 10);
                goto retry_kafka;
            }
            // If producev fails completely, headers must be manually freed
            rd_kafka_headers_destroy(headers);
            free(msg->payload);
        }

        rd_kafka_poll(rk_producer, 0);
        if (msg->auth_token) free(msg->auth_token);
        free(msg);
    }

    // --- ADDITIONAL EGRESS TELEMETRY COUNTERS ---
static _Atomic uint64_t total_egress_messages = 0;
static _Atomic uint64_t dynamodb_failures = 0;

// Upgraded Thread Loop: Prometheus Scrape Engine
void* prometheus_metric_exporter_thread(void *arg) {
    // ... [Previous socket setup code remains identical] ...
    
    char http_response[2048];
    while (1) {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) continue;
        
        char dummy_buf[1024];
        recv(client_fd, dummy_buf, sizeof(dummy_buf), 0);
        
        snprintf(http_response, sizeof(http_response),
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nConnection: close\r\n\r\n"
            "# HELP c_engine_ingress_messages_total Total count of raw binary socket frames parsed.\n"
            "c_engine_ingress_messages_total %llu\n\n"
            "# HELP c_engine_burst_spikes_total Tracks sudden 5 percent traffic load occurrences.\n"
            "c_engine_burst_spikes_total %llu\n\n"
            "# HELP c_engine_backpressure_events_total Counts instances socket epoll reads dropped to save memory.\n"
            "c_engine_backpressure_events_total %llu\n\n"
            "# HELP c_engine_egress_messages_total Total count of unified records streamed back out to clients.\n"
            "c_engine_egress_messages_total %llu\n\n"
            "# HELP c_engine_dynamodb_failures_total Total count of failed or throttled DynamoDB lookup attempts.\n"
            "c_engine_dynamodb_failures_total %llu\n",
            (unsigned long long)total_ingress_messages,
            (unsigned long long)total_burst_events,
            (unsigned long long)queue_overflow_backpressure_events,
            (unsigned long long)total_egress_messages,
            (unsigned long long)dynamodb_failures
        );
        
        send(client_fd, http_response, strlen(http_response), 0);
        close(client_fd);
    }


 // Drain any leftover memory blocks on shutdown
pthread_mutex_lock(&ingress_buf.lock);
while (ingress_buf.count > 0) {
    msg_t *msg = ingress_buf.data[ingress_buf.head];
    //  free(msg->payload);
    free(msg->payload);
    if (msg->auth_token) free(msg->auth_token);
    free(msg);
    ingress_buf.head = (ingress_buf.head + 1) % MAX_QUEUE_SIZE;
    ingress_buf.count--;
}
pthread_mutex_unlock(&ingress_buf.lock);
    if (rd_kafka_producev(...) == -1) {
    // Kafka rejected the payload before taking ownership; clean it manually
    free(msg->payload); 
}
    if (msg->auth_token) free(msg->auth_token);
    free(msg);
    ingress_buf.head = (ingress_buf.head + 1) % MAX_QUEUE_SIZE;
    ingress_buf.count--;
}
pthread_mutex_unlock(&ingress_buf.lock);
 while (1) {
     pthread_mutex_lock(&ingress_buf.lock);
     if (ingress_buf.count == 0) {
         pthread_mutex_unlock(&ingress_buf.lock);
         break;
     }

     // Pop the message under lock, then process it without holding the lock
     msg_t *msg = ingress_buf.data[ingress_buf.head];
     ingress_buf.data[ingress_buf.head] = NULL;
     ingress_buf.head = (ingress_buf.head + 1) % MAX_QUEUE_SIZE;
     ingress_buf.count--;
     pthread_mutex_unlock(&ingress_buf.lock);

     // Recreate headers similar to the running producer so metadata is preserved
     rd_kafka_headers_t *headers = rd_kafka_headers_new(2);
     const char *security_clearance = "CONFIDENTIAL_LEVEL_3";
     rd_kafka_header_add(headers, "X-Security-Clearance", -1, security_clearance, strlen(security_clearance));
     if (msg->auth_token) {
         rd_kafka_header_add(headers, "X-Session-Token", -1, msg->auth_token, strlen(msg->auth_token));
     }

     int produce_failed = 0;
     int attempts = 0;
     while (attempts < 3) {
         if (rd_kafka_producev(rk_producer,
                               RD_KAFKA_V_TOPIC(rd_kafka_topic_name(rkt_ingress)),
                               RD_KAFKA_V_PARTITION(RD_KAFKA_PARTITION_UA),
                               RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_FREE),
                               RD_KAFKA_V_VALUE(msg->payload, msg->length),
                               RD_KAFKA_V_HEADERS(headers),
                               RD_KAFKA_V_END) == -1) {
             rd_kafka_resp_err_t err = rd_kafka_last_error();
             if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                 queue_overflow_backpressure_events++;
                 rd_kafka_poll(rk_producer, 10);
                 attempts++;
                 continue;
             }
             // Irrecoverable failure: headers must be destroyed and payload freed by caller
             rd_kafka_headers_destroy(headers);
             free(msg->payload);
             produce_failed = 1;
         }
         break;
     }

     rd_kafka_poll(rk_producer, 0);
     if (msg->auth_token) free(msg->auth_token);
     free(msg);
 }

 // Ensure outstanding messages are flushed to the broker before exit (timeout 5s)
 rd_kafka_flush(rk_producer, 5000);

     return NULL;
}
