#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <librdkafka/rdkafka.h>

#define MAX_EGRESS_EVENTS 64

// Struct representing the compiled payload sent to downstream clients
typedef struct {
    int client_fd;
    char *data;
    size_t length;
} egress_msg_t;

// Forward declaration of our C SDK wrapper function
int query_dynamodb_metadata(const char* message_id, char* dest_buffer, size_t dest_len);

// Rebalance Callback: Safely coordinates partition shifts for complex consumer groups
static void rebalance_cb(rd_kafka_t *rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t *partitions, void *opaque) {
    switch (err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            printf("%% Consumer Group: Partitions assigned. Restoring local state markers...\n");
            rd_kafka_assign(rk, partitions);
            break;
        case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            printf("%% Consumer Group: Partitions revoked. Flushing pending database queries...\n");
            rd_kafka_assign(rk, NULL); // Commit offsets and yield ownership safely
            break;
        default:
            fprintf(stderr, "%% Rebalance failed: %s\n", rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
    }
}

// Thread 3: Dedicated Kafka Consumer & DynamoDB Hybrid Resolver Loop
void* kafka_consumer_thread(void *arg) {
    rd_kafka_t *rk_consumer = (rd_kafka_t *)arg;
    char db_buffer[2048]; // Thread-local scratchpad for database lookups

    while (1) {
        // Poll Kafka stream with a 100ms blocking window
        rd_kafka_message_t *rkmessage = rd_kafka_consumer_poll(rk_consumer, 100);
        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                fprintf(stderr, "%% Consumer error: %s\n", rd_kafka_message_errstr(rkmessage));
            }
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        // --- IDEMPOTENCY / DYNAMODB JOIN LOGIC ---
        // 1. Extract message business identifier (assuming it is stored in the Kafka Message Key)
        char msg_id[128] = {0};
        if (rkmessage->key && rkmessage->key_len < sizeof(msg_id)) {
            memcpy(msg_id, rkmessage->key, rkmessage->key_len);
        }

        // 2. Query DynamoDB using the custom C wrapper to fetch auxiliary metadata
        memset(db_buffer, 0, sizeof(db_buffer));
        int db_status = query_dynamodb_metadata(msg_id, db_buffer, sizeof(db_buffer));

        if (db_status == 0) { // Success
            // 3. Compile the combined payload (Kafka event + DynamoDB payload)
            size_t combined_len = rkmessage->len + strlen(db_buffer) + 2;
            char *egress_payload = malloc(combined_len);
            
            if (egress_payload) {
                snprintf(egress_payload, combined_len, "%s|%s", (char*)rkmessage->payload, db_buffer);
                
                // TODO: Enqueue this `egress_payload` to your Thread 4 Egress Ring Buffer
                // to be picked up by the epoll outer context and streamed over the socket.
                
                free(egress_payload); 
            }
        } else if (db_status == -2) {
            // DLQ LOGIC: Permanent data or parsing failure. Route to DLQ topic, then commit offset.
            printf("%% Routing payload to DLQ due to invalid database keys.\n");
        } else {
            // Transient database connection drop or throttling. Do NOT commit offset. Backoff and retry.
            usleep(50000); // 50ms backing off window
            rd_kafka_message_destroy(rkmessage);
            continue; 
        }

        // Commit processing offset explicitly back to Kafka cluster
        rd_kafka_commit_message(rk_consumer, rkmessage, 0);
        rd_kafka_message_destroy(rkmessage);
    }
    return NULL;
}
