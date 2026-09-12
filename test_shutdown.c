#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

#define MAX_QUEUE_SIZE 16
#define NUM_MSGS 8

typedef struct msg_t {
    char *payload;
    char *auth_token;
    size_t payload_len;
} msg_t;

typedef struct {
    msg_t *data[MAX_QUEUE_SIZE];
    int head;
    int tail;
    int count;
    pthread_mutex_t lock;
} ring_queue_t;

static ring_queue_t ingress_buf;

static void enqueue_msg(const char *text) {
    pthread_mutex_lock(&ingress_buf.lock);
    if (ingress_buf.count >= MAX_QUEUE_SIZE) {
        fprintf(stderr, "queue full\n");
        pthread_mutex_unlock(&ingress_buf.lock);
        return;
    }
    msg_t *m = calloc(1, sizeof(*m));
    m->payload_len = strlen(text);
    m->payload = strdup(text);
    m->auth_token = NULL;
    ingress_buf.data[ingress_buf.tail] = m;
    ingress_buf.tail = (ingress_buf.tail + 1) % MAX_QUEUE_SIZE;
    ingress_buf.count++;
    pthread_mutex_unlock(&ingress_buf.lock);
}

// Mock produce function
static int mock_produce(msg_t *msg, int simulate_reject, int simulate_take_ownership,
                        int attempt_lock_inside_produce, int *out_took_owner)
{
    if (attempt_lock_inside_produce) {
        pthread_mutex_lock(&ingress_buf.lock);
        printf("[mock_produce] acquired ingress_buf.lock inside produce (simulating callback)\n");
        sleep(1);
        pthread_mutex_unlock(&ingress_buf.lock);
        printf("[mock_produce] released ingress_buf.lock inside produce\n");
    }

    if (simulate_reject) {
        *out_took_owner = 0;
        return -1;
    }

    if (simulate_take_ownership) {
        *out_took_owner = 1;
        return 0;
    }

    *out_took_owner = 0;
    return 0;
}

void good_drain(int simulate_reject, int simulate_take_owner, int attempt_lock_inside_produce)
{
    printf("[good_drain] starting safe drain\n");
    while (1) {
        pthread_mutex_lock(&ingress_buf.lock);
        if (ingress_buf.count == 0) {
            pthread_mutex_unlock(&ingress_buf.lock);
            break;
        }
        msg_t *msg = ingress_buf.data[ingress_buf.head];
        ingress_buf.data[ingress_buf.head] = NULL;
        ingress_buf.head = (ingress_buf.head + 1) % MAX_QUEUE_SIZE;
        ingress_buf.count--;
        pthread_mutex_unlock(&ingress_buf.lock);

        int took_owner = 0;
        int ret = mock_produce(msg, simulate_reject, simulate_take_owner, attempt_lock_inside_produce, &took_owner);
        if (ret == -1) {
            printf("[good_drain] produce rejected for payload='%s' -> freeing payload\n", msg->payload);
            free(msg->payload);
        } else {
            printf("[good_drain] produce succeeded for payload='%s' (took_owner=%d)\n", msg->payload, took_owner);
            if (!took_owner) free(msg->payload);
            else printf("[good_drain] library took ownership; not freeing payload\n");
        }
        if (msg->auth_token) free(msg->auth_token);
        free(msg);
    }
    printf("[good_drain] finished\n");
}

void bad_drain(int simulate_reject, int simulate_take_owner, int attempt_lock_inside_produce)
{
    printf("[bad_drain] starting bad drain (holds lock while producing)\n");
    while (1) {
        pthread_mutex_lock(&ingress_buf.lock);
        if (ingress_buf.count == 0) {
            pthread_mutex_unlock(&ingress_buf.lock);
            break;
        }
        msg_t *msg = ingress_buf.data[ingress_buf.head];
        ingress_buf.data[ingress_buf.head] = NULL;
        ingress_buf.head = (ingress_buf.head + 1) % MAX_QUEUE_SIZE;
        ingress_buf.count--;

        int took_owner = 0;
        int ret = mock_produce(msg, simulate_reject, simulate_take_owner, attempt_lock_inside_produce, &took_owner);
        if (ret == -1) {
            printf("[bad_drain] produce rejected for payload='%s' -> freeing payload\n", msg->payload);
            free(msg->payload);
        } else {
            printf("[bad_drain] produce succeeded for payload='%s' (took_owner=%d)\n", msg->payload, took_owner);
            if (!took_owner) free(msg->payload);
        }
        if (msg->auth_token) free(msg->auth_token);
        free(msg);
        pthread_mutex_unlock(&ingress_buf.lock);
    }
    printf("[bad_drain] finished\n");
}

void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s [good|reject|bad]\n", prog);
}

int main(int argc, char **argv)
{
    if (argc < 2) { print_usage(argv[0]); return 1; }

    pthread_mutex_init(&ingress_buf.lock, NULL);
    ingress_buf.head = ingress_buf.tail = ingress_buf.count = 0;

    for (int i = 0; i < NUM_MSGS; ++i) {
        char buf[64]; snprintf(buf, sizeof(buf), "message-%d", i);
        enqueue_msg(buf);
    }

    if (strcmp(argv[1], "good") == 0) {
        good_drain(0, 1, 0);
    } else if (strcmp(argv[1], "reject") == 0) {
        good_drain(1, 0, 0);
    } else if (strcmp(argv[1], "bad") == 0) {
        pthread_t t;
        struct { int a,b,c; } args = {0,0,1};
        if (pthread_create(&t, NULL, (void *(*)(void *)) bad_drain, &args) != 0) {
            perror("pthread_create"); return 1; }
        sleep(2);
        printf("[main] If bad_drain hasn't completed by now, likely deadlock occurred.\n");
        printf("[main] Exiting (detached thread may remain blocked).\n");
    } else {
        print_usage(argv[0]); return 1;
    }

    pthread_mutex_destroy(&ingress_buf.lock);
    return 0;
}
