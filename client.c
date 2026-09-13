void* client_egress_thread(void *arg) {
    int local_epoll_out_fd = *(int*)arg;

    while (1) {
        egress_msg_t *msg = dequeue_egress();

        // Direct write optimization attempt
        ssize_t sent_bytes = send(msg->client_fd, msg->payload, msg->length, MSG_NOSIGNAL);

        if (sent_bytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // The client TCP window is temporarily saturated. 
                // Register interest in EPOLLOUT to send when ready.
                struct epoll_event ev;
                ev.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
                ev.data.ptr = msg; // Attach payload object reference directly to event tracking
                epoll_ctl(local_epoll_out_fd, EPOLL_CTL_MOD, msg->client_fd, &ev);
                continue; // Skip manual free; event tracking loop will clean it up later
            } else {
                // Catastrophic socket failure (Client abruptly disconnected)
                close(msg->client_fd);
                free(msg->payload);
                free(msg);
            }
        } else if ((size_t)sent_bytes < msg->length) {
            // Partial send handling: Adjust pointer layout and defer the remaining payload to an `epoll` loop
            size_t remaining = msg->length - sent_bytes;
            char *remainder_buffer = malloc(remaining);
            memcpy(remainder_buffer, msg->payload + sent_bytes, remaining);
            
            free(msg->payload);
            msg->payload = remainder_buffer;
            msg->length = remaining;

            struct epoll_event ev;
            ev.events = EPOLLOUT | EPOLLET | EPOLLONESHOT;
            ev.data.ptr = msg;
            epoll_ctl(local_epoll_out_fd, EPOLL_CTL_MOD, msg->client_fd, &ev);
        } else {
            // Perfect clean transmit cycle complete
            free(msg->payload);
            free(msg);
        }
    }
    return NULL;
}
