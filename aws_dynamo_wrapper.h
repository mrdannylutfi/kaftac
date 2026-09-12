#ifndef AWS_DYNAMO_WRAPPER_H
#define AWS_DYNAMO_WRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

// Initializes the underlying global AWS C++ SDK execution environment
void init_aws_sdk(void);

// Shuts down and cleans up AWS SDK memory heaps
void shutdown_aws_sdk(void);

// Queries DynamoDB for a record. Writes a JSON string into dest_buffer.
// Returns: 0 on success, -1 on transient network retry, -2 on permanent item failure
int query_dynamodb_metadata(const char* message_id, char* dest_buffer, size_t dest_len);

#ifdef __cplusplus
}
#endif

#endif
