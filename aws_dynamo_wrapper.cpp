#include "aws_dynamo_wrapper.h"
#include <aws/core/Aws.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <iostream>

static Aws::SDKOptions options;
static Aws::DynamoDB::DynamoDBClient* ddb_client = nullptr;

extern "C" {

void init_aws_sdk(void) {
    Aws::InitAPI(options);
    Aws::Client::ClientConfiguration clientConfig;
    // Essential for low-latency thread scale: keep-alive prevents socket recreation
    clientConfig.connectTimeoutMs = 1000;
    clientConfig.requestTimeoutMs = 2000;
    
    ddb_client = new Aws::DynamoDB::DynamoDBClient(clientConfig);
}

void shutdown_aws_sdk(void) {
    delete ddb_client;
    Aws::ShutdownAPI(options);
}

int query_dynamodb_metadata(const char* message_id, char* dest_buffer, size_t dest_len) {
    if (!ddb_client || !message_id || !dest_buffer) return -2;

    Aws::DynamoDB::Model::GetItemRequest request;
    request.SetTableName("YourProductionMetadataTable");
    
    // Construct the primary partition key dictionary search map
    Aws::DynamoDB::Model::AttributeValue pk;
    pk.SetS(message_id);
    request.AddKey("MessageID", pk);

    // Perform synchronous database call safely isolated within this thread execution
    auto outcome = ddb_client->GetItem(request);

    if (outcome.IsSuccess()) {
        const auto& item = outcome.GetResult().GetItem();
        if (item.empty()) {
            return -2; // Document does not exist (Permanent failure boundary condition)
        }

        // Find the targeted property attribute value
        auto it = item.find("MetadataPayload");
        if (it != item.end()) {
            std::string text = it->second.GetS();
            if (text.length() < dest_len) {
                strncpy(dest_buffer, text.c_str(), dest_len);
                return 0; // Success
            }
        }
        return -2; // Buffer overflow safety mismatch
    } else {
        // Evaluate failure contexts
        const auto& error = outcome.GetError();
        auto err_type = error.GetErrorType();
        
        if (err_type == Aws::DynamoDB::DynamoDBErrors::THROTTLING || 
            err_type == Aws::DynamoDB::DynamoDBErrors::PROVISIONED_THROUGHPUT_EXCEEDED ||
            error.ShouldRetry()) {
            return -1; // Transient network/capacity fault: triggers safe retry backoff in C thread
        }
        return -2; // Access denied or invalid table geometry (Fatal execution exception)
    }
}

}
