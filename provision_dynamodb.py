import boto3
from botocore.exceptions import ClientError

def provision_table():
    # Connect directly to the local mock DynamoDB instance inside the Docker network
    ddb = boto3.resource(
        'dynamodb',
        endpoint_url='http://localhost:8000',
        region_name='us-east-1',
        aws_access_key_id='mock_key',
        aws_secret_access_key='mock_secret'
    )
    
    table_name = "YourProductionMetadataTable"
    
    try:
        print(f"Creating {table_name}...")
        table = ddb.create_table(
            TableName=table_name,
            # Define structural data types used by keys/indexes (S = String)
            AttributeDefinitions=[
                {'AttributeName': 'MessageID', 'AttributeType': 'S'},       # Core Partition Key
                {'AttributeName': 'ClientSessionID', 'AttributeType': 'S'}, # Target Index Key
            ],
            KeySchema=[
                {'AttributeName': 'MessageID', 'KeyType': 'HASH'}           # Primary Key
            ],
            # Global Secondary Index: Allows Thread 4 to query updates by Client Session
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'ClientSessionIndex',
                    'KeySchema': [
                        {'AttributeName': 'ClientSessionID', 'KeyType': 'HASH'}
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL' # Returns all attributes to prevent extra database reads
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 20,
                'WriteCapacityUnits': 20
            }
        )
        # Block script until infrastructure table is fully activated
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Successfully provisioned database table: {table.table_status}")
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("Table already exists. Ready for engine execution testing.")
        else:
            print(f"Catastrophic Provisioning Fault: {e}")

if __name__ == '__main__':
    provision_table()
