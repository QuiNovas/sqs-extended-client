# sqs-extended-client

### Implements the functionality of [amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib) in Python

## Installation
```
pip install sqs-extended-client
```


## Overview
sqs-extended-client allows for sending large messages through SQS via S3. This is the same mechanism that the Amazon library
[amazon-sqs-java-extended-client-lib](https://github.com/awslabs/amazon-sqs-java-extended-client-lib) provides. This library is
interoperable with that library.

To do this, this library automatically extends the normal boto3 SQS client and Queue resource classes upon import using the [botoinator](https://github.com/QuiNovas/botoinator) library. This allows for further extension or decoration if desired.

# Usage

### Note
The s3 bucket must already exist prior to usage, and be accessible by whatever credentials you have available


### Enabling support for large payloads (>256Kb)

```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
```
Arguments:
* large_payload_support -- the S3 bucket name that will store large messages.

### Enabling support for large payloads (>64K)
```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'
sqs.message_size_threshold = 65536

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
queue.message_size_threshold = 65536
```
Arguments:
* message_size_threshold -- the threshold for storing the message in the large messages bucket. Cannot be less than 0 or greater than 262144

### Enabling support for large payloads for all messages
```python
import boto3
import sqs_extended_client

# Low level client
sqs = boto3.client('sqs')
sqs.large_payload_support = 'my-bucket-name'
sqs.always_through_s3 = True

# boto resource
resource = boto3.resource('sqs')
queue = resource.Queue('queue-url')

# Or
queue = resource.create_queue(QueueName='queue-name')

queue.large_payload_support = 'my-bucket-name'
queue.always_through_s3 = True
```
Arguments:
* always_through_s3 -- if True, then all messages will be serialized to S3.
