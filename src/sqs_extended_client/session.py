from boto3 import resource
import botoinator
from concurrent.futures import ThreadPoolExecutor
from json import dumps as jsondumps, loads as jsonloads
import re
from uuid import uuid4


DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144
MESSAGE_POINTER_CLASS = 'com.amazon.sqs.javamessaging.MessageS3Pointer'
RECEIPT_HANDLER_MATCHER = re.compile(r"^-\.\.s3BucketName\.\.-(.*)-\.\.s3BucketName\.\.--\.\.s3Key\.\.-(.*)-\.\.s3Key\.\.-(.*)")
RESERVED_ATTRIBUTE_NAME = 'SQSLargePayloadSize'
S3_BUCKET_NAME_MARKER = "-..s3BucketName..-"
S3_KEY_MARKER = "-..s3Key..-"


def _delete_large_payload_support(self):
  if hasattr(self, '__s3_bucket_name'):
    del self.__s3_bucket_name


def _get_large_payload_support(self):
  return getattr(self, '__s3_bucket_name', None)


def _set_large_payload_support(self, s3_bucket_name):
  assert isinstance(s3_bucket_name, str) or not s3_bucket_name
  setattr(self, '__s3_bucket_name', s3_bucket_name)


def _delete_messsage_size_threshold(self):
  setattr(self, '__message_size_threshold', DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _get_message_size_threshold(self):
  return getattr(self, '__message_size_threshold', DEFAULT_MESSAGE_SIZE_THRESHOLD)


def _set_message_size_threshold(self, message_size_threshold):
  assert isinstance(message_size_threshold, int) and 0 <= message_size_threshold <= DEFAULT_MESSAGE_SIZE_THRESHOLD
  setattr(self, '__message_size_threshold', message_size_threshold)


def _delete_always_through_s3(self):
  setattr(self, '__always_through_s3', False)


def _get_always_through_s3(self):
  return getattr(self, '__always_through_s3', False)


def _set_always_through_s3(self, always_through_s3):
  assert isinstance(always_through_s3, bool)
  assert not always_through_s3 or (always_through_s3 and self.large_payload_support)
  setattr(self, '__always_through_s3', always_through_s3)


def _delete_s3(self):
  if hasattr(self, '__s3'):
    del self.__s3


def _get_s3(self):
  s3 = getattr(self, '__s3', None)
  if not s3:
    s3 = resource('s3')
    self.s3 = s3
  return s3


def _set_s3(self, s3):
  setattr(self, '__s3', s3)


def _set_body(self, body):
  assert isinstance(body, str)
  self.meta.data['Body'] = body


def _set_message_attributes(self, message_attributes):
  assert isinstance(message_attributes, dict)
  self.meta.data['MessageAttributes'] = message_attributes


def _set_receipt_handle(self, receipt_handle):
  assert isinstance(receipt_handle, str)
  setattr(self, '_receipt_handle', receipt_handle)
  self.meta.data['ReceiptHandle'] = receipt_handle


def _Queue(self):
  return getattr(self, '_origin_queue')


def _is_large_message(self, attributes, encoded_body):
  total = 0
  for key, value in attributes.items():
    total = total + len(key.encode())
    if 'DataType' in value:
      total = total + len(value['DataType'].encode())
    if 'StringValue' in value:
      total = total + len(value['StringValue'].encode())
    if 'BinaryValue' in value:
      total = total + len(value['BinaryValue'])
  total = total + len(encoded_body)
  return self.message_size_threshold < total


def _create_s3_put_object_params(self, encoded_body, queue_url):
  return {
    'ACL': 'private',
    'Body': encoded_body,
    'ContentLength': len(encoded_body)
  }


def _store_in_s3(self, queue_url, message_attributes, message_body):
  encoded_body = message_body.encode()
  if self.large_payload_support and (self.always_through_s3 or self._is_large_message(message_attributes, encoded_body)):
    message_attributes[RESERVED_ATTRIBUTE_NAME] = {}
    message_attributes[RESERVED_ATTRIBUTE_NAME]['DataType'] = 'Number'
    message_attributes[RESERVED_ATTRIBUTE_NAME]['StringValue'] = str(len(encoded_body))
    s3_key = str(uuid4())
    self.s3.Object(self.large_payload_support, s3_key).put(**self._create_s3_put_object_params(encoded_body, queue_url))
    message_body = jsondumps([MESSAGE_POINTER_CLASS, {'s3BucketName': self.large_payload_support, 's3Key': s3_key}], separators=(',', ':'))
  return message_attributes, message_body


def _retrieve_from_s3(self, message_attributes, message_body, receipt_handle):
  if (message_attributes.pop(RESERVED_ATTRIBUTE_NAME, None)):
    s3_message_body = jsonloads(message_body)
    if isinstance(s3_message_body, list) and len(s3_message_body) == 2 and s3_message_body[0] == MESSAGE_POINTER_CLASS:
      payload = jsonloads(message_body)[1]
      s3_bucket_name = payload['s3BucketName']
      s3_key = payload['s3Key']
      message_body = self.s3.Object(s3_bucket_name, s3_key).get()['Body'].read().decode()
      receipt_handle_params = {
        'S3_BUCKET_NAME_MARKER': S3_BUCKET_NAME_MARKER,
        'bucket': s3_bucket_name,
        'S3_KEY_MARKER': S3_KEY_MARKER,
        'key': s3_key,
        'receipt_handle': receipt_handle
      }
      receipt_handle = '{S3_BUCKET_NAME_MARKER}{bucket}{S3_BUCKET_NAME_MARKER}{S3_KEY_MARKER}{key}{S3_KEY_MARKER}{receipt_handle}'.format(**receipt_handle_params)
  return message_attributes, message_body, receipt_handle


def _add_custom_attributes(class_attributes):
  class_attributes['large_payload_support'] = property(
    _get_large_payload_support, 
    _set_large_payload_support, 
    _delete_large_payload_support
  )
  class_attributes['message_size_threshold'] = property(
    _get_message_size_threshold, 
    _set_message_size_threshold, 
    _delete_messsage_size_threshold
  )
  class_attributes['always_through_s3'] = property(
    _get_always_through_s3,
    _set_always_through_s3,
    _delete_always_through_s3
  )
  class_attributes['s3'] = property(
    _get_s3,
    _set_s3,
    _delete_s3
  )
  class_attributes['_create_s3_put_object_params'] = _create_s3_put_object_params
  class_attributes['_is_large_message'] = _is_large_message
  class_attributes['_retrieve_from_s3'] = _retrieve_from_s3
  class_attributes['_store_in_s3'] = _store_in_s3


def _add_client_custom_attributes(base_classes, **kwargs):
  _add_custom_attributes(kwargs['class_attributes'])


def _add_message_resource_custom_attributes(class_attributes, **kwargs):
  body_property = class_attributes['body']
  class_attributes['body'] = property(body_property.fget, _set_body, body_property.fdel)
  message_attributes_property = class_attributes['message_attributes']
  class_attributes['message_attributes'] = property(message_attributes_property.fget, _set_message_attributes, message_attributes_property.fdel)
  receipt_handle_property = class_attributes['receipt_handle']
  class_attributes['receipt_handle'] = property(receipt_handle_property.fget, _set_receipt_handle, receipt_handle_property.fdel)
  class_attributes['Queue'] = _Queue


def _add_queue_resource_custom_attributes(class_attributes, **kwargs):
  _add_custom_attributes(class_attributes)  


def _delete_decorator(func):
  def _delete(*args, **kwargs):
    match = RECEIPT_HANDLER_MATCHER.match(args[0].receipt_handle)
    if match:
      args[0].Queue().s3.Bucket(match.group(1)).delete_objects(
        Delete={
          'Objects': [
            {
              'Key': match.group(2)
            }
          ],
          'Quiet': True
        }
      )
      args[0].receipt_handle = match.group(3)
    return func(*args, **kwargs)
  return _delete


def _delete_message_decorator(func):
  def _delete_message(*args, **kwargs):
    match = RECEIPT_HANDLER_MATCHER.match(kwargs['ReceiptHandle'])
    if match:
      args[0].s3.Bucket(match.group(1)).delete_objects(
        Delete={
          'Objects': [
            {
              'Key': match.group(2)
            }
          ],
          'Quiet': True
        }
      )
      kwargs['ReceiptHandle'] = match.group(3)
    return func(*args, **kwargs)
  return _delete_message


def _delete_message_batch_decorator(func):
  def _delete_message_batch(*args, **kwargs):
    bucket_objects = {}
    for entry in kwargs['Entries']:
      match = RECEIPT_HANDLER_MATCHER.match(entry['ReceiptHandle'])
      if match:
        if match.group(1) not in bucket_objects:
          bucket_objects[match.group(1)] = []
        bucket_objects[match.group(1)].append({'Key': match.group(2)})
        entry['ReceiptHandle'] = match.group(3)
    for bucket, objects in bucket_objects.items():
      args[0].s3.Bucket(bucket).delete_objects(
        Delete={
          'Objects': objects,
          'Quiet': True
        }
      )
    return func(*args, **kwargs)
  return _delete_message_batch


def _send_message_decorator(func):
  def _send_message(*args, **kwargs):
    queue_url = kwargs.get('QueueUrl') if 'QueueUrl' in kwargs else args[0].url
    kwargs['MessageAttributes'], kwargs['MessageBody'] = args[0]._store_in_s3(queue_url, kwargs.get('MessageAttributes', {}), kwargs['MessageBody'])
    return func(*args, **kwargs)
  return _send_message


def _send_message_batch_decorator(func):
  def _send_message_batch(*args, **kwargs):
    entries = kwargs['Entries']
    queue_url = kwargs.get('QueueUrl') if 'QueueUrl' in kwargs else args[0].url
    iterables = [ [ None for _ in range(len(entries)) ] for _ in range(3) ]
    for index in range(len(entries)):
      iterables[0][index] = queue_url
      iterables[1][index] = entries[index].get('MessageAttributes', {})
      iterables[2][index] = entries[index]['MessageBody']
    with ThreadPoolExecutor(max_workers=len(entries)) as executor:
      store_results = list(executor.map(args[0]._store_in_s3, *iterables))
    for index in range(len(entries)):
      entries[index]['MessageAttributes'] = store_results[index][0]
      entries[index]['MessageBody'] = store_results[index][1]
    return func(*args, **kwargs)
  return _send_message_batch


def _receive_message_decorator(func):
  def _receive_message(*args, **kwargs):
    if 'MessageAttributeNames' not in kwargs:
      kwargs['MessageAttributeNames'] = []
    assert isinstance(kwargs['MessageAttributeNames'], list), 'MessageAttributeNames must be a list'
    if RESERVED_ATTRIBUTE_NAME not in kwargs['MessageAttributeNames'] and \
        not ('All' in kwargs['MessageAttributeNames'] or '.*' in kwargs['MessageAttributeNames']):
      kwargs['MessageAttributeNames'].append(RESERVED_ATTRIBUTE_NAME)
    response = func(*args, **kwargs)
    messages = response.get('Messages', [])
    if messages:
      iterables = [ [ None for _ in range(len(messages)) ] for _ in range(3) ]
      for index in range(len(messages)):
        iterables[0][index] = messages[index].get('MessageAttributes', {})
        iterables[1][index] = messages[index]['Body']
        iterables[2][index] = messages[index]['ReceiptHandle']
      with ThreadPoolExecutor(max_workers=len(messages)) as executor:
        retrieve_results = list(executor.map(args[0]._retrieve_from_s3, *iterables))
      for index in range(len(messages)):
        messages[index]['MessageAttributes'] = retrieve_results[index][0]
        messages[index]['Body'] = retrieve_results[index][1]
        messages[index]['ReceiptHandle'] = retrieve_results[index][2]
    return response
  return _receive_message


def _receive_messages_decorator(func):
  def _receive_messages(*args, **kwargs):
    if 'MessageAttributeNames' not in kwargs:
      kwargs['MessageAttributeNames'] = []
    assert isinstance(kwargs['MessageAttributeNames'], list), 'MessageAttributeNames must be a list'
    if RESERVED_ATTRIBUTE_NAME not in kwargs['MessageAttributeNames'] and \
        not ('All' in kwargs['MessageAttributeNames'] or '.*' in kwargs['MessageAttributeNames']):
      kwargs['MessageAttributeNames'].append(RESERVED_ATTRIBUTE_NAME)
    messages = func(*args, **kwargs)
    if messages:
      iterables = [ [ None for _ in range(len(messages)) ] for _ in range(3) ]
      for index in range(len(messages)):
        iterables[0][index] = messages[index].message_attributes or {}
        iterables[1][index] = messages[index].body
        iterables[2][index] = messages[index].receipt_handle
      with ThreadPoolExecutor(max_workers=len(messages)) as executor:
        retrieve_results = list(executor.map(args[0]._retrieve_from_s3, *iterables))
      for index in range(len(messages)):
        messages[index].message_attributes = retrieve_results[index][0]
        messages[index].body = retrieve_results[index][1]
        messages[index].receipt_handle = retrieve_results[index][2]
        setattr(messages[index], '_origin_queue', args[0])
    return messages
  return _receive_messages

 
class SQSExtendedClientSession(botoinator.session.DecoratedSession):


  def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 aws_session_token=None, region_name=None,
                 botocore_session=None, profile_name=None):
    super().__init__(
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key,
      aws_session_token=aws_session_token,
      region_name=region_name,
      botocore_session=botocore_session,
      profile_name=profile_name
    )
    self.events.register('creating-client-class.sqs', _add_client_custom_attributes)
    self.events.register('creating-resource-class.sqs.Queue', _add_queue_resource_custom_attributes)
    self.events.register('creating-resource-class.sqs.Message', _add_message_resource_custom_attributes)
    self.register_client_decorator('sqs', 'delete_message', _delete_message_decorator)
    self.register_client_decorator('sqs', 'delete_message_batch', _delete_message_batch_decorator)
    self.register_client_decorator('sqs', 'send_message', _send_message_decorator)
    self.register_client_decorator('sqs', 'send_message_batch', _send_message_batch_decorator)
    self.register_client_decorator('sqs', 'receive_message', _receive_message_decorator)
    self.register_resource_decorator('sqs', 'Queue', 'delete_messages', _delete_message_batch_decorator)
    self.register_resource_decorator('sqs', 'Queue', 'send_message', _send_message_decorator)
    self.register_resource_decorator('sqs', 'Queue', 'send_messages', _send_message_batch_decorator)
    self.register_resource_decorator('sqs', 'Queue', 'receive_messages', _receive_messages_decorator)
    self.register_resource_decorator('sqs', 'Message', 'delete', _delete_decorator)
