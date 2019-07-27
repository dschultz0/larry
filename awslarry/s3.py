import boto3
import botocore
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from io import StringIO, BytesIO
import os
import json
import awslarry.sts as sts
import uuid
import urllib.request


# Retrieves S3 objects
def get_object(bucket=None, key=None, uri=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    return boto3.resource('s3').Bucket(bucket).Object(key=key).get()


def read_object(bucket=None, key=None, uri=None):
    return get_object(bucket, key, uri)['Body'].read()


def read_dict(bucket=None, key=None, uri=None):
    return json.loads(read_object(bucket, key, uri).decode('utf-8'))


def read_text(bucket=None, key=None, uri=None):
    return read_object(bucket, key, uri).decode('utf-8')


def read_list_of_dict(bucket=None, key=None, uri=None):
    obj = read_object(bucket, key, uri)
    lines = obj.decode('utf-8').split('\n')
    records = []
    for line in lines:
        if len(line) > 0:
            record = json.loads(line)
            records.append(record)
    return records


def read_image(bucket=None, key=None, uri=None):
    try:
        from Pillow import Image
        return Image.open(BytesIO(read_object(bucket, key, uri)))
    except ImportError as e:
        # We'll simply raise the ImportError to let the developer know this requires Pillow to function
        raise e


def write(body, bucket=None, key=None, uri=None, acl=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    obj = boto3.resource('s3').Bucket(bucket).Object(key=key)
    if acl:
        obj.put(Body=body, ACL=acl)
    else:
        obj.put(Body=body)
    return compose_uri(bucket, key)


def write_object(value, bucket=None, key=None, uri=None, acl=None, temp_prefix=None):
    if temp_prefix:
        bucket = _temp_bucket()
        key = temp_prefix + str(uuid.uuid4())
    if type(value) is dict:
        return write(json.dumps(value), bucket, key, uri, acl)
    elif type(value) is str:
        return write(value, bucket, key, uri, acl)
    elif type(value) is list:
        buff = StringIO()
        for row in value:
            buff.write(json.dumps(row) + '\n')
        return write(buff.getvalue(), bucket, key, uri, acl)
    elif value is None:
        return write('', bucket, key, uri, acl)
    else:
        return write(value, bucket, key, uri, acl)


def write_as_csv(rows, bucket=None, key=None, uri=None, acl=None, delimiter=',', columns=None, headers=None):
    def _array_to_string(_row, _delimiter):
        _line = ''
        for x, col in enumerate(_row):
            if x == 0:
                _line = str(col)
            else:
                _line = _line + _delimiter + str(col)
        return _line

    if len(rows) == 0:
        return write('', bucket, key, uri, acl)
    else:
        buff = StringIO()
        if type(rows[0]) is list:
            for row in rows:
                buff.write(_array_to_string(row, delimiter) + "\n")
        elif type(rows[0]) is dict:
            if columns:
                keys = columns
            else:
                keys = rows[0].keys()
            if headers:
                buff.write(_array_to_string(headers, delimiter) + "\n")
            else:
                buff.write(_array_to_string(keys, delimiter) + "\n")
            for row in rows:
                line = ''
                for i, k in enumerate(keys):
                    value = '' if row.get(k) is None else str(row.get(k))
                    if i == 0:
                        line = value
                    else:
                        line = line + "," + value
                buff.write(line + "\n")
        elif type(rows[0]) is str:
            buff.writelines(rows)
        else:
            raise Exception('Invalid input')
        return write(buff.getvalue(), bucket, key, uri, acl)


def write_image(image, image_format, bucket=None, key=None, uri=None):
    buff = BytesIO()
    image.save(buff, image_format)
    buff.seek(0)
    return write(buff, bucket, key, uri)


def rename_object(old_bucket_name, old_key, new_bucket_name, new_key):
    s3 = boto3.resource('s3')
    copy_source = {
        'Bucket': old_bucket_name,
        'Key': old_key
    }
    s3.meta.client.copy(copy_source, new_bucket_name, new_key)
    s3.meta.client.delete_object(Bucket=old_bucket_name, Key=old_key)


def object_exists(bucket=None, key=None, uri=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    s3 = boto3.resource('s3')
    try:
        s3.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    return True


def find_largest_common_prefix(list_of_tuples):
    prefix, z = list_of_tuples[0]
    for key, z in list_of_tuples:
        while key[:len(prefix)] != prefix and len(prefix) > 0:
            prefix = prefix[:-1]
    return prefix


def find_keys_not_present(bucket, key_tuples=None, uri_tuples=None):
    if uri_tuples:
        key_tuples = []
        for uri, z in uri_tuples:
            b, key = decompose_uri(uri)
            key_tuples.append((key, z))
    prefix = find_largest_common_prefix(key_tuples)

    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)
    keys = []
    for obj in bucket_obj.objects.filter(Prefix=prefix):
        keys.append(obj.key)

    not_found = []
    for key, z in key_tuples:
        if key not in keys:
            not_found.append((key, z))
    return not_found


# Functions for breaking down an s3 path
def decompose_uri(uri):
    bucket_name = get_bucket_name(uri)
    return bucket_name, get_bucket_key(bucket_name, uri)


def get_bucket_name(uri):
    return uri.split('/')[2]


def get_bucket_key(bucket_name, uri):
    pos = uri.find(bucket_name) + len(bucket_name) + 1
    return uri[pos:]


def compose_uri(bucket, key):
    return "s3://{}/{}".format(bucket, key)


def list_objects(bucket=None, prefix=None, uri=None):
    if uri:
        (bucket, prefix) = decompose_uri(uri)
    client = boto3.client('s3')
    objects = []
    paginator = client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for obj in page['Contents']:
            if obj['Size'] > 0:
                objects.append(obj['Key'])
    return objects


def fetch(url, bucket=None, key=None, uri=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    try:
        with urllib.request.urlopen(url) as response:
            return write_object(response.read(), bucket=bucket, key=key)
    except Exception as e:
        print('Failed to retrieve {} due to {}'.format(url, e))


def download(directory, bucket=None, key=None, uri=None, use_threads=True):
    if uri:
        (bucket, key) = decompose_uri(uri)
    s3 = boto3.resource('s3')
    config = TransferConfig(use_threads=use_threads)
    s3_object_local = os.path.join(directory, key.split('/')[-1])
    try:
        s3.Bucket(bucket).download_file(key, s3_object_local, Config=config)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return s3_object_local


def download_to_temp(bucket=None, key=None, uri=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    temp_dir = create_temp_dir()
    file = os.path.join(temp_dir, key.split('/')[-1])
    if not os.path.isfile(file):
        print('starting download')
        download(temp_dir, bucket, key, use_threads=True)
        print('download complete')
    return file


# Create a temp folder
def create_temp_dir():
    _temp_dir = os.getcwd() + "/temp"
    if not os.path.isdir(_temp_dir):
        os.makedirs(_temp_dir)
    return _temp_dir


# Makes an object public, returns the url of the object
def make_public(bucket=None, key=None, uri=None):
    if uri:
        (bucket, key) = decompose_uri(uri)
    s3 = boto3.resource('s3')
    s3.meta.client.put_object_acl(Bucket=bucket, Key=key, ACL='public-read')
    return 'https://{}.s3.amazonaws.com/{}'.format(bucket, key)


def _temp_bucket():
    s3 = boto3.resource('s3')
    bucket = '{}-awslarry'.format(sts.account_id())
    try:
        s3.meta.client.head_bucket(Bucket=bucket)
    except ClientError:
        # Attempt to create the bucket anyway in case the account doesn't have permissions to perform head requests
        try:
            bucket_obj = s3.create_bucket(
                Bucket=bucket,
                ACL='private',
                CreateBucketConfiguration={'LocationConstraint': boto3.session.Session().region_name})
            bucket_obj.wait_until_exists()
        except ClientError:
            print('Made unsuccessful create bucket request')
    return bucket
