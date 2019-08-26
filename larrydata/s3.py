import botocore
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from io import StringIO, BytesIO
import os
import json
import larrydata.sts as sts
import uuid
import urllib.request
import larrydata


# Local S3 client and resource objects
_client = None
_resource = None


def client():
    """
    Helper function to retrieve an S3 client.
    :return: Boto3 S3 client
    """
    global _client
    if _client is None:
        _client = larrydata.session().client('s3')
    return _client


def resource():
    """
    Helper function to retrieve an S3 resource
    :return: Boto3 S3 resource
    """
    global _resource
    if _resource is None:
        _resource = larrydata.session().resource('s3')
    return _resource


def delete_object(bucket=None, key=None, uri=None):
    """
    Deletes the object defined by the bucket/key pair (or uri).
    :param bucket: The S3 bucket
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: Dict containing the boto3 response
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    return resource().Bucket(bucket).Object(key=key).delete()


def get_object(bucket=None, key=None, uri=None):
    """
    Performs a 'get' of the object defined by the bucket/key pair (or uri).
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: Dict containing the Body of the object and associated attributes
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    return resource().Bucket(bucket).Object(key=key).get()


def read_object(bucket=None, key=None, uri=None, amt=None):
    """
    Performs a 'get' of the object defined by the bucket/key pair (or uri)
    and then performs a 'read' of the Body of that object.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param amt: The max amount of bytes to read from the object. All data is read if omitted.
    :return: The bytes contained in the object
    """
    return get_object(bucket, key, uri)['Body'].read(amt)


def read_dict(bucket=None, key=None, uri=None, encoding='utf-8'):
    """
    Reads in the s3 object defined by the bucket/key pair (or uri) and
    loads the json contents into a dict.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :return: A dict representation of the json contained in the object
    """
    return json.loads(read_object(bucket, key, uri).decode(encoding))


def read_str(bucket=None, key=None, uri=None, encoding='utf-8'):
    """
    Reads in the s3 object defined by the bucket/key pair (or uri) and
    decodes it to text.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :return: The contents of the object as a string
    """
    return read_object(bucket, key, uri).decode(encoding)


def read_list_of_dict(bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    """
    Reads in the s3 object defined by the bucket/key pair (or uri) and
    loads the JSON Lines data into a list of dict objects.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :return: The contents of the object as a list of dict objects
    """
    obj = read_object(bucket, key, uri)
    lines = obj.decode(encoding).split(newline)
    records = []
    for line in lines:
        if len(line) > 0:
            record = json.loads(line)
            records.append(record)
    return records


def read_list_of_str(bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    """
    Reads in the s3 object defined by the bucket/key pair (or uri) and
    loads the JSON Lines data into a list of dict objects.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :return: The contents of the object as a list of dict objects
    """
    obj = read_object(bucket, key, uri)
    lines = obj.decode(encoding).split(newline)
    records = []
    for line in lines:
        if len(line) > 0:
            records.append(line)
    return records


def read_pillow_image(bucket=None, key=None, uri=None):
    """
    Reads in the s3 object defined by the bucket/key pair (or uri) and
    loads it into a Pillow image object
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The contents of the object as a Pillow image object
    """
    try:
        from PIL import Image
        return Image.open(BytesIO(read_object(bucket, key, uri)))
    except ImportError as e:
        # Simply raise the ImportError to let the user know this requires Pillow to function
        raise e


def write(body, bucket=None, key=None, uri=None, acl=None, content_type=None):
    """
    Write an object to the bucket/key pair (or uri).
    :param body: Data to write
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param content_type: A standard MIME type describing the format of the object data
    :return: The URI of the object written to S3
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    obj = resource().Bucket(bucket).Object(key=key)
    if acl and content_type:
        obj.put(Body=body, ACL=acl, ContentType=content_type)
    elif acl:
        obj.put(Body=body, ACL=acl)
    elif content_type:
        obj.put(Body=body, ContentType=content_type)
    else:
        obj.put(Body=body)
    return compose_uri(bucket, key)


def write_temp_object(value, prefix, acl=None):
    """
    Write an object to a temp bucket with a unique UUID.
    :param value: Object to write to S3
    :param prefix: Prefix to attach ahead of the UUID as the key
    :param acl: The canned ACL to apply to the object
    :return: The URI of the object written to S3
    """
    bucket = _temp_bucket()
    key = prefix + str(uuid.uuid4())
    return write_object(value, bucket=bucket, key=key, acl=acl)


def write_object(value, bucket=None, key=None, uri=None, acl=None, newline='\n'):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.
    :param value: Object to write to S3
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param newline: Character(s) to use as a newline for list objects
    :return: The URI of the object written to S3
    """
    if type(value) is dict:
        return write(json.dumps(value), bucket, key, uri, acl)
    elif type(value) is str:
        return write(value, bucket, key, uri, acl)
    elif type(value) is list:
        buff = StringIO()
        for row in value:
            if type(row) is dict:
                buff.write(json.dumps(row) + newline)
            else:
                buff.write(str(row) + newline)
        return write(buff.getvalue(), bucket, key, uri, acl)
    elif value is None:
        return write('', bucket, key, uri, acl)
    else:
        return write(value, bucket, key, uri, acl)


def write_pillow_image(image, image_format, bucket=None, key=None, uri=None):
    """
    Write an image to the bucket/key pair (or uri).
    :param image: The image to write to S3
    :param image_format: The format of the image (png, jpeg, etc)
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URI of the object written to S3
    """
    buff = BytesIO()
    image.save(buff, image_format)
    buff.seek(0)
    return write(buff, bucket, key, uri)


def write_as_csv(rows, bucket=None, key=None, uri=None, acl=None, delimiter=',', columns=None, headers=None):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.
    :param rows: List of data to write, rows can be of type list, dict or str
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param delimiter: Column delimiter to use, ',' by default
    :param columns: The columns to write out from the source rows, dict keys or list indexes
    :param headers: Headers to add to the output
    :return: The URI of the object written to S3
    """
    def _array_to_string(_row, _delimiter, _indices=None):
        if _indices is None:
            _indices = range(len(_row))
        _line = ''
        for x in _indices:
            _line = str(row[x]) if x==0 else _line + _delimiter + str(row[x])
        return _line

    buff = StringIO()

    # empty
    if rows is None or len(rows) == 0:
        if headers:
            buff.write(_array_to_string(headers, delimiter) + "\n")
        buff.write('')

    # list
    elif type(rows[0]) is list:
        indices = columns if columns else None
        if headers:
            buff.write(_array_to_string(headers, delimiter) + "\n")
        for row in rows:
            buff.write(_array_to_string(row, delimiter, indices) + "\n")

    # dict
    elif type(rows[0]) is dict:
        keys = columns if columns else rows[0].keys()
        buff.write(_array_to_string(headers if headers else keys, delimiter) + "\n")

        for row in rows:
            line = ''
            for i, k in enumerate(keys):
                value = '' if row.get(k) is None else str(row.get(k))
                line = value if i == 0 else line + delimiter + value
            buff.write(line + "\n")

    # string
    elif type(rows[0]) is str:
        buff.writelines(rows)
    else:
        raise Exception('Invalid input')
    return write(buff.getvalue(), bucket, key, uri, acl)


def rename_object(old_bucket_name, old_key, new_bucket_name, new_key):
    """
    Renames an object in S3.
    :param old_bucket_name: Source bucket
    :param old_key: Source key
    :param new_bucket_name: Target bucket
    :param new_key: Target key
    :return: None
    """
    copy_source = {
        'Bucket': old_bucket_name,
        'Key': old_key
    }
    client().copy(copy_source, new_bucket_name, new_key)
    client().delete_object(Bucket=old_bucket_name, Key=old_key)


def object_exists(bucket=None, key=None, uri=None):
    """
    Checks to see if an object with the given bucket/key (or uri) exists.
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: True if the key exists, if not, False
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    try:
        resource().Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    return True


def _find_largest_common_prefix(values):
    """
    Searches through a list of values to find the longest possible common prefix amongst them. Useful for optimizing
    more costly searches. Supports lists of strings or tuples. If tuples are used, the first value is assumed to be
    the value to search on.
    :param values: List of values (strings or tuples containing a string in the first position)
    :return: String prefix common to all values
    """
    if type(values[0]) is tuple:
        prefix, *_ = values[0]
    else:
        prefix = values[0]

    for value in values:
        key = value[0] if type(value) is tuple else value
        while key[:len(prefix)] != prefix and len(prefix) > 0:
            prefix = prefix[:-1]
    return prefix


def find_keys_not_present(bucket, keys=None, uris=None):
    """
    Searches an S3 bucket for a list of keys and returns any that cannot be found.
    :param bucket: The S3 bucket to search
    :param keys: A list of keys to search for (strings or tuples containing a string in the first position)
    :param uris: A list of S3 URIs to search for (strings or tuples containing a string in the first position)
    :return: A list of keys that were not found (strings or tuples based on the input values)
    """

    # If URIs are passed, convert to a list of keys to use for the search
    if uris:
        keys = []
        for value in uris:
            if type(value) is tuple:
                uri, *z = value
                b, key = decompose_uri(uri)
                keys.append(tuple([key])+tuple(z))
            else:
                b, key = decompose_uri(value)
                keys.append(key)

    # Find the longest common prefix to use as the search term
    prefix = _find_largest_common_prefix(keys)

    # Get a list of all keys in the bucket that match the prefix
    bucket_obj = resource().Bucket(bucket)
    all_keys = []
    for obj in bucket_obj.objects.filter(Prefix=prefix):
        all_keys.append(obj.key)

    # Search for any keys that can't be found
    not_found = []
    for value in keys:
        key = value[0] if type(value) is tuple else value
        if key not in all_keys:
            not_found.append(value)
    return not_found


def decompose_uri(uri):
    """
    Decompose an S3 URI into a bucket and key
    :param uri: S3 URI
    :return: Tuple containing a bucket and key
    """
    bucket_name = get_bucket_name(uri)
    return bucket_name, get_bucket_key(bucket_name, uri)


def get_bucket_name(uri):
    """
    Retrieve the bucket portion from an S3 URI
    :param uri: S3 URI
    :return: Bucket name
    """
    return uri.split('/')[2]


def get_bucket_key(bucket_name, uri):
    """
    Retrieves the key portion of an S3 URI
    :param bucket_name: S3 bucket name
    :param uri: S3 URI
    :return: Key value
    """
    pos = uri.find(bucket_name) + len(bucket_name) + 1
    return uri[pos:]


def compose_uri(bucket, key):
    """
    Compose a bucket and key into an S3 URI
    :param bucket: Bucket name
    :param key: Object key
    :return: S3 URI string
    """
    return "s3://{}/{}".format(bucket, key)


def list_objects(bucket=None, prefix=None, uri=None, include_empty_keys=False):
    """
    Returns a list of the object keys in the provided bucket that begin with the provided prefix.
    :param bucket: The S3 bucket to query
    :param prefix: The key prefix to use in searching the bucket
    :param uri: An s3:// path containing the bucket and prefix
    :param include_empty_keys: True if you want to include keys associated with objects of size=0
    :return: A generator of object keys
    """
    if uri:
        (bucket, prefix) = decompose_uri(uri)
    paginator = client().get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket, 'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for obj in page.get('Contents',[]):
            if obj['Size'] > 0 or include_empty_keys:
                yield obj['Key']


def fetch(url, bucket=None, key=None, uri=None):
    """
    Retrieves the data defined by a URL to an S3 location.
    :param url: URL to retrieve
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URI of the object written to S3
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    try:
        with urllib.request.urlopen(url) as response:
            return write_object(response.read(), bucket=bucket, key=key)
    except Exception as e:
        print('Failed to retrieve {} due to {}'.format(url, e))


def download(directory, bucket=None, key=None, uri=None, use_threads=True):
    """
    Downloads the an S3 object to a directory on the local file system.
    :param directory: The directory to download the object to
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param use_threads: Enables the use_threads transfer config
    :return: Path of the local file
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    config = TransferConfig(use_threads=use_threads)
    s3_object_local = os.path.join(directory, key.split('/')[-1])
    try:
        resource().Bucket(bucket).download_file(key, s3_object_local, Config=config)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return s3_object_local


def download_to_temp(bucket=None, key=None, uri=None):
    """
    Downloads the an S3 object to a temp directory on the local file system.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: Path of the local file
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    temp_dir = _create_temp_dir()
    file = os.path.join(temp_dir, key.split('/')[-1])
    if not os.path.isfile(file):
        print('starting download')
        download(temp_dir, bucket, key, use_threads=True)
        print('download complete')
    return file


def _create_temp_dir():
    """
    Creates a temp directory in the current path.
    :return: The path of the temp directory
    """
    _temp_dir = os.getcwd() + "/temp"
    if not os.path.isdir(_temp_dir):
        os.makedirs(_temp_dir)
    return _temp_dir


def make_public(bucket=None, key=None, uri=None):
    """
    Makes the object defined by the bucket/key pair (or uri) public.
    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URL of the object
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    client().put_object_acl(Bucket=bucket, Key=key, ACL='public-read')
    return get_public_url(bucket=bucket, key=key)


def get_public_url(bucket=None, key=None, uri=None):
    """
    Returns the public URL of an S3 object (assuming it's public).
    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URL of the object
    """
    if uri:
        (bucket, key) = decompose_uri(uri)
    return 'https://{}.s3.amazonaws.com/{}'.format(bucket, key)


def _temp_bucket():
    """
    Create a bucket that will be used as temp storage for larrydata commands.
    The bucket will be created in the region associated with the current session
    using a name based on the current session account id and region.
    :return: The name of the created bucket
    """
    region = boto3.session.Session().region_name
    bucket = '{}-larrydata-{}'.format(sts.account_id(), region)
    try:
        client().head_bucket(Bucket=bucket)
    except ClientError:
        # Attempt to create the bucket anyway in case the account doesn't have permissions to perform head requests
        try:
            bucket_obj = resource().create_bucket(
                Bucket=bucket,
                ACL='private',
                CreateBucketConfiguration={'LocationConstraint': region})
            bucket_obj.wait_until_exists()
        except ClientError as e:
            print('Made unsuccessful create bucket request')
            raise e
    return bucket
