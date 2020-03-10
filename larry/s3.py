import boto3
import botocore
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from io import StringIO, BytesIO
import os
import json
from larry import utils
from larry import sts
from larry import types
import uuid
from urllib.request import Request
from urllib import parse
from urllib import request
from zipfile import ZipFile
from collections import Mapping
import inspect
import re

# Local S3 resource object
resource = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()

ACL_PRIVATE = 'private'
ACL_PUBLIC_READ = 'public-read'
ACL_PUBLIC_READ_WRITE = 'public-read-write'
ACL_AUTHENTICATED_READ = 'authenticated-read'
ACL_AWS_EXEC_READ = 'aws-exec-read'
ACL_BUCKET_OWNER_READ = 'bucket-owner-read'
ACL_BUCKET_OWNER_FULL_CONTROL = 'bucket-owner-full-control'

CLASS_STANDARD = 'STANDARD'
CLASS_REDUCED_REDUNDANCY = 'REDUCED_REDUNDANCY'
CLASS_STANDARD_IA = 'STANDARD_IA'
CLASS_ONEZONE_IA = 'ONEZONE_IA'
CLASS_INTELLIGENT_TIERING = 'INTELLIGENT_TIERING'
CLASS_GLACIER = 'GLACIER'
CLASS_DEEP_ARCHIVE = 'DEEP_ARCHIVE'


def set_session(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws__session_token=None,
                region_name=None,
                profile_name=None,
                boto_session=None):
    """
    Sets the boto3 session for this module to use a specified configuration state.
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws__session_token: AWS temporary session token
    :param region_name: Default region when creating new connections
    :param profile_name: The name of a profile to use
    :param boto_session: An existing session to use
    :return: None
    """
    global __session, resource
    __session = boto_session if boto_session is not None else boto3.session.Session(
        **utils.copy_non_null_keys(locals()))
    sts.set_session(boto_session=__session)
    resource = __session.resource('s3')


def __load_resource(func):
    def decorated(*args, **kwargs):
        if 's3_resource' not in kwargs:
            kwargs['s3_resource'] = resource
        return func(*args, **kwargs)

    return decorated


def __decompose_location(require_bucket=True, require_key=False, key_arg='key'):
    def decorate(func):
        spec = inspect.getfullargspec(func)
        offset = len(spec.args)

        def decomposed(*args, **kwargs):
            location = args[offset:]

            uri, bucket, key = (None, None, None)
            if kwargs.get('uri') is None and kwargs.get('bucket') is None and kwargs.get(key_arg) is None:
                if len(location) == 0:
                    raise Exception('A location must be specified')
                if len(location) > 2:
                    raise Exception('Too many location values')
                if len(location) == 1:
                    if location[0].startswith('s3:'):
                        (bucket, key) = decompose_uri(location[0])
                        if bucket is None:
                            raise Exception('Invalid S3 URI')
                    else:
                        bucket = location[0]
                else:
                    (bucket, key) = location
            elif len(location) > 0:
                raise Exception('Both positional location and ' + key_arg + ' values are present')
            if kwargs.get('uri'):
                (bucket, key) = decompose_uri(kwargs.get('uri'))
                if bucket is None:
                    raise Exception('Invalid S3 URI')
            if bucket:
                kwargs['bucket'] = bucket
            if key:
                kwargs[key_arg] = key

            if require_bucket and (kwargs.get('bucket') is None or len(kwargs.get('bucket')) == 0):
                raise Exception('A bucket must be provided')
            if require_key and (kwargs.get(key_arg) is None or len(kwargs.get(key_arg)) == 0):
                raise Exception('A key must be provided')

            return func(*args, **kwargs)

        return decomposed

    return decorate


@__load_resource
@__decompose_location(require_key=True)
def delete_object(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Deletes the object defined by the bucket/key pair or uri.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: Dict containing the boto3 response
    """
    return s3_resource.Bucket(bucket).Object(key=key).delete()


@__load_resource
@__decompose_location(require_key=True)
def get(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Performs a 'get' of the object defined by the bucket/key pair or uri.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: Dict containing the Body of the object and associated attributes
    """
    return s3_resource.Bucket(bucket).Object(key=key).get()


@__load_resource
@__decompose_location(require_key=True)
def get_size(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Returns the content_length of an S3 object.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: Size in bytes
    """
    return s3_resource.Bucket(bucket).Object(key=key).content_length


@__load_resource
@__decompose_location(require_key=True)
def read(*location, bucket=None, key=None, uri=None, byte_count=None, s3_resource=None):
    """
    Performs a 'get' of the object defined by the bucket/key pair or uri
    and then performs a 'read' of the Body of that object.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param byte_count: The max number of bytes to read from the object. All data is read if omitted.
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The bytes contained in the object
    """
    return get(bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)['Body'].read(byte_count)


@__load_resource
@__decompose_location(require_key=True)
def read_as(o_type, *location, bucket=None, key=None, uri=None, encoding='utf-8', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and loads the
    contents into an object of the specified type
    :param o_type: A value defined in larry.types to load the data using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: An object representation of the data in S3
    """
    obj = read(bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)
    # TODO: Would a handler or local constant be a better idea here?
    if o_type == types.TYPE_DICT:
        return json.loads(obj.decode(encoding), object_hook=utils.JSONDecoder)
    elif o_type == types.TYPE_STRING:
        return obj.decode(encoding)
    elif o_type == types.TYPE_PILLOW_IMAGE:
        try:
            from PIL import Image
            return Image.open(BytesIO(obj))
        except ImportError as e:
            # Simply raise the ImportError to let the user know this requires Pillow to function
            raise e
    else:
        raise Exception('Unhandled type')


@__load_resource
@__decompose_location(require_key=True)
def read_list_as(o_type, *location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri, decodes it to a string, and
    splits it into lines. Returns an array containing an object representing the contents of each line.
    :param o_type: A value defined in larry.types to load the data using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: An object representation of the data in S3
    """
    obj = read(bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)
    lines = obj.decode(encoding).split(newline)
    records = []
    for line in lines:
        if len(line) > 0:
            # TODO: Would a handler or local constant be a better idea here?
            if o_type == types.TYPE_DICT:
                records.append(json.loads(line, object_hook=utils.JSONDecoder))
            elif o_type == types.TYPE_STRING:
                records.append(line)
            else:
                raise Exception('Unhandled type')
    return records


@__load_resource
@__decompose_location(require_key=True)
def read_iter_as(o_type, *location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri, decodes it to a string, and
    splits it into lines. Returns an iterator of objects representing the contents of each line.
    :param o_type: A value defined in larry.types to load the data using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: An object representation of the data in S3
    """
    obj = read(bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)
    lines = obj.decode(encoding).split(newline)
    for line in lines:
        if len(line) > 0:
            # TODO: Would a handler or local constant be a better idea here?
            if o_type == types.TYPE_DICT:
                yield json.loads(line, object_hook=utils.JSONDecoder)
            elif o_type == types.TYPE_STRING:
                yield line
            else:
                raise Exception('Unhandled type')


@__load_resource
@__decompose_location(require_key=True)
def read_dict(*location, bucket=None, key=None, uri=None, encoding='utf-8', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and
    loads the json contents into a dict.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: A dict representation of the json contained in the object
    """
    return read_as(types.TYPE_DICT, bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def read_str(*location, bucket=None, key=None, uri=None, encoding='utf-8', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and
    decodes it to text.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The contents of the object as a string
    """
    return read_as(types.TYPE_STRING, bucket=bucket, key=key, uri=uri, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def read_list_of_dict(*location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and
    loads the JSON Lines data into a list of dict objects.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The contents of the object as a list of dict objects
    """
    return read_list_as(types.TYPE_DICT, bucket=bucket, key=key, uri=uri,
                        encoding=encoding, newline=newline, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def read_list_of_str(*location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n', s3_resource=None):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and
    loads the JSON Lines data into a list of dict objects.
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :param newline: The line separator to use when reading in the object, \n by default
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The contents of the object as a list of dict objects
    """
    return read_list_as(types.TYPE_STRING, bucket=bucket, key=key, uri=uri,
                        encoding=encoding, newline=newline, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def write(body, *location, bucket=None, key=None, uri=None, acl=None, content_type=None, content_encoding=None,
          content_language=None, content_length=None, metadata=None, sse=None, storage_class=None, redirect=None,
          tags=None, s3_resource=None):
    """
    Write an object to the bucket/key pair or uri.
    :param body: Data to write
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param content_type: A standard MIME type describing the format of the object data
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
    mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param redirect: If the bucket is configured as a website, redirects requests for this object to another object in
    the same bucket or to an external URL.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URI of the object written to S3
    """
    params = utils.map_parameters(locals(), {
        'acl': 'ACL',
        'body': 'Body',
        'content_encoding': 'ContentEncoding',
        'content_language': 'ContentLanguage',
        'content_length': 'ContentLength',
        'content_type': 'ContentType',
        'metadata': 'Metadata',
        'sse': 'ServerSideEncryption',
        'storage_class': 'StorageClass',
        'redirect': 'WebsiteRedirectLocation'
    })
    if tags:
        params['Tagging'] = parse.urlencode(tags) if isinstance(tags, Mapping) else tags

    obj = s3_resource.Bucket(bucket).Object(key=key)
    obj.put(**params)
    return compose_uri(bucket, key)


extension_types = {
    'css': 'text/css',
    'html': 'text/html',
    'xhtml': 'text/html',
    'htm': 'text/html',
    'xml': 'text/xml',
    'csv': 'text/csv',
    'txt': 'text/plain',
    'png': 'image/png',
    'jpeg': 'image/jpeg',
    'jpg': 'image/jpeg',
    'gif': 'image/gif',
    'jsonl': 'application/x-jsonlines',
    'json': 'application/json',
    'js': 'application/javascript',
    'zip': 'application/zip',
    'pdf': 'application/pdf',
    'sql': 'application/sql'
}


@__load_resource
@__decompose_location(require_key=True)
def write_as(value, o_type, *location, bucket=None, key=None, uri=None, acl=None, newline='\n', delimiter=',',
             columns=None, headers=None, content_type=None, content_encoding=None, content_language=None,
             content_length=None, metadata=None, sse=None, storage_class=None, redirect=None, tags=None,
             s3_resource=None):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.
    :param value: Object to write to S3
    :param o_type: A value defined in larry.types to write the data using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param newline: Character(s) to use as a newline for list objects
    :param delimiter: Column delimiter to use, ',' by default
    :param columns: The columns to write out from the source rows, dict keys or list indexes
    :param headers: Headers to add to the output
    :param acl: The canned ACL to apply to the object
    :param content_type: Content type to apply to the file, if not present a suggested type will be applied
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
    mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param redirect: If the bucket is configured as a website, redirects requests for this object to another object in
    the same bucket or to an external URL.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URI of the object written to S3
    """
    obj = None
    extension = key.split('.')[-1]
    if o_type == types.TYPE_STRING:
        if content_type is None:
            content_type = extension_types.get(extension, 'text/plain')
        obj = value
    elif o_type == types.TYPE_DICT:
        if content_type is None:
            content_type = 'application/json'
        obj = json.dumps(value, cls=utils.JSONEncoder)
    elif o_type == types.TYPE_PILLOW_IMAGE:
        obj = BytesIO()
        fmt = 'PNG' if value.format is None else value.format
        value.save(obj, fmt)
        obj.seek(0)
        if content_type is None:
            content_type = extension_types.get(extension, extension_types.get(fmt.lower(), 'text/plain'))
    elif o_type == types.TYPE_JSON_LINES:
        if content_type is None:
            content_type = extension_types.get(extension, 'text/plain')
        buff = StringIO()
        for row in value:
            buff.write(json.dumps(row, cls=utils.JSONEncoder) + newline)
        obj = buff.getvalue()
    elif o_type == types.TYPE_DELIMITED:
        if content_type is None:
            content_type = extension_types.get(extension, 'text/plain')
        buff = StringIO()
        # empty
        if value is None or len(value) == 0:
            if headers:
                buff.write(_array_to_string(headers, delimiter) + newline)
            buff.write('')

        # list
        elif isinstance(value[0], list):
            indices = columns if columns else None
            if headers:
                buff.write(_array_to_string(headers, delimiter) + newline)
            for row in value:
                buff.write(_array_to_string(row, delimiter, indices) + newline)

        # dict
        elif isinstance(value[0], Mapping):
            keys = columns if columns else value[0].keys()
            buff.write(_array_to_string(headers if headers else keys, delimiter) + newline)

            for row in value:
                line = ''
                for i, k in enumerate(keys):
                    value = '' if row.get(k) is None else str(row.get(k))
                    line = value if i == 0 else line + delimiter + value
                buff.write(line + "\n")

        # string
        elif isinstance(value[0], str):
            buff.writelines(value)
        else:
            raise Exception('Invalid input')
        obj = buff.getvalue()
    else:
        raise Exception('Unhandled type')
    return write(obj, bucket, key, uri, acl=acl, content_type=content_type, content_encoding=content_encoding,
                 content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                 storage_class=storage_class, redirect=redirect, tags=tags, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def write_object(value, *location, bucket=None, key=None, uri=None, newline='\n', acl=None, content_type=None,
                 content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
                 storage_class=None, redirect=None, tags=None, s3_resource=None):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.
    :param value: Object to write to S3
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param newline: Character(s) to use as a newline for list objects
    :param content_type: Content type to apply to the file, if not present a suggested type will be applied
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
    mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param redirect: If the bucket is configured as a website, redirects requests for this object to another object in
    the same bucket or to an external URL.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URI of the object written to S3
    """
    # TODO: Add support for pandas and numpy objects
    extension = key.split('.')[-1]
    # JSON
    if isinstance(value, Mapping):
        write_as(value, types.TYPE_DICT, bucket=bucket, key=key, uri=uri, acl=acl, newline=newline,
                 content_type=content_type, content_encoding=content_encoding,
                 content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                 storage_class=storage_class, redirect=redirect, tags=tags, s3_resource=s3_resource)
    # Text
    elif isinstance(value, str):
        write_as(value, types.TYPE_STRING, bucket=bucket, key=key, uri=uri, acl=acl, newline=newline,
                 content_type=content_type, content_encoding=content_encoding,
                 content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                 storage_class=storage_class, redirect=redirect, tags=tags, s3_resource=s3_resource)

    # List
    elif isinstance(value, list):
        if content_type is None:
            content_type = extension_types.get(extension, 'text/plain')
        buff = StringIO()
        for row in value:
            if isinstance(row, Mapping):
                buff.write(json.dumps(row, cls=utils.JSONEncoder) + newline)
            else:
                buff.write(str(row) + newline)
        return write(buff.getvalue(), bucket, key, uri, acl, content_type=content_type,
                     content_encoding=content_encoding, content_language=content_language,
                     content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                     redirect=redirect, tags=tags, s3_resource=s3_resource)
    elif value is None:
        return write('', bucket, key, uri, acl, s3_resource=s3_resource, content_type=content_type,
                     content_encoding=content_encoding, content_language=content_language,
                     content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                     redirect=redirect, tags=tags)
    else:
        # try to write it as an image
        try:
            write_as(value, types.TYPE_PILLOW_IMAGE, bucket=bucket, key=key, uri=uri, acl=acl, newline=newline,
                     content_type=content_type, content_encoding=content_encoding, content_language=content_language,
                     content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                     redirect=redirect, tags=tags, s3_resource=s3_resource)
        except Exception:
            return write(value, bucket, key, uri, acl, content_type=content_type, content_encoding=content_encoding,
                         content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                         storage_class=storage_class, redirect=redirect, tags=tags, s3_resource=s3_resource)


def _array_to_string(row, delimiter, indices=None):
    if indices is None:
        indices = range(len(row))
    line = ''
    for x in indices:
        line = str(row[x]) if x == 0 else line + delimiter + str(row[x])
    return line


@__load_resource
@__decompose_location(require_key=True)
def write_delimited(rows, *location, bucket=None, key=None, uri=None, acl=None, newline='\n', delimiter=',',
                    columns=None, headers=None, content_type=None, content_encoding=None, content_language=None,
                    content_length=None, metadata=None, sse=None, storage_class=None, redirect=None, tags=None,
                    s3_resource=None):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.
    :param rows: List of data to write, rows can be of type list, dict or str
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param newline: Character(s) to use as a newline for list objects
    :param delimiter: Column delimiter to use, ',' by default
    :param columns: The columns to write out from the source rows, dict keys or list indexes
    :param headers: Headers to add to the output
    :param content_type: Content type to apply to the file, if not present a suggested type will be applied
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
    mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param redirect: If the bucket is configured as a website, redirects requests for this object to another object in
    the same bucket or to an external URL.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URI of the object written to S3
    """
    return write_as(rows, types.TYPE_DELIMITED, bucket=bucket, key=key, uri=uri, acl=acl, newline=newline,
                    delimiter=delimiter, columns=columns, headers=headers, content_type=content_type,
                    content_encoding=content_encoding, content_language=content_language, content_length=content_length,
                    metadata=metadata, sse=sse, storage_class=storage_class, redirect=redirect, tags=tags,
                    s3_resource=s3_resource)


@__load_resource
def rename_object(old_bucket=None, old_key=None, old_uri=None, new_bucket=None, new_key=None, new_uri=None,
                  s3_resource=None):
    """
    Renames an object in S3.
    :param old_bucket: Source bucket
    :param old_key: Source key
    :param old_uri: An s3:// path containing the bucket and key of the source object
    :param new_bucket: Target bucket
    :param new_key: Target key
    :param new_uri: An s3:// path containing the bucket and key of the source object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: None
    """
    if old_uri:
        (old_bucket, old_key) = decompose_uri(old_uri)
    if new_uri:
        (new_bucket, new_key) = decompose_uri(new_uri)
    s3_resource = s3_resource if s3_resource else resource
    copy_source = {
        'Bucket': old_bucket,
        'Key': old_key
    }
    s3_resource.meta.client.copy(copy_source, new_bucket, new_key)
    s3_resource.meta.client.delete_object(Bucket=old_bucket, Key=old_key)


@__load_resource
def copy_object(old_bucket=None, old_key=None, old_uri=None, new_bucket=None, new_key=None, new_uri=None,
                s3_resource=None):
    """
    Copies an object in S3.
    :param old_bucket: Source bucket
    :param old_key: Source key
    :param old_uri: An s3:// path containing the bucket and key of the source object
    :param new_bucket: Target bucket
    :param new_key: Target key
    :param new_uri: An s3:// path containing the bucket and key of the source object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: None
    """
    if old_uri:
        (old_bucket, old_key) = decompose_uri(old_uri)
    if new_uri:
        (new_bucket, new_key) = decompose_uri(new_uri)
    s3_resource = s3_resource if s3_resource else resource
    s3_resource.meta.client.copy({'Bucket': old_bucket, 'Key': old_key}, new_bucket, new_key)


@__load_resource
@__decompose_location(require_key=True)
def exists(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Checks to see if an object with the given bucket/key (or uri) exists.
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: True if the key exists, if not, False
    """
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            raise e
    return True


uri_regex = re.compile("^[sS]3:\/\/([a-z0-9\.\-]{3,})\/?(.*)")


def decompose_uri(uri):
    """
    Decompose an S3 URI into a bucket and key
    :param uri: S3 URI
    :return: Tuple containing a bucket and key
    """
    m = uri_regex.match(uri)
    if m:
        return m.groups()
    else:
        return None, None


def get_bucket_name(uri):
    """
    Retrieve the bucket portion from an S3 URI
    :param uri: S3 URI
    :return: Bucket name
    """
    return decompose_uri(uri)[0]


def get_object_key(uri):
    """
    Retrieves the key portion of an S3 URI
    :param uri: S3 URI
    :return: Key value
    """
    return decompose_uri(uri)[1]


def compose_uri(bucket, key):
    """
    Compose a bucket and key into an S3 URI
    :param bucket: Bucket name
    :param key: Object key
    :return: S3 URI string
    """
    return "s3://{}/{}".format(bucket, key)


@__load_resource
@__decompose_location(key_arg='prefix')
def list_objects(*location, bucket=None, prefix=None, uri=None, include_empty_objects=False, s3_resource=None):
    """
    Returns a list of the object keys in the provided bucket that begin with the provided prefix.
    :param bucket: The S3 bucket to query
    :param prefix: The key prefix to use in searching the bucket
    :param uri: An s3:// path containing the bucket and prefix
    :param include_empty_objects: True if you want to include keys associated with objects of size=0
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: A generator of object keys
    """
    paginator = s3_resource.meta.client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket}
    if prefix:
        operation_parameters['Prefix'] = prefix
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for obj in page.get('Contents', []):
            if obj['Size'] > 0 or include_empty_objects:
                yield obj['Key']


def _find_largest_common_prefix(values):
    """
    Searches through a list of values to find the longest possible common prefix amongst them. Useful for optimizing
    more costly searches. Supports lists of strings or tuples. If tuples are used, the first value is assumed to be
    the value to search on.
    :param values: List of values (strings or tuples containing a string in the first position)
    :return: String prefix common to all values
    """
    if isinstance(values[0], tuple):
        prefix, *_ = values[0]
    else:
        prefix = values[0]

    for value in values:
        key = value[0] if isinstance(value, tuple) else value
        while key[:len(prefix)] != prefix and len(prefix) > 0:
            prefix = prefix[:-1]
    return prefix


@__load_resource
def find_keys_not_present(bucket, keys=None, uris=None, s3_resource=None):
    """
    Searches an S3 bucket for a list of keys and returns any that cannot be found.
    :param bucket: The S3 bucket to search
    :param keys: A list of keys to search for (strings or tuples containing a string in the first position)
    :param uris: A list of S3 URIs to search for (strings or tuples containing a string in the first position)
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: A list of keys that were not found (strings or tuples based on the input values)
    """

    # If URIs are passed, convert to a list of keys to use for the search
    if uris:
        keys = []
        for value in uris:
            if isinstance(value, tuple):
                uri, *z = value
                b, key = decompose_uri(uri)
                keys.append(tuple([key]) + tuple(z))
            else:
                b, key = decompose_uri(value)
                keys.append(key)

    # Find the longest common prefix to use as the search term
    prefix = _find_largest_common_prefix(keys)

    # Get a list of all keys in the bucket that match the prefix
    bucket_obj = s3_resource.Bucket(bucket)
    all_keys = []
    for obj in bucket_obj.objects.filter(Prefix=prefix):
        all_keys.append(obj.key)

    # Search for any keys that can't be found
    not_found = []
    for value in keys:
        key = value[0] if isinstance(value, tuple) else value
        if key not in all_keys:
            not_found.append(value)
    return not_found


@__load_resource
@__decompose_location(require_key=True)
def fetch(url, *location, bucket=None, key=None, uri=None, s3_resource=None, **kwargs):
    """
    Retrieves the data defined by a URL to an S3 location.
    :param url: URL to retrieve
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URI of the object written to S3
    """
    req = Request(url, **kwargs)
    with request.urlopen(req) as response:
        return write_object(response.read(), bucket=bucket, key=key, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def download(directory, *location, bucket=None, key=None, uri=None, use_threads=True, s3_resource=None):
    """
    Downloads the an S3 object to a directory on the local file system.
    :param directory: The directory to download the object to
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param use_threads: Enables the use_threads transfer config
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: Path of the local file
    """
    config = TransferConfig(use_threads=use_threads)
    s3_object_local = os.path.join(directory, key.split('/')[-1])
    try:
        s3_resource.Bucket(bucket).download_file(key, s3_object_local, Config=config)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return s3_object_local


@__load_resource
@__decompose_location(require_key=True)
def download_to_temp(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Downloads the an S3 object to a temp directory on the local file system.
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: Path of the local file
    """
    temp_dir = _create_temp_dir()
    file = os.path.join(temp_dir, key.split('/')[-1])
    if not os.path.isfile(file):
        print('starting download')
        download(temp_dir, bucket, key, use_threads=True, s3_resource=s3_resource)
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


@__load_resource
@__decompose_location(require_key=True)
def upload(file_name, *location, bucket=None, key=None, uri=None, s3_resource=None):
    s3_resource.Bucket(bucket).upload_file(file_name, key)
    return compose_uri(bucket, key)


@__load_resource
def write_temp_object(value, prefix, acl=None, s3_resource=None, bucket_identifier=None, region=None,
                      bucket=None):
    """
    Write an object to a temp bucket with a unique UUID.
    :param value: Object to write to S3
    :param prefix: Prefix to attach ahead of the UUID as the key
    :param acl: The canned ACL to apply to the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :param bucket_identifier: The identifier to attach to the temp bucket that will be used for writing to s3, typically
    the account id (from STS) for the account being used
    :param region: The s3 region to store the data in
    :param bucket: The bucket to use instead of creating/using a temp bucket
    :return: The URI of the object written to S3
    """
    s3_resource = s3_resource if s3_resource else resource
    if bucket is None:
        bucket = get_temp_bucket(region=region, bucket_identifier=bucket_identifier, s3_resource=s3_resource)
    key = prefix + str(uuid.uuid4())
    return write_object(value, bucket=bucket, key=key, acl=acl, s3_resource=s3_resource)


@__load_resource
@__decompose_location(require_key=True)
def make_public(*location, bucket=None, key=None, uri=None, s3_resource=None):
    """
    Makes the object defined by the bucket/key pair (or uri) public.
    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :return: The URL of the object
    """
    s3_resource.meta.client.put_object_acl(Bucket=bucket, Key=key, ACL='public-read')
    return get_public_url(bucket=bucket, key=key)


@__decompose_location()
def get_public_url(*location, bucket=None, key=None, uri=None):
    """
    Returns the public URL of an S3 object (assuming it's public).
    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URL of the object
    """
    if key:
        return 'https://{}.s3.amazonaws.com/{}'.format(bucket, parse.quote(key))
    else:
        return 'https://{}.s3.amazonaws.com'.format(bucket)


@__load_resource
def create_bucket(bucket, acl='private', region=__session.region_name, s3_resource=None):
    bucket_obj = s3_resource.Bucket(bucket)
    bucket_obj.load()
    if bucket_obj.creation_date is None:
        bucket_obj.create(ACL=acl, CreateBucketConfiguration={'LocationConstraint': region})
        bucket_obj.wait_until_exists()
    return bucket_obj


@__load_resource
def delete_bucket(bucket, s3_resource=None):
    bucket_obj = s3_resource.Bucket(bucket)
    bucket_obj.delete()


@__load_resource
def get_temp_bucket(region=None, s3_resource=None, bucket_identifier=None):
    """
    Create a bucket that will be used as temp storage for larry commands.
    The bucket will be created in the region associated with the current session
    using a name based on the current session account id and region.
    :param region: Region to locate the temp bucket
    :param s3_resource: Boto3 resource to use if you don't wish to use the default resource
    :param bucket_identifier: The bucket identifier to use as a unique identifier for the bucket, defaults to the
    account id associated with the session
    :return: The name of the created bucket
    """
    if region is None:
        region = __session.region_name
    if bucket_identifier is None:
        bucket_identifier = sts.account_id()
    bucket = '{}-larry-{}'.format(bucket_identifier, region)
    create_bucket(bucket, region=region, s3_resource=s3_resource)
    return bucket


# TODO: add filter parameter
# TODO: rationalize the list params
def download_to_zip(file, bucket, prefix=None, prefixes=None):
    if prefix:
        prefixes = [prefix]
    with ZipFile(file, 'w') as zfile:
        for prefix in prefixes:
            for key in list_objects(bucket, prefix):
                zfile.writestr(parse.quote(key), data=read(bucket, key))


def file_name_portion(uri):
    file = decompose_uri(uri)[1].split('/')[-1]
    return file[:file.rfind('.')]
