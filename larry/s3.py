import botocore.exceptions
import boto3
from boto3.s3.transfer import TransferConfig
import os
import posixpath
import re
import uuid
import json
import csv
import pickle
import mimetypes
from io import StringIO, BytesIO
import tempfile
from collections.abc import Mapping
import warnings
import larry.core
from larry.utils.dispatch import larrydispatch
from larry import utils
from larry import sts
from larry import ClientError
from larry.core import ResourceWrapper, attach_exception_handler, supported_kwargs
from urllib import parse, request
from zipfile import ZipFile
from enum import Enum

# A local instance of the boto3 session to use
__session = boto3.session.Session()
# Local S3 resource object
__resource = __session.resource('s3')

URI_REGEX = re.compile("^[sS]3://([a-z0-9.-]{3,})/?(.*)")
DEFAULT_ENCODING = "utf-8"
DEFAULT_NEWLINE = "\n"

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

# The following associations override guesses from mimetypes
__extension_types = {
    'csv': 'text/csv',
    'jsonl': 'application/x-jsonlines',
    'js': 'text/javascript',
    'zip': 'application/zip',
    'sql': 'application/sql',
    'webp': 'image/webp',
    'ico': 'image/vnd.microsoft.icon',
    'pkl': 'application/octet-stream'
}

__content_type_to_pillow_format = {
    'image/png': 'PNG',
    'image/jpeg': 'JPEG',
    'image/gif': 'GIF',
    'image/tiff': 'TIFF',
    'image/webp': 'WebP',
    'image/bmp': 'BMP',
    'image/vnd.microsoft.icon': 'ICO',
    'image/x-icon': 'ICO'
}


def __getattr__(name):
    if name == 'resource':
        return __resource
    elif name == 'session':
        return __session
    elif name == 'client':
        return __resource.meta.client
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _get_resource():
    return __resource


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
    global __session, __resource
    __session = boto_session if boto_session is not None else boto3.session.Session(
        **larry.core.copy_non_null_keys(locals()))
    sts.set_session(boto_session=__session)
    __resource = __session.resource('s3')


def normalize_location(*location, uri: str = None, bucket: str = None, key: str = None,
                       require_bucket=True, require_key=True, key_arg='key', allow_multiple=False):
    if not any([uri, bucket, key]):
        if len(location) == 0:
            raise TypeError('A location must be specified')
        if len(location) > 2:
            raise TypeError('Too many location values')
        if len(location) == 1:
            if isinstance(location[0], Object) or type(location[0]).__name__ == "s3.Object":
                bucket = location[0].bucket_name
                key = location[0].key
            elif isinstance(location[0], list) and location[0][0].startswith('s3:'):
                uri = location[0]
            elif isinstance(location[0], str) and location[0].startswith('s3:'):
                uri = location[0]
            else:
                bucket = location[0]
        else:
            (bucket, key) = location
    elif len(location) > 0:
        raise TypeError('Both positional location and ' + key_arg + ' values are present')

    if uri:
        if isinstance(uri, list) and len(uri) > 0:
            if not allow_multiple:
                raise TypeError('You cannot provide a list of URIs for this function')
            pairs = [split_uri(u) for u in uri]
            key = []
            bucket = pairs[0][0]
            for pair in pairs:
                key.append(pair[1])
                if pair[0] != bucket:
                    raise TypeError('Multiple values for bucket are not allowed')
        else:
            (bucket, key) = split_uri(uri)
            if bucket is None:
                raise TypeError('Invalid S3 URI')

    if isinstance(key, list) and not allow_multiple:
        raise TypeError('You cannot provide a list of keys for this function')

    if isinstance(bucket, Bucket) or type(bucket).__name__ == "s3.Bucket":
        bucket = bucket.name

    if require_bucket:
        if not isinstance(bucket, str):
            raise TypeError(f"bucket must be of type 'str'")
        if bucket is None or len(bucket) == 0:
            raise TypeError('A bucket must be provided')
    if require_key:
        if not isinstance(key, str) and not isinstance(key, list):
            raise TypeError("key must be of type 'str'")
        if key is None or len(key) == 0:
            raise TypeError('A key must be provided')

    return bucket, key, uri


class Object(ResourceWrapper):
    """
    Wraps the boto3 S3
    `Object <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#object>`_
    resource with helper functions to make it easier to interact with objects
    and access additional attributes.

    .. code-block:: python

        import larry as lry
        obj = lry.s3.Object('bucket_name', 'key')

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    """

    def __init__(self, *location, bucket=None, key=None, uri=None):
        bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
        super().__init__(Bucket(bucket).Object(key=key))

    @property
    @attach_exception_handler
    def tags(self):
        """
        Returns dict containing a key/value pair for the tags that have been attached to the object.
        """
        tags = self.meta.client.get_object_tagging(Bucket=self.bucket_name, Key=self.key).get('TagSet', [])
        return {pair['Key']: pair['Value'] for pair in tags}

    @property
    @attach_exception_handler
    def exists(self):
        """
        Attempts to load header information for the S3 object and returns true if it exists, false otherwise.
        """
        try:
            self.load()
        except ClientError as e:
            if e.code == "404":
                return False
            else:
                raise e
        return True

    @attach_exception_handler
    def set_acl(self, acl):
        """
        Assigns the provided ACL to the object.
        """
        self.meta.client.put_object_acl(Bucket=self.bucket_name, Key=self.key, ACL=acl)

    @attach_exception_handler
    def set_content_type(self, content_type):
        # TODO: Fix bug where this will wipe the ACL on the existing file
        self.copy_from(CopySource={'Bucket': self.bucket_name, 'Key': self.key},
                       ContentType=content_type,
                       MetadataDirective='REPLACE',
                       TaggingDirective='COPY')

    def make_public(self):
        """
        Assigns a public-read ACL to the object to allow anyone to access it.
        """
        self.set_acl(ACL_PUBLIC_READ)
        return self.url

    @property
    def url(self):
        """
        Returns the public URL of the object (assuming permissions have been set appropriately).
        """
        return _object_url(self.bucket_name, self.key)

    @property
    def uri(self):
        return join_uri(self.bucket_name, self.key)

    @property
    def bucket(self):
        return Bucket(self.bucket_name)

    def __repr__(self):
        return f'Object(bucket="{self.bucket_name}", key="{self.key}")'


class Bucket(ResourceWrapper):
    """
    Wraps the boto3 S3
    `Bucket <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucket>`_
    resource with helper functions to make it easier to interact with buckets
    and access additional attributes.

    .. code-block:: python

        import larry as lry
        bucket = lry.s3.Bucket('bucket_name')

    :param bucket: The S3 bucket
    """

    def __init__(self, bucket):
        super().__init__(_get_resource().Bucket(bucket))

    @property
    @attach_exception_handler
    def exists(self):
        """
        Will attempt to retrieve information for the S3 bucket and returns true if it exists, false otherwise.
        """
        try:
            create_date = self.creation_date
            return create_date is not None
        except ClientError as e:
            if e.code == "404":
                return False
            else:
                raise e

    @property
    def url(self):
        """
        Returns the public URL of the bucket (assuming permissions have been set appropriately).
        """
        return _bucket_url(self.name)

    @property
    def website(self):
        """
        Returns a BucketWebsite resource object.
        """

        class BucketWebsite(ResourceWrapper):
            def __init__(self, bucket):
                super().__init__(bucket.Website())

        return BucketWebsite(self)

    def __repr__(self):
        return f'Bucket("{self.name}")'

    @property
    def cors(self):
        try:
            return [CorsRule.from_response(rule) for rule in self.Cors().cors_rules]
        except botocore.exceptions.ClientError as e:
            if e.response.get("Error", {}).get("Code") == "NoSuchCORSConfiguration":
                return None
            else:
                raise e

    @cors.setter
    def cors(self, rules):
        if isinstance(rules, CorsRule):
            rules = [rules.to_dict()]
        elif isinstance(rules, dict):
            rules = [rules]
        elif isinstance(rules, list):
            rules = [rule.to_dict() if isinstance(rule, CorsRule) else rule for rule in rules]
        else:
            raise Exception("CORS rules must be a single or list of larry.s3.CorsRule objects or dict objects")
        self.Cors().put(CORSConfiguration={
            "CORSRules": rules
        })

    @cors.deleter
    def cors(self):
        self.Cors().delete()


class CorsRule:
    def __init__(self, allowed_methods, allowed_origins, _id=None, allowed_headers=None, expose_headers=None,
                 max_age_seconds=None):
        self.allowed_methods = allowed_methods
        self.allowed_origins = allowed_origins
        self.id = _id
        self.allowed_headers = allowed_headers
        self.expose_headers = expose_headers
        self.max_age_seconds = max_age_seconds

    @classmethod
    def from_response(cls, rule):
        return cls(allowed_methods=rule.get("AllowedMethods"),
                   allowed_origins=rule.get("AllowedOrigins"),
                   _id=rule.get("ID"),
                   allowed_headers=rule.get("AllowedHeaders"),
                   expose_headers=rule.get("ExposeHeaders"),
                   max_age_seconds=rule.get("MaxAgeSeconds"))

    @classmethod
    def default(cls):
        return cls(["GET"], ["*"])

    def to_dict(self):
        value = {
            "AllowedMethods": self.allowed_methods,
            "AllowedOrigins": self.allowed_origins,
        }
        if self.id:
            value["ID"] = self.id
        if self.allowed_headers:
            value["AllowedHeaders"] = self.allowed_headers
        if self.expose_headers:
            value["ExposeHeaders"] = self.expose_headers
        if self.max_age_seconds:
            value["MaxAgeSeconds"] = self.max_age_seconds
        return value

    def __repr__(self):
        return f"CorsRule({self.allowed_methods}, {self.allowed_origins})"


def delete(*location, bucket=None, key=None, uri=None):
    """
    Deletes the object defined by the bucket/key pair or uri.

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket
    :param key: The key of the object, this can be a single str value or a list of keys to delete
    :param uri: An s3:// path containing the bucket and key of the object
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    if isinstance(key, list):
        Bucket(bucket=bucket).delete_objects(Delete={'Objects': [{'Key': k} for k in key], 'Quiet': True})
    else:
        Object(bucket=bucket, key=key).delete()


def size(*location, bucket=None, key=None, uri=None):
    """
    Returns the number of bytes (content_length) in an S3 object.

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: Size in bytes
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return Object(bucket=bucket, key=key).content_length


def get_content_type(*location, bucket=None, key=None, uri=None):
    """
    Returns the content type assigned to the object

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: A standard MIME type describing the format of the object data.
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return Object(bucket=bucket, key=key).content_type


def read(*location, bucket=None, key=None, uri=None, byte_count=None):
    """
    Retrieves the contents of an S3 object

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param byte_count: The max number of bytes to read from the object. All data is read if omitted.
    :return: The bytes contained in the object
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return Object(bucket=bucket, key=key).get()['Body'].read(byte_count)


@larrydispatch
def read_as(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    """
    Reads in the s3 object defined by the bucket/key pair or uri and loads the
    contents into an object of the specified type.

    .. code-block:: python

        import larry as lry
        import numpy as np
        np_array = lry.s3.read_as(np.ndarray, 'my-bucket', 'my-key')

    :param type_: The data type to indicate how to read in the data
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param encoding: The charset to use when decoding the object bytes, utf-8 by default
    :return: An object representation of the data in S3
    """
    raise TypeError("Unsupported type")


@read_as.register_module_name("numpy")
@read_as.register_type_name("ndarray")
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    try:
        import numpy as np
        with tempfile.TemporaryFile() as fp:
            download(fp, bucket=bucket, key=key, uri=uri)
            fp.seek(0)
            return np.fromfile(fp)
    except ImportError as ex:
        # Simply raise the ImportError to let the user know this requires Numpy to function
        raise ex


@read_as.register_module_name("cv2")
@read_as.register_callable_name("imread")
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    fp = None
    try:
        fp = tempfile.NamedTemporaryFile(delete=False)
        download(fp, bucket=bucket, key=key, uri=uri)
        fp.close()
        if type_.__name__ in ['cv2', 'cv2.cv2']:
            img = type_.imread(fp.name, **kwargs)
        else:
            img = type_(fp.name, **kwargs)
    finally:
        if fp:
            os.remove(fp.name)
    return img


@read_as.register_eq(json)
@read_as.register_eq(dict)
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)

    try:
        return json.loads(objct.decode(encoding), object_hook=utils.JSONDecoder)
    except json.JSONDecodeError as ex:
        if kwargs.get("allow_single_quotes"):
            # TODO: Use a more stable replace operation that will handle nested quotes
            return json.loads(objct.decode(encoding).replace("'", '"'), object_hook=utils.JSONDecoder)
        else:
            raise ex


@read_as.register_eq(str)
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    return objct.decode(encoding)


@read_as.register_module_name("PIL.Image")
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    return type_.open(BytesIO(objct))


@read_as.register_eq([dict])
@read_as.register_eq([json])
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    lines = objct.decode(encoding).split(kwargs.get("newline", DEFAULT_NEWLINE))
    if kwargs.get("use_decoder"):
        return [json.loads(line, object_hook=utils.JSONDecoder) for line in lines if len(line) > 0]
    else:
        return [json.loads(line) for line in lines if len(line) > 0]


@read_as.register_eq([str])
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    lines = objct.decode(encoding).split(kwargs.get("newline", DEFAULT_NEWLINE))
    return [line for line in lines if len(line) > 0]


@read_as.register_eq(csv)
@read_as.register_eq(csv.reader)
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    return csv.reader(StringIO(objct.decode(encoding)), **kwargs)


@read_as.register_eq(csv.DictReader)
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    return csv.DictReader(StringIO(objct.decode(encoding)), **kwargs)


@read_as.register_eq(pickle)
def _(type_, *location, bucket=None, key=None, uri=None, encoding='utf-8', **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    objct = read(bucket=bucket, key=key, uri=uri)
    return pickle.loads(objct, **kwargs)


def _write(body, bucket=None, key=None, uri=None, acl=None, content_type=None, content_encoding=None,
           content_language=None, content_length=None, metadata=None, sse=None, storage_class=None,
           tags=None, encoding=None):
    """
    Write an object to the bucket/key pair or uri.
    :return: The object written to S3
    """
    params = larry.core.map_parameters(locals(), {
        'acl': 'ACL',
        'content_encoding': 'ContentEncoding',
        'content_language': 'ContentLanguage',
        'content_length': 'ContentLength',
        'content_type': 'ContentType',
        'metadata': 'Metadata',
        'sse': 'ServerSideEncryption',
        'storage_class': 'StorageClass',
    })
    if tags:
        params['Tagging'] = parse.urlencode(tags) if isinstance(tags, Mapping) else tags
    if isinstance(body, str):
        if encoding is None:
            encoding = DEFAULT_ENCODING
        params["Body"] = body.encode(encoding)
    else:
        params["Body"] = body

    obj = Object(bucket=bucket, key=key)
    obj.put(**params)
    return obj


@larrydispatch
def format_type_for_write(_type, value, key=None, content_type=None, **kwargs):
    return value, __recommend_content_type(content_type, key)


@format_type_for_write.register_module_name("cv2")
@format_type_for_write.register_callable_name("imwrite")
def _(_type, value, key=None, content_type=None, **kwargs):
    suffix = os.path.splitext(key)[1]
    content_type = __recommend_content_type(content_type, key, "image/png")
    handle, filepath = tempfile.mkstemp(suffix=suffix if suffix else '.png')
    try:
        if _type.__name__ in ['cv2', 'cv2.cv2']:
            _type.imwrite(filepath, value, **kwargs)
        else:
            _type(filepath, value, **kwargs)
        with open(filepath, 'rb') as fp:
            result = fp.read()
    finally:
        os.close(handle)
        if os.path.exists(filepath):
            os.remove(filepath)
    return {"value": result, "content_type": content_type}


@format_type_for_write.register_eq(str)
def _(_type, value, key=None, content_type=None, **kwargs):
    return value, __recommend_content_type(content_type, key, "text/plain")


@format_type_for_write.register_eq(int)
@format_type_for_write.register_eq(float)
def _(_type, value, key=None, content_type=None, **kwargs):
    return str(value), __recommend_content_type(content_type, key, "text/plain")


@format_type_for_write.register_eq(dict)
@format_type_for_write.register_eq(json)
def _(_type, value, key=None, content_type=None, **kwargs):
    kw = supported_kwargs(json.dumps, **kwargs)
    return (json.dumps(value, cls=kwargs.get("cls", utils.JSONEncoder), **kw),
            __recommend_content_type(content_type, key, "application/json"))


@format_type_for_write.register_eq([str])
def _(_type, value, key=None, content_type=None, **kwargs):
    buff = StringIO()
    for row in value:
        buff.write(row + kwargs.get("newline", DEFAULT_NEWLINE))
    return buff.getvalue(), __recommend_content_type(content_type, key, "text/plain")


@format_type_for_write.register_eq([dict])
@format_type_for_write.register_eq([json])
def _(_type, value, key=None, content_type=None, **kwargs):
    kw = supported_kwargs(json.dumps, **kwargs)
    buff = StringIO()
    for row in value:
        buff.write(json.dumps(row, cls=kwargs.get("cls", utils.JSONEncoder), **kw) + kwargs.get("newline", DEFAULT_NEWLINE))
    return buff.getvalue(), __recommend_content_type(content_type, key, "text/plain")


@format_type_for_write.register_eq(csv)
@format_type_for_write.register_eq(csv.writer)
def _(_type, value, key=None, content_type=None, **kwargs):
    buff = StringIO()
    writer = csv.writer(buff, **kwargs)
    for row in value:
        writer.writerow(row)
    return buff.getvalue(), __recommend_content_type(content_type, key, "text/plain")


@format_type_for_write.register_eq(pickle)
def _(_type, value, key=None, content_type=None, **kwargs):
    return pickle.dumps(value, **kwargs), __recommend_content_type(content_type, key, "application/octet-stream")


def __get_pillow_format(value, content_type, key, **kwargs):
    content_type = __recommend_content_type(content_type, key, value.get_format_mimetype())
    fmt = kwargs.get("format", value.format)
    if fmt is None:
        fmt = __content_type_to_pillow_format.get(content_type, "PNG")
    return content_type, fmt


@format_type_for_write.register_module_name("PIL.Image")
def _(_type, value, key=None, content_type=None, **kwargs):
    content_type, fmt = __get_pillow_format(value, content_type, key, **kwargs)
    objct = BytesIO()
    value.save(objct, fmt)
    objct.seek(0)
    return objct.getvalue(), content_type


@format_type_for_write.register_type_name("ndarray")
@format_type_for_write.register_class_name("ndarray")
def _(_type, value, key=None, content_type=None, **kwargs):
    kw = {k: v for k, v in kwargs.items() if k in ["sep", "format"]}
    with tempfile.TemporaryFile() as fp:
        value.tofile(fp, **kw)
        fp.seek(0)
        return fp.file.read(), None


def write_as(value, _type, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
             content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
             storage_class=None, tags=None, encoding=None, **kwargs):
    """
    Write an object to the bucket/key pair (or uri), converting the python
    object to an appropriate format to write to file.

    :param value: Object to write to S3
    :param _type: The data type to write the value using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param content_type: Content type to apply to the file, if not present a suggested type will be applied
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
        mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param encoding: The byte encoding to use for str values.
    :return: The URI of the object written to S3
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    value, content_type = format_type_for_write(_type, value, key, content_type)
    return _write(value, bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                  tags=tags, encoding=encoding)


def __recommend_content_type(content_type, key, default=None):
    if content_type is None:
        if key:
            suffix = os.path.splitext(key)[1]
            extension = suffix[1:].lower() if suffix else None
            content_type = __extension_types.get(extension, mimetypes.guess_type(key)[0])
        content_type = content_type if content_type else default
    return content_type


@larrydispatch
def write(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
          content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
          storage_class=None, tags=None, encoding=None, **kwargs):
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
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param encoding: The byte encoding to use for str values.
    :return: The URI of the object written to S3
    """
    raise TypeError(f"No write operation defined for value of type {type(value)}")


@write.register(Mapping)
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    return write_as(value, dict, *location, bucket=bucket, key=key, uri=uri, acl=acl,
                    content_type=content_type, content_encoding=content_encoding,
                    content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                    storage_class=storage_class, tags=tags, encoding=encoding, **kwargs)


@write.register(str)
@write.register(bytes)
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    return write_as(value, str, *location, bucket=bucket, key=key, uri=uri, acl=acl,
                    content_type=content_type, content_encoding=content_encoding,
                    content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                    storage_class=storage_class, tags=tags, encoding=encoding, **kwargs)


@write.register(StringIO)
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return _write(value.getvalue(), bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                  tags=tags, encoding=encoding)


@write.register(BytesIO)
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    value.seek(0)
    return _write(value.getvalue(), bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                  tags=tags, encoding=encoding)


@write.register(type(None))
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return _write('', bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                  tags=tags, encoding=encoding)


@write.register(list)
# TODO: Replace with iter solution?
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    content_type = __recommend_content_type(content_type, key, "text/plain")
    buff = StringIO()
    for row in value:
        if isinstance(row, Mapping):
            v, ct = format_type_for_write(dict, row, **kwargs)
        else:
            v, ct = format_type_for_write(str, row, **kwargs)
        buff.write(v + kwargs.get("newline", DEFAULT_NEWLINE))
    return _write(buff.getvalue(), bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                  tags=tags, encoding=encoding)


@write.register_class_name("PngImageFile")
@write.register_class_name("JpegImageFile")
# TODO: Add the rest; better option?
# TODO: Consider other ways to pass this to the write_as option. The problem is that the Image object isn't available to pass directly from here
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    content_type, fmt = __get_pillow_format(value, content_type, key, **kwargs)
    objct = BytesIO()
    value.save(objct, fmt)
    objct.seek(0)
    return _write(objct, bucket=bucket, key=key, uri=uri, acl=acl, content_type=content_type,
                  content_encoding=content_encoding, content_language=content_language,
                  content_length=content_length,
                  metadata=metadata, sse=sse, storage_class=storage_class, tags=tags)


@write.register_class_name("ndarray")
def _(value, *location, bucket=None, key=None, uri=None, acl=None, content_type=None,
      content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
      storage_class=None, tags=None, encoding=None, **kwargs):
    return write_as(value, value, *location, bucket=bucket, key=key, uri=uri, acl=acl,
                    content_type=content_type, content_encoding=content_encoding,
                    content_language=content_language, content_length=content_length, metadata=metadata, sse=sse,
                    storage_class=storage_class, tags=tags, **kwargs)


def __append(content, bucket=None, key=None, prefix=None, suffix=None, encoding=None):
    """
    Reads in an existing object, adds additional content, and then writes it back out with the same attributes
    and ACLs.
    """
    # load the object and build the parameters that will be used to rewrite it
    objct = Object(bucket, key)
    values = {
        'content_encoding': objct.content_encoding,
        'content_language': objct.content_language,
        'content_type': objct.content_type,
        'metadata': objct.metadata,
        'sse': objct.server_side_encryption,
        'storage_class': objct.storage_class
    }
    params = larry.core.map_parameters(values, {
        'content_encoding': 'ContentEncoding',
        'content_language': 'ContentLanguage',
        'content_length': 'ContentLength',
        'content_type': 'ContentType',
        'metadata': 'Metadata',
        'sse': 'ServerSideEncryption',
        'storage_class': 'StorageClass',
    })
    if objct.tags:
        params['Tagging'] = parse.urlencode(objct.tags)

    # get the current ACL
    acl = objct.Acl()
    grants = acl.grants
    owner = acl.owner

    if prefix:
        content = prefix + content
    if suffix:
        content = content + suffix

    if isinstance(content, str):
        if encoding is None:
            encoding = DEFAULT_ENCODING
        content = content.encode(encoding)

    body = objct.get()['Body'].read() + content
    objct.put(Body=body, **params)
    objct.Acl().put(AccessControlPolicy={
        'Grants': grants,
        'Owner': owner
    })


def append_as(value, _type, *location, bucket=None, key=None, uri=None, prefix=None, suffix=None, encoding=DEFAULT_ENCODING,
              **kwargs):
    """
    Append content to the end of an s3 object. Assumes that the data should be treated as text in most cases.

    Note that this is not efficient as it requires a read/write for each call and isn't thread safe. It is only
    intended as a helper for simple operations such as capturing infrequent events and should not be used in a
    multithreading or multi-user environment.

    :param value: Data to write
    :param _type: The data type to write the value using
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param prefix: Value to prepend to the value
    :param suffix: Value to attach to the end of the value such as "\n"
    :param encoding: Encoding to use when writing str to bytes
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    value, content_type = format_type_for_write(_type, value, key, None)
    __append(value, bucket=bucket, key=key, prefix=prefix, suffix=suffix, encoding=encoding)


@larrydispatch
def append(value, *location, bucket=None, key=None, uri=None, prefix=None, suffix=None, encoding=DEFAULT_ENCODING, **kwargs):
    """
    Append content to the end of an s3 object. Assumes that the data should be treated as text in most cases.

    Note that this is not efficient as it requires a read/write for each call and isn't thread safe. It is only
    intended as a helper for simple operations such as capturing infrequent events and should not be used in a
    multithreading or multi-user environment.

    :param value: Data to write
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param prefix: Value to prepend to the value
    :param suffix: Value to attach to the end of the value such as "\n"
    :param encoding: Encoding to use when writing str to bytes
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    __append(value, bucket=bucket, key=key, prefix=prefix, suffix=suffix)


@append.register(str)
@append.register(int)
@append.register(float)
def _(value, *location, bucket=None, key=None, uri=None, prefix=None, suffix=None, encoding=DEFAULT_ENCODING, **kwargs):
    append_as(value, type(value), *location, bucket=bucket, key=key, uri=uri, prefix=prefix, suffix=suffix,
              encoding=encoding, **kwargs)


@append.register(Mapping)
def _(value, *location, bucket=None, key=None, uri=None, prefix=None, suffix=None, encoding=DEFAULT_ENCODING, **kwargs):
    append_as(value, type(value), *location, bucket=bucket, key=key, uri=uri, prefix=prefix, suffix=suffix,
              encoding=encoding, **kwargs)


@append.register(list)
# TODO: Replace with iter solution?
def _(value, *location, bucket=None, key=None, uri=None, prefix=None, suffix=None, encoding=DEFAULT_ENCODING, **kwargs):
    buff = StringIO()
    for row in value:
        if isinstance(row, Mapping):
            v, ct = format_type_for_write(dict, row, **kwargs)
        else:
            v, ct = format_type_for_write(str, row, **kwargs)
        buff.write(v + kwargs.get("newline", DEFAULT_NEWLINE))
    append_as(buff.getvalue(), str, *location, bucket=bucket, key=key, uri=uri, prefix=prefix, suffix=suffix,
              encoding=encoding, **kwargs)


def move(old_bucket=None, old_key=None, old_uri=None, new_bucket=None, new_key=None, new_uri=None):
    """
    Creates a copy of an S3 object in a new location and deletes the object from the existing location.

    :param old_bucket: Source bucket
    :param old_key: Source key
    :param old_uri: An s3:// path containing the bucket and key of the source object
    :param new_bucket: Target bucket
    :param new_key: Target key
    :param new_uri: An s3:// path containing the bucket and key of the source object
    :return: None
    """
    # TODO: Add support for passing location without parameter names
    if old_uri:
        (old_bucket, old_key) = split_uri(old_uri)
    if new_uri:
        (new_bucket, new_key) = split_uri(new_uri)
    copy_source = {
        'Bucket': old_bucket,
        'Key': old_key
    }
    _get_resource().meta.client.copy(copy_source, new_bucket, new_key)
    _get_resource().meta.client.delete_object(Bucket=old_bucket, Key=old_key)


def copy(src_bucket=None, src_key=None, src_uri=None, new_bucket=None, new_key=None, new_uri=None):
    """
    Copies an object in S3.

    :param src_bucket: Source bucket
    :param src_key: Source key
    :param src_uri: An s3:// path containing the bucket and key of the source object
    :param new_bucket: Target bucket
    :param new_key: Target key
    :param new_uri: An s3:// path containing the bucket and key of the source object
    :return: None
    """
    # TODO: Add support for passing location without parameter names
    if src_uri:
        (src_bucket, src_key) = split_uri(src_uri)
    if new_uri:
        (new_bucket, new_key) = split_uri(new_uri)
    _get_resource().meta.client.copy({'Bucket': src_bucket, 'Key': src_key}, new_bucket, new_key)


def exists(*location, bucket=None, key=None, uri=None):
    """
    Checks to see if an object with the given bucket/key (or uri) exists.

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: True if the key exists, if not, False
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return Object(bucket=bucket, key=key).exists


def list_objects(*location, bucket=None, prefix=None, uri=None, include_empty_objects=False):
    """
    Returns a iterable of the keys in the bucket that begin with the provided prefix.

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket to query
    :param prefix: The key prefix to use in searching the bucket
    :param uri: An s3:// path containing the bucket and prefix
    :param include_empty_objects: True if you want to include keys associated with objects of size=0
    :return: A generator of s3 Objects
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=prefix, uri=uri,
                                          key_arg="prefix", require_key=False)
    paginator = _get_resource().meta.client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket}
    if prefix:
        operation_parameters['Prefix'] = prefix
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for objct in page.get('Contents', []):
            if objct['Size'] > 0 or include_empty_objects:
                yield Object(bucket=bucket, key=objct['Key'])


def list_buckets():
    """
    Returns a iterable of the keys in the bucket that begin with the provided prefix.

    :return: A generator of Bucket objects
    """
    for bucket in _get_resource().meta.client.list_buckets().get('Buckets'):
        yield Bucket(bucket['Name'])


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
            if isinstance(value, tuple):
                uri, *z = value
                b, key = split_uri(uri)
                keys.append(tuple([key]) + tuple(z))
            else:
                b, key = split_uri(value)
                keys.append(key)

    # Find the longest common prefix to use as the search term
    prefix = _find_largest_common_prefix(keys)

    # Get a list of all keys in the bucket that match the prefix
    bucket_obj = Bucket(bucket=bucket)
    all_keys = []
    for objct in bucket_obj.objects.filter(Prefix=prefix):
        all_keys.append(objct.key)

    # Search for any keys that can't be found
    not_found = []
    for value in keys:
        key = value[0] if isinstance(value, tuple) else value
        if key not in all_keys:
            not_found.append(value)
    return not_found


def fetch(url, *location, bucket=None, key=None, uri=None, content_type=None, content_encoding=None,
          content_language=None, content_length=None, metadata=None, sse=None, storage_class=None,
          tags=None, acl=None, incl_user_agent=False, **kwargs):
    """
    Retrieves the data referenced by a URL to an S3 location.

    :param url: URL to retrieve
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for the object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: An S3 policy to apply the S3 location
    :param content_type: A standard MIME type describing the format of the object data
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
    mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :param incl_user_agent: If true, a user agent string will be added as a header to the request
    :return: The URI of the object written to S3
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    if incl_user_agent:
        if "headers" in kwargs:
            kwargs["headers"]["User-Agent"] = utils.user_agent()
        else:
            kwargs["headers"] = {"User-Agent": utils.user_agent()}
    req = request.Request(url, **kwargs)
    with request.urlopen(req) as response:
        return _write(response.read(), bucket=bucket, key=key, acl=acl,
                      content_type=content_type, content_encoding=content_encoding, content_language=content_language,
                      content_length=content_length, metadata=metadata, sse=sse, storage_class=storage_class,
                      tags=tags)


def download(file, *location, bucket=None, key=None, uri=None, use_threads=True):
    """
    Downloads the an S3 object to a directory on the local file system.

    :param file: The file, file-like object, or directory to download the object to
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param use_threads: Enables the use_threads transfer config
    :return: Path of the local file
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    config = TransferConfig(use_threads=use_threads)
    objct = Object(bucket=bucket, key=key)
    if isinstance(file, str):
        if os.path.isdir(file):
            file = os.path.join(file, key.split('/')[-1])
        objct.download_file(file, Config=config)
        return file
    else:
        objct.download_fileobj(file, Config=config)
        # TODO: Validate that this will always work, do BytesIO objects have names?
        return file.name


def download_to_temp(*location, bucket=None, key=None, uri=None):
    """
    Downloads the an S3 object to a temp directory on the local file system.

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object to be retrieved from the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :return: A file pointer to the temp file
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    fp = tempfile.TemporaryFile()
    download(fp, bucket=bucket, key=key, uri=uri)
    fp.seek(0)
    return fp


def generate_presigned_get(*location, bucket=None, key=None, uri=None, expires_in=None, http_method=None):
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    params = {
        "ClientMethod": "get_object",
        "Params": {"Bucket": bucket, "Key": key}
    }
    if expires_in:
        params["ExpiresIn"] = expires_in
    if http_method:
        params["HttpMethod"] = http_method
    return __resource.meta.client.generate_presigned_url(**params)


def upload(file, *location, bucket=None, key=None, uri=None, acl=None, content_type=None, content_encoding=None,
           content_language=None, content_length=None, metadata=None, sse=None, storage_class=None,
           tags=None):
    """
    Uploads a local file to S3

    :param file: The file or file-like object
    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object to retrieve
    :param key: The key of the object location in the bucket
    :param uri: An s3:// path containing the bucket and key of the object
    :param acl: The canned ACL to apply to the object
    :param content_type: Content type to apply to the file
    :param content_encoding: Specifies what content encodings have been applied to the object and thus what decoding
        mechanisms must be applied to obtain the media-type referenced by the Content-Type header field.
    :param content_language: The language the content is in.
    :param content_length: Size of the body in bytes.
    :param metadata: A map of metadata to store with the object in S3.
    :param sse: The server-side encryption algorithm used when storing this object in Amazon S3.
    :param storage_class: The S3 storage class to store the object in.
    :param tags: The tag-set for the object. Can be either a dict or url encoded key/value string.
    :return: The uri of the file in S3
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    extra = larry.core.map_parameters(locals(), {
        'acl': 'ACL',
        'content_encoding': 'ContentEncoding',
        'content_language': 'ContentLanguage',
        'content_length': 'ContentLength',
        'content_type': 'ContentType',
        'metadata': 'Metadata',
        'sse': 'ServerSideEncryption',
        'storage_class': 'StorageClass',
    })
    if tags:
        extra['Tagging'] = parse.urlencode(tags) if isinstance(tags, Mapping) else tags
    params = {} if len(extra.keys()) == 0 else {'ExtraArgs': extra}
    objct = Object(bucket=bucket, key=key)
    # TODO: Assign content type?
    if isinstance(file, str):
        objct.upload_file(file, **params)
    else:
        objct.upload_fileobj(file, **params)
    return objct


def write_temp(value, prefix, acl=None, bucket_identifier=None, region=None,
               bucket=None):
    """
    Write an object to a temp bucket with a unique UUID.

    :param value: Object to write to S3
    :param prefix: Prefix to attach ahead of the UUID as the key
    :param acl: The canned ACL to apply to the object
    :param bucket_identifier: The identifier to attach to the temp bucket that will be used for writing to s3, typically
        the account id (from STS) for the account being used
    :param region: The s3 region to store the data in
    :param bucket: The bucket to use instead of creating/using a temp bucket
    :return: The URI of the object written to S3
    """
    if bucket is None:
        bucket = temp_bucket(region=region, bucket_identifier=bucket_identifier)
    key = prefix + str(uuid.uuid4())
    return write(value, bucket=bucket, key=key, acl=acl)


def make_public(*location, bucket=None, key=None, uri=None):
    """
    Makes the object defined by the bucket/key pair (or uri) public.

    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    :return: The URL of the object
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return Object(bucket=bucket, key=key).make_public()


def create_bucket(bucket, acl=ACL_PRIVATE, region=None):
    """
    Creates a bucket in S3 and waits until it has been created.

    :param bucket: The name of the bucket
    :param acl: The canned ACL to apply to the object
    :param region: The region to location the S3 bucket, defaults to the region of the current session
    """
    if region is None:
        region = __session.region_name
    bucket_obj = Bucket(bucket=bucket)
    bucket_obj.create(ACL=acl, CreateBucketConfiguration={'LocationConstraint': region})
    bucket_obj.wait_until_exists()
    return bucket_obj


def delete_bucket(bucket):
    """
    Deletes an S3 bucket.

    :param bucket: The name of the bucket
    """
    bucket_obj = Bucket(bucket=bucket)
    bucket_obj.delete()
    bucket_obj.wait_until_not_exists()


def temp_bucket(region=None, bucket_identifier=None):
    """
    This will generate a temporary bucket that can be used to store intermediate data for use in various operations.
    If the bucket doesn't not already exist, one will be created in the current region with the name
    <account-id>-larry-<region>.

    :param region: Region to locate the temp bucket
    :param bucket_identifier: The bucket identifier to use as a unique identifier for the bucket, defaults to the
        account id associated with the session
    :return: The name of the created bucket
    """
    if region is None:
        region = __session.region_name
    if bucket_identifier is None:
        bucket_identifier = sts.account_id()
    bucket = '{}-larry-{}'.format(bucket_identifier, region)
    create_bucket(bucket, region=region)
    return bucket


def download_to_zip(file, bucket, prefix=None, prefixes=None):
    """
    Retrieves a list of objects contained in the bucket and downloads them to a zip file.

    :param file: The file location to write a zip file to.
    :param bucket: The name of the S3 bucket
    :param prefix: A prefix to filter objects for
    :param prefixes: A list of prefixes to filter for
    """
    if prefix:
        prefixes = [prefix]
    with ZipFile(file, 'w') as zf:
        for prefix in prefixes:
            for key in list_objects(bucket, prefix):
                zf.writestr(parse.quote(key.key), data=read(bucket, key))


def split_uri(uri):
    """
    Split an S3 URI into a bucket and key

    :param uri: S3 URI
    :return: Tuple containing a bucket and key
    """
    if isinstance(uri, str):
        m = URI_REGEX.match(uri)
        if m:
            return m.groups()
    return None, None


def is_uri(uri):
    if isinstance(uri, str):
        m = URI_REGEX.match(uri)
        if m:
            return True
    return False


def uri_bucket(uri):
    """
    Retrieve the bucket portion from an S3 URI

    :param uri: S3 URI
    :return: Bucket name
    """
    return split_uri(uri)[0]


def uri_key(uri):
    """
    Retrieves the key portion of an S3 URI

    :param uri: S3 URI
    :return: Key value
    """
    return split_uri(uri)[1]


def join_uri(bucket, *key_paths):
    """
    Compose a bucket and key into an S3 URI. The handling of the components of the key path works similarly to
    the os.path.join command with the exception of handling of absolute paths which are ignored.

    :param bucket: Bucket name
    :param key_paths: Components of the key path.
    :return: S3 URI string
    """
    # strip any leading slashes
    bucket = bucket[1:] if bucket.startswith('/') else bucket
    key_paths = [path[1:] if path.startswith('/') else path for path in key_paths]

    return 's3://{}'.format(posixpath.join(bucket, *key_paths))


def basename(key_or_uri):
    """
    Returns the file name from an S3 URI or key. Mirrors the behavior of the os.path.basename function.

    .. code-block:: python

        >>> import larry as lry
        >>> lry.s3.basename('s3://my-bucket/my-dir/sub-dir/my-file.txt')
        my-file.txt

    :param key_or_uri: An S3 URI or object key
    """
    if '/' in key_or_uri:
        return key_or_uri.split('/')[-1]
    else:
        return key_or_uri


def basename_split(key_or_uri):
    """
    Extracts the basename from an S3 key or URI and splits the contents into a tuple of the file name and extension.

    .. code-block:: python

        >>> import larry as lry
        >>> lry.s3.basename_split('s3://my-bucket/my-dir/sub-dir/my-file.txt')
        ('myfile', '.txt')

    :param key_or_uri: An S3 URI or object key
    """
    return os.path.splitext(basename(key_or_uri))


def _object_url(bucket, key):
    if '.' in bucket:
        return f'https://s3.amazonaws.com/{bucket}/{parse.quote(key)}'
    else:
        return f'https://{bucket}.s3.amazonaws.com/{parse.quote(key)}'


def _bucket_url(bucket):
    if '.' in bucket:
        return f'https://s3.amazonaws.com/{bucket}'
    else:
        return f'https://{bucket}.s3.amazonaws.com'


def url(*location, bucket=None, key=None, uri=None):
    """
    Returns the public URL of an S3 object or bucket (assuming it's public).

    :param location: Positional values for bucket, key, and/or uri
    :param bucket: The S3 bucket for object
    :param key: The key of the object
    :param uri: An s3:// path containing the bucket and key of the object
    """
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri, require_key=False)
    if key:
        return _object_url(bucket, key)
    else:
        return _bucket_url(bucket)


def read_list_as(o_type, *location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    warnings.warn("Use read_as([<type>], ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return read_as([o_type], bucket=bucket, key=key, encoding=encoding, newline=newline)


def read_iter_as(o_type, *location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    warnings.warn("Use read_as([<type>], ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return iter(read_as(bucket=bucket, key=key, encoding=encoding, newline=newline))


def read_dict(*location, bucket=None, key=None, uri=None, encoding='utf-8', use_decoder=False):
    warnings.warn("Use read_as(dict, ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return read_as(dict, bucket=bucket, key=key, uri=uri, encoding=encoding, use_decoder=use_decoder)


def read_str(*location, bucket=None, key=None, uri=None, encoding='utf-8'):
    warnings.warn("Use read_as(str, ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return read_as(str, bucket=bucket, key=key, uri=uri)


def read_list_of_dict(*location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    warnings.warn("Use read_as([dict], ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return read_as([dict], bucket=bucket, key=key, uri=uri,
                   encoding=encoding, newline=newline)


def read_list_of_str(*location, bucket=None, key=None, uri=None, encoding='utf-8', newline='\n'):
    warnings.warn("Use read_as([str], ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return read_as([str], bucket=bucket, key=key, uri=uri,
                   encoding=encoding, newline=newline)


def write_delimited(rows, *location, bucket=None, key=None, uri=None, acl=None, newline='\n', delimiter=',',
                    columns=None, headers=None, content_type=None, content_encoding=None, content_language=None,
                    content_length=None, metadata=None, sse=None, storage_class=None, tags=None):
    warnings.warn("Use write_as(row, csv, ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return write_as(rows, csv, bucket=bucket, key=key, uri=uri, acl=acl, newline=newline,
                    delimiter=delimiter, columns=columns, headers=headers, content_type=content_type,
                    content_encoding=content_encoding, content_language=content_language, content_length=content_length,
                    metadata=metadata, sse=sse, storage_class=storage_class, tags=tags)


def write_object(value, *location, bucket=None, key=None, uri=None, newline='\n', acl=None, content_type=None,
                 content_encoding=None, content_language=None, content_length=None, metadata=None, sse=None,
                 storage_class=None, tags=None, **params):
    warnings.warn("Use read_as(iter(<type>), ...)", DeprecationWarning)
    bucket, key, uri = normalize_location(*location, bucket=bucket, key=key, uri=uri)
    return write(value, bucket=bucket, key=key, uri=uri, newline=newline, acl=acl, content_type=content_type,
                 content_encoding=content_encoding, content_language=content_language, content_length=content_length,
                 metadata=metadata, sse=sse, storage_class=storage_class, tags=tags, **params)


class ErrorCodes(Enum):
    AccessDenied = 'AccessDenied'
    AccountProblem = 'AccountProblem'
    AllAccessDisabled = 'AllAccessDisabled'
    AmbiguousGrantByEmailAddress = 'AmbiguousGrantByEmailAddress'
    AuthorizationHeaderMalformed = 'AuthorizationHeaderMalformed'
    BadDigest = 'BadDigest'
    BucketAlreadyExists = 'BucketAlreadyExists'
    BucketAlreadyOwnedByYou = 'BucketAlreadyOwnedByYou'
    BucketNotEmpty = 'BucketNotEmpty'
    CredentialsNotSupported = 'CredentialsNotSupported'
    CrossLocationLoggingProhibited = 'CrossLocationLoggingProhibited'
    EntityTooSmall = 'EntityTooSmall'
    EntityTooLarge = 'EntityTooLarge'
    ExpiredToken = 'ExpiredToken'
    IllegalLocationConstraintException = 'IllegalLocationConstraintException'
    IllegalVersioningConfigurationException = 'IllegalVersioningConfigurationException'
    IncompleteBody = 'IncompleteBody'
    IncorrectNumberOfFilesInPostRequest = 'IncorrectNumberOfFilesInPostRequest'
    InlineDataTooLarge = 'InlineDataTooLarge'
    InternalError = 'InternalError'
    InvalidAccessKeyId = 'InvalidAccessKeyId'
    InvalidAddressingHeader = 'InvalidAddressingHeader'
    InvalidArgument = 'InvalidArgument'
    InvalidBucketName = 'InvalidBucketName'
    InvalidBucketState = 'InvalidBucketState'
    InvalidDigest = 'InvalidDigest'
    InvalidEncryptionAlgorithmError = 'InvalidEncryptionAlgorithmError'
    InvalidLocationConstraint = 'InvalidLocationConstraint'
    InvalidObjectState = 'InvalidObjectState'
    InvalidPart = 'InvalidPart'
    InvalidPartOrder = 'InvalidPartOrder'
    InvalidPayer = 'InvalidPayer'
    InvalidPolicyDocument = 'InvalidPolicyDocument'
    InvalidRange = 'InvalidRange'
    InvalidRequest = 'InvalidRequest'
    InvalidSecurity = 'InvalidSecurity'
    InvalidSOAPRequest = 'InvalidSOAPRequest'
    InvalidStorageClass = 'InvalidStorageClass'
    InvalidTargetBucketForLogging = 'InvalidTargetBucketForLogging'
    InvalidToken = 'InvalidToken'
    InvalidURI = 'InvalidURI'
    KeyTooLongError = 'KeyTooLongError'
    MalformedACLError = 'MalformedACLError'
    MalformedPOSTRequest = 'MalformedPOSTRequest'
    MalformedXML = 'MalformedXML'
    MaxMessageLengthExceeded = 'MaxMessageLengthExceeded'
    MaxPostPreDataLengthExceededError = 'MaxPostPreDataLengthExceededError'
    MetadataTooLarge = 'MetadataTooLarge'
    MethodNotAllowed = 'MethodNotAllowed'
    MissingAttachment = 'MissingAttachment'
    MissingContentLength = 'MissingContentLength'
    MissingRequestBodyError = 'MissingRequestBodyError'
    MissingSecurityElement = 'MissingSecurityElement'
    MissingSecurityHeader = 'MissingSecurityHeader'
    NoLoggingStatusForKey = 'NoLoggingStatusForKey'
    NoSuchBucket = 'NoSuchBucket'
    NoSuchBucketPolicy = 'NoSuchBucketPolicy'
    NoSuchKey = 'NoSuchKey'
    NoSuchLifecycleConfiguration = 'NoSuchLifecycleConfiguration'
    NoSuchUpload = 'NoSuchUpload'
    NoSuchVersion = 'NoSuchVersion'
    NotImplemented = 'NotImplemented'
    NotSignedUp = 'NotSignedUp'
    OperationAborted = 'OperationAborted'
    PermanentRedirect = 'PermanentRedirect'
    PreconditionFailed = 'PreconditionFailed'
    Redirect = 'Redirect'
    RestoreAlreadyInProgress = 'RestoreAlreadyInProgress'
    RequestIsNotMultiPartContent = 'RequestIsNotMultiPartContent'
    RequestTimeout = 'RequestTimeout'
    RequestTimeTooSkewed = 'RequestTimeTooSkewed'
    RequestTorrentOfBucketError = 'RequestTorrentOfBucketError'
    ServerSideEncryptionConfigurationNotFoundError = 'ServerSideEncryptionConfigurationNotFoundError'
    ServiceUnavailable = 'ServiceUnavailable'
    SignatureDoesNotMatch = 'SignatureDoesNotMatch'
    SlowDown = 'SlowDown'
    TemporaryRedirect = 'TemporaryRedirect'
    TokenRefreshRequired = 'TokenRefreshRequired'
    TooManyBuckets = 'TooManyBuckets'
    UnexpectedContent = 'UnexpectedContent'
    UnresolvableGrantByEmailAddress = 'UnresolvableGrantByEmailAddress'
    UserKeyMustBeSpecified = 'UserKeyMustBeSpecified'
