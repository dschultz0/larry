import pickle
import unittest
import larry as lry
from larry.types import Box
from larry.utils import json_dumps
import datetime
import numpy as np
from moto import mock_s3
import json
from PIL import Image


# S3 testing objects
SIMPLE_DICT = {
    'a': {'key': 'value'},
    'b': ['a', 'b', 'c'],
    '1': 15,
    'box': Box([3,4,5,6]),
    'date': datetime.datetime.now()
}
SIMPLE_LIST_OF_DICTS = [
    {'a': 124, 'b': 'value'},
    {'a': 939, 'b': 'foo'},
    {'a': 389, 'b': 'bar', '3': 'new'}
]
SIMPLE_LIST = ['a', 'b', 'c', '3', 'd', '2', 'e', '1']
SIMPLE_STRING = 'foobar'
NUMPY_SHAPE = (30,40)
NUMPY_ARRAY = np.random.rand(*NUMPY_SHAPE)
IMAGE_URL = 'https://hilltop-demo.s3-us-west-2.amazonaws.com/images/1557026914963.jpg'
IMAGE_PATH = "test/assets/larry.jpg"
BUCKET = 'larry-testing'
KEY = 'test-objects/s3/1557026914963.jpg'
URI = 's3://{}/{}'.format(BUCKET, KEY)
PATH_PREFIX = 's3/'
URI_PREFIX = 's3://{}/{}'.format(BUCKET, PATH_PREFIX)


@mock_s3
class S3Tests(unittest.TestCase):

    @staticmethod
    def _parameter_permutations(*args, **kwargs):
        """
        Will iterate through variations of the parameters with various approaches to passing bucket, key and uri
        Assumes that the bucket and key are passed in the kwargs.
        """
        args = list(args)
        kw_noloc = kwargs.copy()
        bucket = kw_noloc.pop("bucket")
        key = kw_noloc.pop("key")
        uri = lry.s3.join_uri(bucket, key)
        kw_uri = kw_noloc.copy()
        kw_uri["uri"] = uri
        return [
            (args, kwargs),
            (args + [bucket, key], kw_noloc),
            (args + [lry.s3.Object(bucket, key)], kw_noloc),
            (args + [lry.s3.Bucket(bucket), key], kw_noloc),
            (args + [uri], kw_noloc),
            (args, kw_uri),
        ]

    def setUp(self):
        lry.set_session()  # necessary to override initial session with moto
        lry.s3.create_bucket(BUCKET)
        lry.s3.fetch(IMAGE_URL, BUCKET, KEY)

    def test_obj(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=KEY):
            o = lry.s3.Object(*args, **kw)
            o.load()
        o = lry.s3.Object(BUCKET, KEY[:-1])
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_delete(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=PATH_PREFIX + 'delete_test.jpg'):
            o = lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
            o.load()
            lry.s3.delete(*args, **kw)
            with self.assertRaises(lry.ClientError) as context:
                o.load()
            self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_get_size(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=KEY):
            self.assertGreater(lry.s3.size(*args, **kw), 10000)

    def test_dict(self):
        key = PATH_PREFIX + 'dict.json'
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=key):
            o = lry.s3.write(SIMPLE_DICT, *args, **kw)
            self.assertEqual(json_dumps(lry.s3.read_as(dict, *args, **kw)), json_dumps(SIMPLE_DICT))
            self.assertEqual(o.content_type, "application/json")
            o.delete()
            o = lry.s3.write_as(SIMPLE_DICT, dict, *args, **kw)
            self.assertEqual(json_dumps(lry.s3.read_as(dict, *args, **kw)), json_dumps(SIMPLE_DICT))
            self.assertEqual(o.content_type, "application/json")
            o.delete()
            o = lry.s3.write_as(SIMPLE_DICT, json, *args, **kw)
            self.assertEqual(json_dumps(lry.s3.read_as(json, *args, **kw)), json_dumps(SIMPLE_DICT))
            self.assertEqual(o.content_type, "application/json")
            o.delete()

    def test_list_of_dict(self):
        def list_dump(l):
            return [json_dumps(i) for i in l]
        key = PATH_PREFIX + 'dictlist.jsonl'
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=key):
            o = lry.s3.write(SIMPLE_LIST_OF_DICTS, *args, **kw)
            self.assertEqual(list_dump(lry.s3.read_as([dict], *args, **kw)), list_dump(SIMPLE_LIST_OF_DICTS))
            o.delete()
            o = lry.s3.write_as(SIMPLE_LIST_OF_DICTS, [dict], *args, **kw)
            self.assertEqual(list_dump(lry.s3.read_as([dict], *args, **kw)), list_dump(SIMPLE_LIST_OF_DICTS))
            o.delete()
            o = lry.s3.write_as(SIMPLE_LIST_OF_DICTS, [json], *args, **kw)
            self.assertEqual(list_dump(lry.s3.read_as([json], *args, **kw)), list_dump(SIMPLE_LIST_OF_DICTS))
            o.delete()

    def test_list(self):
        key = PATH_PREFIX + 'list.txt'
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=key):
            o = lry.s3.write(SIMPLE_LIST, *args, **kw)
            self.assertEqual(lry.s3.read_as([str], *args, **kw), SIMPLE_LIST)
            o.delete()
            o = lry.s3.write_as(SIMPLE_LIST, [str], *args, **kw)
            self.assertEqual(lry.s3.read_as([str], *args, **kw), SIMPLE_LIST)
            o.delete()

    def test_string(self):
        key = PATH_PREFIX + 'list.txt'
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=key):
            o = lry.s3.write(SIMPLE_STRING, *args, **kw)
            self.assertEqual(lry.s3.read_as(str, *args, **kw), SIMPLE_STRING)
            o.delete()
            o = lry.s3.write_as(SIMPLE_STRING, str, *args, **kw)
            self.assertEqual(lry.s3.read_as(str, *args, **kw), SIMPLE_STRING)
            o.delete()

    def test_move(self):
        key1 = PATH_PREFIX + 'string1.txt'
        key2 = PATH_PREFIX + 'string2.txt'
        uri1 = lry.s3.join_uri(BUCKET, key1)
        uri2 = lry.s3.join_uri(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, BUCKET, key1)
        lry.s3.move(old_bucket=BUCKET, old_key=key1, new_bucket=BUCKET, new_key=key2)
        self.assertEqual(lry.s3.read_as(str, bucket=BUCKET, key=key2), SIMPLE_STRING)
        lry.s3.delete(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, uri1)
        lry.s3.move(old_uri=uri1, new_uri=uri2)
        self.assertEqual(lry.s3.read_as(str, uri2), SIMPLE_STRING)
        lry.s3.delete(uri2)

    def test_copy(self):
        key1 = PATH_PREFIX + 'string1.txt'
        key2 = PATH_PREFIX + 'string2.txt'
        uri1 = lry.s3.join_uri(BUCKET, key1)
        uri2 = lry.s3.join_uri(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, BUCKET, key1)
        lry.s3.copy(src_bucket=BUCKET, src_key=key1, new_bucket=BUCKET, new_key=key2)
        self.assertEqual(lry.s3.read_as(str, bucket=BUCKET, key=key1), lry.s3.read_as(str, bucket=BUCKET, key=key2))
        lry.s3.delete(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, uri1)
        lry.s3.copy(src_uri=uri1, new_uri=uri2)
        self.assertEqual(lry.s3.read_as(str, uri1), lry.s3.read_as(str, uri2))
        lry.s3.delete(uri2)
        lry.s3.delete(uri1)

    def test_exists(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=KEY):
            self.assertTrue(lry.s3.exists(*args, **kw))
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=KEY[:-1]):
            self.assertFalse(lry.s3.exists(*args, **kw))

    def test_fetch(self):
        key = PATH_PREFIX + 'fetched.jpg'
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=key):
            o = lry.s3.fetch(IMAGE_URL, *args, **kw)
            self.assertTrue(lry.s3.exists(*args, **kw))
            o.delete()

    def test_numpy(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=PATH_PREFIX + "numpy.npy"):
            o = lry.s3.write(NUMPY_ARRAY, *args, **kw)
            np.testing.assert_array_equal(NUMPY_ARRAY,
                                          lry.s3.read_as(np.ndarray, *args, **kw).reshape(NUMPY_SHAPE))
            o.delete()
            o = lry.s3.write_as(NUMPY_ARRAY, np.ndarray, *args, **kw)
            np.testing.assert_array_equal(NUMPY_ARRAY,
                                          lry.s3.read_as(np.ndarray, *args, **kw).reshape(NUMPY_SHAPE))
            o.delete()

    def test_pillow(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=PATH_PREFIX + "image.jpg"):
            with Image.open(IMAGE_PATH) as img:
                o = lry.s3.write_as(img, Image, *args, **kw)
                oi = lry.s3.read_as(Image, *args, **kw)
                self.assertEqual(oi.size, img.size)
                self.assertEqual(oi.format, img.format)
                o.delete()
                o = lry.s3.write(img, *args, **kw)
                oi = lry.s3.read_as(Image, *args, **kw)
                self.assertEqual(oi.size, img.size)
                self.assertEqual(oi.format, img.format)
                o.delete()

    def test_pickle(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=PATH_PREFIX + '.pkl'):
            o = lry.s3.write_as(SIMPLE_DICT, pickle, *args, **kw)
            self.assertEqual(json_dumps(lry.s3.read_as(pickle, *args, **kw)), json_dumps(SIMPLE_DICT))
            self.assertEqual(o.content_type, "application/octet-stream")
            o.delete()

    def test_append(self):
        for args, kw in S3Tests._parameter_permutations(bucket=BUCKET, key=PATH_PREFIX + "append.txt"):
            o = lry.s3.write("Header", *args, **kw)
            for v in SIMPLE_LIST:
                lry.s3.append_as(v, str, *args, **kw, prefix="\n")
            self.assertTrue(lry.s3.read_as(str, *args, **kw), "\n".join(["Header"]+SIMPLE_LIST))
            o.delete()
            o = lry.s3.write("Header", *args, **kw)
            for v in SIMPLE_LIST:
                lry.s3.append(v, *args, **kw, prefix="\n")
            self.assertTrue(lry.s3.read_as(str, *args, **kw), "\n".join(["Header"]+SIMPLE_LIST))
            o.delete()

    def test_bucket(self):
        bucket1 = 'larry-testing-create1'
        bucket2 = 'larry-testing-create2'
        b1 = lry.s3.create_bucket(bucket1)
        self.assertEqual(lry.session().region_name,
                         lry.s3.resource.meta.client.get_bucket_location(Bucket=bucket1)['LocationConstraint'])
        b1.delete()
        lry.s3.create_bucket(bucket1)
        lry.s3.delete_bucket(bucket1)
        lry.s3.create_bucket(bucket2, region='us-west-1')
        self.assertEqual('us-west-1',
                         lry.s3.resource.meta.client.get_bucket_location(Bucket=bucket2)['LocationConstraint'])
        lry.s3.delete_bucket(bucket2)

    def test_cors(self):
        b = lry.s3.Bucket(BUCKET)
        self.assertIsNone(b.cors)
        b.cors = lry.s3.CorsRule.default()
        self.assertEqual([rule.to_dict() for rule in b.cors], [lry.s3.CorsRule.default().to_dict()])
        b.cors = lry.s3.CorsRule.default().to_dict()
        self.assertEqual([rule.to_dict() for rule in b.cors], [lry.s3.CorsRule.default().to_dict()])
        b.cors = [lry.s3.CorsRule.default()]
        self.assertEqual([rule.to_dict() for rule in b.cors], [lry.s3.CorsRule.default().to_dict()])
        b.cors = [lry.s3.CorsRule.default().to_dict()]
        self.assertEqual([rule.to_dict() for rule in b.cors], [lry.s3.CorsRule.default().to_dict()])
        del b.cors
        self.assertIsNone(b.cors)


if __name__ == '__main__':
    unittest.main()
