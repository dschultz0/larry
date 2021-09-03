import unittest
import larry as lry
from larry.types import Box
from larry.utils import json_dumps
import datetime
import numpy as np

# S3 testing objects
SIMPLE_DICT = {
    'a': {'key': 'value'},
    'b': ['a', 'b', 'c'],
    '1': 15,
    'box': Box.from_coords([3,4,5,6]),
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
BUCKET = 'larry-testing'
KEY = 'test-objects/s3/1557026914963.jpg'
URI = 's3://{}/{}'.format(BUCKET, KEY)
PATH_PREFIX = 's3/'
URI_PREFIX = 's3://{}/{}'.format(BUCKET, PATH_PREFIX)


class S3Tests(unittest.TestCase):

    def test_obj(self):
        o = lry.s3.obj(BUCKET, KEY)
        o.load()
        o = lry.s3.obj(URI)
        o.load()
        o = lry.s3.obj(bucket=BUCKET, key=KEY)
        o.load()
        o = lry.s3.obj(uri=URI)
        o.load()
        o = lry.s3.obj(BUCKET, KEY[:-1])
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_delete(self):
        uri = lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        o = lry.s3.obj(uri)
        o.load()
        lry.s3.delete(uri)
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(uri=uri)
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(o.uri_bucket, o.key)
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(bucket=o.uri_bucket, key=o.key)
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_delete_multiple(self):
        uris = [lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        lry.s3.delete(uris)
        o = lry.s3.obj(uris[3])
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])
        uris = [lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        lry.s3.delete(uri=uris)
        uris = [lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        bucket = lry.s3.split_uri(uris[0])[0]
        keys = [lry.s3.split_uri(uri)[1] for uri in uris]
        lry.s3.delete(bucket, keys)
        uris = [lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        lry.s3.delete(bucket=bucket, key=keys)
        o = lry.s3.obj(uris[4])
        with self.assertRaises(lry.ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_get_size(self):
        self.assertGreater(lry.s3.size(URI), 10000)
        self.assertGreater(lry.s3.size(uri=URI), 10000)
        self.assertGreater(lry.s3.size(BUCKET, KEY), 10000)
        self.assertGreater(lry.s3.size(bucket=BUCKET, key=KEY), 10000)

    def test_readwrite_dict(self):
        key = PATH_PREFIX + 'dict.json'
        uri = lry.s3.compose_uri(BUCKET, key)
        dict_uri = lry.s3.write(SIMPLE_DICT, BUCKET, key)
        self.assertEqual(json_dumps(lry.s3.read_dict(BUCKET, key)), json_dumps(SIMPLE_DICT))
        lry.s3.delete(dict_uri)
        dict_uri = lry.s3.write(SIMPLE_DICT, bucket=BUCKET, key=key)
        self.assertEqual(json_dumps(lry.s3.read_dict(bucket=BUCKET, key=key)), json_dumps(SIMPLE_DICT))
        lry.s3.delete(dict_uri)
        dict_uri = lry.s3.write(SIMPLE_DICT, uri)
        self.assertEqual(json_dumps(lry.s3.read_dict(uri)), json_dumps(SIMPLE_DICT))
        lry.s3.delete(dict_uri)
        dict_uri = lry.s3.write(SIMPLE_DICT, uri=uri)
        self.assertEqual(json_dumps(lry.s3.read_dict(uri=uri)), json_dumps(SIMPLE_DICT))
        lry.s3.delete(dict_uri)

    def test_readwrite_list_of_dict(self):
        def list_dump(l):
            return [json_dumps(i) for i in l]
        key = PATH_PREFIX + 'dictlist.jsonl'
        uri = lry.s3.compose_uri(BUCKET, key)
        dictlist_uri = lry.s3.write(SIMPLE_LIST_OF_DICTS, BUCKET, key)
        self.assertEqual(list_dump(lry.s3.read_list_of_dict(BUCKET, key)), list_dump(SIMPLE_LIST_OF_DICTS))
        lry.s3.delete(dictlist_uri)
        dictlist_uri = lry.s3.write(SIMPLE_LIST_OF_DICTS, bucket=BUCKET, key=key)
        self.assertEqual(list_dump(lry.s3.read_list_of_dict(bucket=BUCKET, key=key)), list_dump(SIMPLE_LIST_OF_DICTS))
        lry.s3.delete(dictlist_uri)
        dictlist_uri = lry.s3.write(SIMPLE_LIST_OF_DICTS, uri)
        self.assertEqual(list_dump(lry.s3.read_list_of_dict(uri)), list_dump(SIMPLE_LIST_OF_DICTS))
        lry.s3.delete(dictlist_uri)
        dictlist_uri = lry.s3.write(SIMPLE_LIST_OF_DICTS, uri=uri)
        self.assertEqual(list_dump(lry.s3.read_list_of_dict(uri=uri)), list_dump(SIMPLE_LIST_OF_DICTS))
        lry.s3.delete(dictlist_uri)

    def test_readwrite_list(self):
        key = PATH_PREFIX + 'list.txt'
        uri = lry.s3.compose_uri(BUCKET, key)
        list_uri = lry.s3.write(SIMPLE_LIST, BUCKET, key)
        self.assertEqual(lry.s3.read_list_of_str(BUCKET, key), SIMPLE_LIST)
        lry.s3.delete(list_uri)
        list_uri = lry.s3.write(SIMPLE_LIST, bucket=BUCKET, key=key)
        self.assertEqual(lry.s3.read_list_of_str(bucket=BUCKET, key=key), SIMPLE_LIST)
        lry.s3.delete(list_uri)
        list_uri = lry.s3.write(SIMPLE_LIST, uri)
        self.assertEqual(lry.s3.read_list_of_str(uri), SIMPLE_LIST)
        lry.s3.delete(list_uri)
        list_uri = lry.s3.write(SIMPLE_LIST, uri=uri)
        self.assertEqual(lry.s3.read_list_of_str(uri=uri), SIMPLE_LIST)
        lry.s3.delete(list_uri)

    def test_readwrite_string(self):
        key = PATH_PREFIX + 'string.txt'
        uri = lry.s3.compose_uri(BUCKET, key)
        string_uri = lry.s3.write(SIMPLE_STRING, BUCKET, key)
        self.assertEqual(lry.s3.read_str(BUCKET, key), SIMPLE_STRING)
        lry.s3.delete(string_uri)
        string_uri = lry.s3.write(SIMPLE_STRING, bucket=BUCKET, key=key)
        self.assertEqual(lry.s3.read_str(bucket=BUCKET, key=key), SIMPLE_STRING)
        lry.s3.delete(string_uri)
        string_uri = lry.s3.write(SIMPLE_STRING, uri)
        self.assertEqual(lry.s3.read_str(uri), SIMPLE_STRING)
        lry.s3.delete(string_uri)
        string_uri = lry.s3.write(SIMPLE_STRING, uri=uri)
        self.assertEqual(lry.s3.read_str(uri=uri), SIMPLE_STRING)
        lry.s3.delete(string_uri)

    def test_rename(self):
        key1 = PATH_PREFIX + 'string1.txt'
        key2 = PATH_PREFIX + 'string2.txt'
        uri1 = lry.s3.compose_uri(BUCKET, key1)
        uri2 = lry.s3.compose_uri(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, BUCKET, key1)
        lry.s3.rename(old_bucket=BUCKET, old_key=key1, new_bucket=BUCKET, new_key=key2)
        self.assertEqual(lry.s3.read_str(bucket=BUCKET, key=key2), SIMPLE_STRING)
        lry.s3.delete(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, uri1)
        lry.s3.rename(old_uri=uri1, new_uri=uri2)
        self.assertEqual(lry.s3.read_str(uri2), SIMPLE_STRING)
        lry.s3.delete(uri2)

    def test_copy(self):
        key1 = PATH_PREFIX + 'string1.txt'
        key2 = PATH_PREFIX + 'string2.txt'
        uri1 = lry.s3.compose_uri(BUCKET, key1)
        uri2 = lry.s3.compose_uri(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, BUCKET, key1)
        lry.s3.copy(src_bucket=BUCKET, src_key=key1, new_bucket=BUCKET, new_key=key2)
        self.assertEqual(lry.s3.read_str(bucket=BUCKET, key=key1), lry.s3.read_str(bucket=BUCKET, key=key2))
        lry.s3.delete(BUCKET, key2)
        lry.s3.write(SIMPLE_STRING, uri1)
        lry.s3.copy(src_uri=uri1, new_uri=uri2)
        self.assertEqual(lry.s3.read_str(uri1), lry.s3.read_str(uri2))
        lry.s3.delete(uri2)
        lry.s3.delete(uri1)

    def test_exists(self):
        self.assertTrue(lry.s3.exists(BUCKET, KEY))
        self.assertFalse(lry.s3.exists(BUCKET, KEY[:-1]))
        self.assertTrue(lry.s3.exists(bucket=BUCKET, key=KEY))
        self.assertFalse(lry.s3.exists(bucket=BUCKET, key=KEY[:-1]))
        self.assertTrue(lry.s3.exists(URI))
        self.assertFalse(lry.s3.exists(URI[:-1]))
        self.assertTrue(lry.s3.exists(uri=URI))
        self.assertFalse(lry.s3.exists(uri=URI[:-1]))

    def test_fetch(self):
        key = PATH_PREFIX + 'fetched.jpg'
        uri = lry.s3.compose_uri(BUCKET, key)
        image_uri = lry.s3.fetch(IMAGE_URL, BUCKET, key)
        self.assertTrue(lry.s3.exists(BUCKET, key))
        lry.s3.delete(image_uri)
        image_uri = lry.s3.fetch(IMAGE_URL, bucket=BUCKET, key=key)
        self.assertTrue(lry.s3.exists(BUCKET, key))
        lry.s3.delete(image_uri)
        image_uri = lry.s3.fetch(IMAGE_URL, uri)
        self.assertTrue(lry.s3.exists(uri))
        lry.s3.delete(image_uri)
        image_uri = lry.s3.fetch(IMAGE_URL, uri=uri)
        self.assertTrue(lry.s3.exists(uri))
        lry.s3.delete(image_uri)

    def test_readwrite_numpy(self):
        key = PATH_PREFIX + 'numpy.npy'
        uri = lry.s3.compose_uri(BUCKET, key)
        numpy_uri = lry.s3.write(NUMPY_ARRAY, BUCKET, key)
        np.testing.assert_array_equal(NUMPY_ARRAY,
                                      lry.s3.read_as(lry.types.TYPE_NP_ARRAY, BUCKET, key).reshape(NUMPY_SHAPE))
        lry.s3.delete(numpy_uri)
        numpy_uri = lry.s3.write(NUMPY_ARRAY, bucket=BUCKET, key=key)
        np.testing.assert_array_equal(NUMPY_ARRAY,
                                      lry.s3.read_as(lry.types.TYPE_NP_ARRAY, bucket=BUCKET, key=key).reshape(NUMPY_SHAPE))
        lry.s3.delete(numpy_uri)
        numpy_uri = lry.s3.write(NUMPY_ARRAY, uri)
        np.testing.assert_array_equal(NUMPY_ARRAY,
                                      lry.s3.read_as(lry.types.TYPE_NP_ARRAY, uri).reshape(NUMPY_SHAPE))
        lry.s3.delete(numpy_uri)
        numpy_uri = lry.s3.write(NUMPY_ARRAY, uri=uri)
        np.testing.assert_array_equal(NUMPY_ARRAY,
                                      lry.s3.read_as(lry.types.TYPE_NP_ARRAY, uri=uri).reshape(NUMPY_SHAPE))
        lry.s3.delete(numpy_uri)

    def test_readwrite_pillow(self):
        key = PATH_PREFIX + 'pillow.jpg'
        uri = lry.s3.compose_uri(BUCKET, key)
        img = lry.s3.read_as(lry.types.TYPE_PILLOW_IMAGE, BUCKET, KEY)
        w_uri = lry.s3.write(img, BUCKET, key)
        self.assertTrue(lry.s3.exists(BUCKET, key))

    def test_createdelete_bucket(self):
        bucket1 = 'larry-testing-create1'
        bucket2 = 'larry-testing-create2'
        if not lry.s3.bukt(bucket1).exists:
            lry.s3.create_bucket(bucket1)
        print(lry.s3.resource.meta.client.get_bucket_location(Bucket=bucket1)['LocationConstraint'])
        lry.s3.delete_bucket(bucket1)
        lry.set_session(region_name='us-west-1')
        if not lry.s3.bukt(bucket2).exists:
            lry.s3.create_bucket(bucket2, region='us-west-1')
        print(lry.s3.resource.meta.client.get_bucket_location(Bucket=bucket2)['LocationConstraint'])
        lry.s3.delete_bucket(bucket2)


if __name__ == '__main__':
    unittest.main()
