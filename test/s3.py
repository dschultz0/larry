import unittest
import larry as lry
from botocore.exceptions import ClientError

# S3 testing objects
SIMPLE_DICT = {
    'a': {'key': 'value'},
    'b': ['a', 'b', 'c'],
    '1': 15
}
SIMPLE_LIST_OF_DICTS = [
    {'a': 124, 'b': 'value'},
    {'a': 939, 'b': 'foo'},
    {'a': 389, 'b': 'bar', '3': 'new'}
]
SIMPLE_LIST = ['a', 'b', 'c', '3', 'd', '2', 'e', '1']
SIMPLE_STRING = 'foobar'
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
        with self.assertRaises(ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_delete(self):
        uri = lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        o = lry.s3.obj(uri)
        o.load()
        lry.s3.delete(uri)
        with self.assertRaises(ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(uri=uri)
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(o.bucket_name, o.key)
        lry.s3.fetch(IMAGE_URL, BUCKET, PATH_PREFIX + 'delete_test.jpg')
        lry.s3.delete(bucket=o.bucket_name, key=o.key)
        with self.assertRaises(ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_delete_multiple(self):
        uris = [lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        print(uris)
        lry.s3.delete(uris)
        o = lry.s3.obj(uris[3])
        with self.assertRaises(ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])
        uris = [lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        lry.s3.delete(uri=uris)
        uris = [lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        bucket = lry.s3.decompose_uri(uris[0])[0]
        keys = [lry.s3.decompose_uri(uri)[1] for uri in uris]
        lry.s3.delete(bucket, keys)
        uris = [lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete1.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete2.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete3.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete4.txt'),
                lry.s3.write_object(SIMPLE_STRING, BUCKET, PATH_PREFIX + 'delete5.txt')]
        lry.s3.delete(bucket=bucket, key=keys)
        o = lry.s3.obj(uris[4])
        with self.assertRaises(ClientError) as context:
            o.load()
        self.assertEqual('Not Found', context.exception.response['Error']['Message'])

    def test_get_size(self):
        self.assertGreater(lry.s3.get_size(URI), 10000)
        self.assertGreater(lry.s3.get_size(uri=URI), 10000)
        self.assertGreater(lry.s3.get_size(BUCKET, KEY), 10000)
        self.assertGreater(lry.s3.get_size(bucket=BUCKET, key=KEY), 10000)

    def test_readwrite_dict(self):
        dict_uri = ld.s3.write_temp_object(SIMPLE_DICT, prefix)
        self.assertEqual(ld.s3.read_dict(uri=dict_uri), SIMPLE_DICT)

    def test_readwrite_list_of_dict(self):
        listofdicts_uri = ld.s3.write_temp_object(SIMPLE_LIST_OF_DICTS, prefix)
        self.assertEqual(ld.s3.read_list_of_dict(uri=listofdicts_uri), SIMPLE_LIST_OF_DICTS)

    def test_readwrite_list(self):
        list_uri = ld.s3.write_temp_object(SIMPLE_LIST, prefix)
        self.assertEqual(ld.s3.read_list_of_str(uri=list_uri), SIMPLE_LIST)

    def test_readwrite_string(self):
        string_uri = ld.s3.write_temp_object(SIMPLE_STRING, prefix)
        self.assertEqual(ld.s3.read_str(uri=string_uri), SIMPLE_STRING)

    def test_rename_object(self):
        dict_uri = ld.s3.write_temp_object(SIMPLE_DICT, prefix)
        temp_bucket, src_key = ld.s3.decompose_uri(dict_uri)
        ld.s3.rename_object(temp_bucket, src_key, temp_bucket, src_key + '.renamed')
        new_uri = ld.s3.compose_uri(temp_bucket, src_key + '.renamed')
        self.assertEqual(new_uri, dict_uri+'.renamed')

    def test_object_exists(self):
        dict_uri = ld.s3.write_temp_object(SIMPLE_DICT, prefix)
        self.assertTrue(ld.s3.object_exists(uri=dict_uri))

    def test_list_and_delete(self):
        dict_uri = ld.s3.write_temp_object(SIMPLE_DICT, prefix)
        temp_bucket, src_key = ld.s3.decompose_uri(dict_uri)
        for key in ld.s3.list_objects(temp_bucket, prefix):
            ld.s3.delete_object(temp_bucket, key)
        objects = list(ld.s3.list_objects(temp_bucket, prefix))
        self.assertEqual(len(objects),0)

    def test_fetch(self):
        fetched_uri = ld.s3.fetch(IMAGE_URL, bucket=temp_bucket, key=prefix + 'fetched.jpg')
        image = ld.s3.read_pillow_image(uri=fetched_uri)
        image_uri = ld.s3.write_pillow_image(image, 'JPEG', uri=fetched_uri + '.rewritten.jpg')
        image2 = ld.s3.read_pillow_image(uri=image_uri)
        ld.s3.download_to_temp(uri=image_uri)
        image_url = ld.s3.make_public(uri=image_uri)
        fetched_uri2 = ld.s3.fetch(image_url, bucket=temp_bucket, key=prefix + 'fetched2.jpg')
        image3 = ld.s3.read_pillow_image(uri=fetched_uri2)
        ld.s3.delete_object(uri=fetched_uri)
        ld.s3.delete_object(uri=image_uri)
        ld.s3.delete_object(uri=fetched_uri2)
        image3

if __name__ == '__main__':
    unittest.main()
