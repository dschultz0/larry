import unittest
import larrydata as ld

# S3 testing objects
my_dict = {
    'a': {'key': 'value'},
    'b': ['a', 'b', 'c'],
    '1': 15
}
my_listofdicts = [
    {'a': 124, 'b': 'value'},
    {'a': 939, 'b': 'foo'},
    {'a': 389, 'b': 'bar', '3': 'new'}
]
my_list = ['a', 'b', 'c', '3', 'd', '2', 'e', '1']
my_string = 'foobar'
my_image_url = 'https://hilltop-demo.s3-us-west-2.amazonaws.com/images/1557026914963.jpg'
prefix = 'larrydata-testing/'


class S3Tests(unittest.TestCase):

    def test_readwrite_dict(self):
        dict_uri = ld.s3.write_temp_object(my_dict, prefix)
        self.assertEqual(ld.s3.read_dict(uri=dict_uri),  my_dict)

    def test_readwrite_list_of_dict(self):
        listofdicts_uri = ld.s3.write_temp_object(my_listofdicts, prefix)
        self.assertEqual(ld.s3.read_list_of_dict(uri=listofdicts_uri), my_listofdicts)

    def test_readwrite_list(self):
        list_uri = ld.s3.write_temp_object(my_list, prefix)
        self.assertEqual(ld.s3.read_list_of_str(uri=list_uri), my_list)

    def test_readwrite_string(self):
        string_uri = ld.s3.write_temp_object(my_string, prefix)
        self.assertEqual(ld.s3.read_str(uri=string_uri), my_string)

    def test_rename_object(self):
        dict_uri = ld.s3.write_temp_object(my_dict, prefix)
        temp_bucket, src_key = ld.s3.decompose_uri(dict_uri)
        ld.s3.rename_object(temp_bucket, src_key, temp_bucket, src_key + '.renamed')
        new_uri = ld.s3.compose_uri(temp_bucket, src_key + '.renamed')
        self.assertEqual(new_uri, dict_uri+'.renamed')

    def test_object_exists(self):
        dict_uri = ld.s3.write_temp_object(my_dict, prefix)
        self.assertTrue(ld.s3.object_exists(uri=dict_uri))

    def test_list_and_delete(self):
        dict_uri = ld.s3.write_temp_object(my_dict, prefix)
        temp_bucket, src_key = ld.s3.decompose_uri(dict_uri)
        for key in ld.s3.list_objects(temp_bucket, prefix):
            ld.s3.delete_object(temp_bucket, key)
        objects = list(ld.s3.list_objects(temp_bucket, prefix))
        self.assertEqual(len(objects),0)

    def test_fetch(self):
        fetched_uri = ld.s3.fetch(my_image_url, bucket=temp_bucket, key=prefix + 'fetched.jpg')
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
