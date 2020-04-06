import unittest
import larry as lry
from larry.types import Box
from larry.utils import json_dumps, make_lambda_result_json_safe
import datetime
from botocore.exceptions import ClientError
import inspect


def simple_function(event, context):
    print('simple log')
    return 'Hello world'


def complex_function(event, context):
    print('testing log entry')
    box = Box.from_coords([3,4,5,6])
    return make_lambda_result_json_safe({
        'functionResult': inner_function(),
        'innerList': INNER_LIST,
        'innerString': INNER_STRING,
        'box': box
    })


def inner_function():
    return INNER_DICT


INNER_DICT = {
    'a': {'key': 'value'},
    'box': Box.from_coords([3,4,5,6]),
    'date': datetime.datetime.now()
}
INNER_LIST = ['a', 'b', 'c', '3', 'd', '2', 'e', '1']
INNER_STRING = 'foobar'


class S3Tests(unittest.TestCase):

    def test_generate(self):
        code = lry.lmbda.generate_code_from_function(simple_function)
        self.assertTrue('import json' in code)
        self.assertTrue("return 'Hello world'" in code)
        code = lry.lmbda.generate_code_from_function(complex_function)
        print(code)
        print(inspect.getsourcefile(complex_function))
