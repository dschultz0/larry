import json
from datetime import datetime, timedelta
from larry.mturk.HIT import HIT
from larry.mturk.Assignment import Assignment
from larry.types import Box
import re
from urllib import request


DATE_FORMAT = '%Y-%m-%d %H:%M:%S%z'
TIME_FORMAT = '%H:%M:%S'
__user_agent = None


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime):
            return date_to_string(obj)
        elif isinstance(obj, timedelta):
            return round(obj.total_seconds(), 3)
        elif isinstance(obj, HIT):
            hit = {i: obj[i] for i in obj if i != 'Question'}
            hit['__HIT__'] = True
            return hit
        elif isinstance(obj, Assignment):
            assignment = {i: obj[i] for i in obj}
            assignment['__Assignment__'] = True
            return assignment
        elif isinstance(obj, Box):
            box = obj.data
            box['__Box__'] = True
            return box
        return json.JSONEncoder.default(self, obj)


def JSONDecoder(dct):
    if '__HIT__' in dct:
        return HIT(dct)
    if '__Assignment__' in dct:
        return Assignment(dct)
    if '__Box__' in dct:
        return Box(dct)
    return dct


def json_loads(value, **kwargs):
    return json.loads(value, object_hook=JSONDecoder, **kwargs)


def json_dumps(value, **kwargs):
    return json.dumps(value, cls=JSONEncoder, **kwargs)


def make_lambda_result_json_safe(value, encoder=JSONEncoder):
    """
    Because Lambda uses a default json encoder to serialize return values it will often fail when
    returning user defined classes. This function will encode the object to JSON using the larry encoder
    and then load it back into a standard object tree that Lambda will be able to encode.
    :param value: The object to make safe
    :param encoder: The encoder to use to encode user defined classes, defaults to the larry JSONEncoder
    :return: An object with custom classes stripped out
    """
    return json.loads(json.dumps(value, cls=encoder))


def date_to_string(obj):
    return obj.strftime(DATE_FORMAT)


def parse_date(obj):
    return datetime.strptime(obj, DATE_FORMAT)


def create_s3_key(path, extension):
    """Given a path and a file type, strips out all non A-Z0-9 characters and
    appends the file type to the end: making paths and urls s3 friendly"""
    return re.sub(r'\W+', '', path) + '.' + extension


def list_chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def user_agent():
    global __user_agent
    if __user_agent is None:
        try:
            # Attempt to get a recent user agent if possible
            with request.urlopen("https://jnrbsn.github.io/user-agents/user-agents.json") as response:
                __user_agent = json.loads(response.read())[0]
        except:
            __user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
    return __user_agent
