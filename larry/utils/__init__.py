import json
import datetime
from larry.mturk.HIT import HIT
from larry.mturk.Assignment import Assignment
from larry.types import Box


DATE_FORMAT = '%Y-%m-%d %H:%M:%S%z'
TIME_FORMAT = '%H:%M:%S'


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return date_to_string(obj)
        elif isinstance(obj, datetime.timedelta):
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
            box = {i: obj[i] for i in obj}
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


def date_to_string(obj):
    return obj.strftime(DATE_FORMAT)


def parse_date(obj):
    return datetime.datetime.strptime(obj, DATE_FORMAT)


def copy_non_null_keys(param_list):
    result = {}
    for key, val in param_list.items():
        if val is not None:
            result[key] = val
    return result


def map_parameters(parameters, key_map):
    result = {}
    for k, i in key_map.items():
        if parameters.get(k) is not None:
            result[i] = parameters[k]
    return result
