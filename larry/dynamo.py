import larry.core
import boto3
from larry.core import ResourceWrapper, iterate_through_paginated_items
import csv
import itertools
from collections.abc import Mapping

# A local instance of the boto3 session to use
__session = boto3.session.Session()
# Local DynamoDB resource object
__resource = __session.resource('dynamodb')


def _get_resource(): return __resource


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
    __resource = __session.resource('dynamodb')


class Table(ResourceWrapper):
    """
    Wraps the boto3 DynamoDB
    `Table <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table>`_
    resource with helper functions.

    .. code-block:: python

        import larry as lry
        bucket = lry.dynamodb.Table('table_name')

    :param table_name: The Table's name identifier
    """

    def __init__(self, table_name):
        super().__init__(_get_resource().Table(table_name))


def _scan(table_name,
          index_name=None,
          start_key=None,
          limit=None,
          select=None,
          filter_expression=None,
          projection_expression=None,
          expression_attribute_names=None,
          expression_attribute_values=None,
          consistent_read=None):
    table = Table(table_name)
    params = larry.core.map_parameters(locals(), {
        'index_name': 'IndexName',
        'start_key': 'ExclusiveStartKey',
        'limit': 'Limit',
        'select': 'Select',
        'filter_expression': 'FilterExpression',
        'projection_expression': 'ProjectionExpression',
        'expression_attribute_names': 'ExpressionAttributeNames',
        'expression_attribute_values': 'ExpressionAttributeValues',
        'consistent_read': 'ConsistentRead',
    })
    return table.scan(**params)


def scan_iter(table_name,
              index_name=None,
              limit=None,
              select=None,
              filter_expression=None,
              projection_expression=None,
              expression_attribute_names=None,
              expression_attribute_values=None,
              consistent_read=None):
    params = locals()

    def _scan_page(last_evaluated_key=None):
        return _scan(**params, start_key=last_evaluated_key)

    for item in iterate_through_paginated_items(_scan_page, 'Items', 'LastEvaluatedKey'):
        yield item


def export_to_csv(table_name,
                  file_name,
                  newline='\n',
                  fieldnames=None,
                  evaluate_fields_for=100,
                  **attrs):
    iterator = scan_iter(table_name)
    preselect = None
    if fieldnames is None:
        preselect = list(itertools.islice(iterator, evaluate_fields_for))
        fieldnames = set()
        for rec in preselect:
            fieldnames.update(rec.keys())
        fieldnames = list(fieldnames)
        print(fieldnames)

    def _render_record(record):
        if isinstance(fieldnames, Mapping):
            result = {}
            for key, value in fieldnames.items():
                if value is None or isinstance(value, str):
                    result[key] = record[key]
                elif callable(value):
                    result[key] = value(record)
                else:
                    raise TypeError('Field mapping with value of type {} is not supported'.format(type(value)))
            return result
        else:
            return record

    with open(file_name, 'w', newline=newline) as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, **attrs)
        writer.writeheader()
        if preselect:
            for row in preselect:
                writer.writerow(_render_record(row))
        for row in iterator:
            writer.writerow(_render_record(row))


def test_export(table_name, file_name):
    export_to_csv(table_name, file_name, fieldnames={
        'agent_id': None,
        'name': None,
        'organization': None,
        'languages': None,
        'roles': None,
        'is_agent': lambda x: int('agent' in x['roles']),
        'is_customer': lambda x: int('customer' in x['roles']),
        'is_template_editor': lambda x: int('template_editor' in x['roles']),
        'is_template_creator': lambda x: int('template_creator' in x['roles']),
        'is_template_admin': lambda x: int('template_admin' in x['roles']),
        'is_admin': lambda x: int('admin' in x['roles']),
        'is_results_viewer': lambda x: int('results_viewer' in x['roles']),
        'login': None,
        'preferences': None
    })