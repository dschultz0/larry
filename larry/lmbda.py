from larry import utils
from larry import types
import boto3
import inspect
import zipfile
from io import BytesIO
import json
import builtins
import dis
from collections import Mapping
import base64
from botocore.exceptions import ClientError

client = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()

TYPE_REQUEST_RESPONSE = 'RequestResponse'
TYPE_DRY_RUN = 'DryRun'
TYPE_EVENT = 'Event'


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
    global __session, client
    __session = boto_session if boto_session is not None else boto3.session.Session(**utils.copy_non_null_keys(locals()))
    client = __session.client('lambda')


def get(name):
    return client.get_function(FunctionName=name)


def get_if_exists(name):
    try:
        return get(name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return None
        else:
            raise e


def create_from_function(name, function, role, runtime='python3.8', timeout=None, memory_size=None, publish=True,
                         description=None):
    package, handler = package_function(function)
    return create(name, package, handler, role, runtime, timeout, memory_size, publish, description)


def package_function(function):
    obj = BytesIO()
    zf = zipfile.ZipFile(obj, "a")
    zf.writestr('handler.py', generate_code_from_function(function), zipfile.ZIP_DEFLATED)
    for zfile in zf.filelist:
        zfile.create_system = 0
        zfile.external_attr = 0
    zf.close()
    obj.seek(0)
    return obj.read(), 'handler.' + function.__name__


def create_or_update(name, package=None, handler=None, role=None, runtime='python3.8', timeout=None, memory_size=None,
                     publish=True, description=None):
    # TODO: Add create_role=True parameter that will generate a service role with the same name
    # TODO: Ideally also inspect the code to see what boto clients it creates. In the generate step with imports?
    existing = get_if_exists(name)
    if existing:
        if package is not None:
            update(name, package, publish=publish)
        # TODO: Update the other config values if they've changed
        return existing['Configuration']['FunctionArn']
    else:
        return create(name, package, handler, role, runtime=runtime, timeout=timeout, memory_size=memory_size,
                      publish=publish, description=description)


def create(name, package, handler, role, runtime='python3.8', timeout=None, memory_size=None, publish=True,
           description=None):
    params = {
        'FunctionName': name,
        'Runtime': runtime,
        'Role': role,
        'Handler': handler,
        'Code': {'ZipFile': package},
        'Publish': publish
    }
    if description:
        params['Description'] = description
    if timeout:
        params['Timeout'] = timeout
    if memory_size:
        params['MemorySize'] = memory_size
    resp = client.create_function(**params)
    return resp['FunctionArn']


def update(name, package, publish=True, dry_run=False):
    params = utils.map_parameters(locals(), {
        'publish': 'Publish',
        'dry_run': 'DryRun',
        'name': 'FunctionName'
    })
    params['ZipFile'] = package
    resp = client.update_function_code(**params)
    return resp['FunctionArn']


def delete(name):
    client.delete_function(FunctionName=name)


def get_as_function(name, o_type=types.TYPE_DICT):
    def func(**kwargs):
        return invoke_as(name, o_type, payload=kwargs, invoke_type=TYPE_REQUEST_RESPONSE, logs=False)
    return func


def invoke(name, payload=None, invoke_type=TYPE_REQUEST_RESPONSE, logs=False, context=None):
    params = utils.map_parameters(locals(), {
        'context': 'ClientContext ',
        'invoke_type': 'InvocationType',
        'name': 'FunctionName'
    })
    if payload:
        if isinstance(payload, (Mapping, list)):
            params['Payload'] = json.dumps(payload, cls=utils.JSONEncoder)
        else:
            params['Payload'] = payload
    if logs:
        params['LogType'] = 'Tail'
        resp = client.invoke(**params)
        return resp['Payload'], base64.b64decode(resp['LogResult']).decode('utf-8').split('\n')
    else:
        resp = client.invoke(**params)
        return resp['Payload']


def invoke_as(name, o_type, payload=None, invoke_type=TYPE_REQUEST_RESPONSE, logs=False, context=None):
    result = invoke(name, payload=payload, invoke_type=invoke_type, logs=logs, context=context)
    log = None
    if logs:
        payload, log = result
    else:
        payload = result
    result = None
    if o_type == types.TYPE_STRING:
        result = payload.read().decode('utf-8')
    elif o_type == types.TYPE_DICT:
        result = json.loads(payload.read())
    else:
        raise Exception('Unhandled type')
    if logs:
        return result, log
    else:
        return result


def invoke_as_string(name, payload=None, invoke_type=TYPE_REQUEST_RESPONSE, logs=False, context=None):
    return invoke_as(name, types.TYPE_STRING, payload=payload, invoke_type=invoke_type, logs=logs, context=context)


def invoke_as_json(name, payload=None, invoke_type=TYPE_REQUEST_RESPONSE, logs=False, context=None):
    return invoke_as(name, types.TYPE_DICT, payload=payload, invoke_type=invoke_type, logs=logs, context=context)


def _get_function_calls(func, built_ins=False):
    # the used instructions
    ins = list(dis.get_instructions(func))[::-1]
    # dict for function names (so they are unique)
    names = {}

    # go through call stack
    for i, inst in list(enumerate(ins))[::-1]:
        # find last CALL_FUNCTION
        if inst.opname[:13] == "CALL_FUNCTION":

            # function takes ins[i].arg number of arguments
            ep = i + inst.arg + (2 if inst.opname[13:16] == "_KW" else 1)

            # parse argument list (Python2)
            if inst.arg == 257:
                k = i+1
                while k < len(ins) and ins[k].opname != "BUILD_LIST":
                    k += 1

                ep = k-1

            # LOAD that loaded this function
            entry = ins[ep]

            # ignore list comprehensions / ...
            name = str(entry.argval)
            if "." not in name and entry.opname == "LOAD_GLOBAL" and (built_ins or not hasattr(builtins, name)):
                # save name of this function
                names[name] = True

            # reduce this CALL_FUNCTION and all its paramters to one entry
            ins = ins[:i] + [entry] + ins[ep + 1:]
    return sorted(list(names.keys()))


def generate_code_from_function(handler, imports=('json', 'boto3')):
    code = ''

    # build the list of imports
    for val in imports:
        if val.startswith('import') or val.startswith('from'):
            code += val + '\n'
        else:
            code += 'import ' + val + '\n'

    code += '\n' + inspect.getsource(handler)

    frame = inspect.currentframe()
    try:
        ns = globals()
        cf = frame
        for fn in _get_function_calls(handler, False):
            while fn not in ns:
                cf = cf.f_back
                if cf:
                    ns = cf.f_globals
                else:
                    raise Exception('Unable to location the function "{}"'.format(fn))
            code += '\n' + inspect.getsource(ns[fn])
    finally:
        del frame
    return code

