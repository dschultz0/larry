from larry import utils
from larry import types
from larry.s3 import decompose_uri
import boto3
import inspect
import zipfile
from io import BytesIO
import json
import builtins
import dis
from collections import Mapping, UserDict
import base64
from botocore.exceptions import ClientError

client = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()

INVOKE_TYPE_REQUEST_RESPONSE = 'RequestResponse'
INVOKE_TYPE_DRY_RUN = 'DryRun'
INVOKE_TYPE_EVENT = 'Event'

RUNTIME_NODEJS = 'nodejs'
RUNTIME_NODEJS_4_3 = 'nodejs4.3'
RUNTIME_NODEJS_6_10 = 'nodejs6.10'
RUNTIME_NODEJS_8_10 = 'nodejs8.10'
RUNTIME_NODEJS_10_X = 'nodejs10.x'
RUNTIME_NODEJS_12_X = 'nodejs12.x'
RUNTIME_JAVA_8 = 'java8'
RUNTIME_JAVA_11 = 'java11'
RUNTIME_PYTHON_2_7 = 'python2.7'
RUNTIME_PYTHON_3_6 = 'python3.6'
RUNTIME_PYTHON_3_7 = 'python3.7'
RUNTIME_PYTHON_3_8 = 'python3.8'
RUNTIME_DOTNETCORE_1_0 = 'dotnetcore1.0'
RUNTIME_DOTNETCORE_2_0 = 'dotnetcore2.0'
RUNTIME_DOTNETCORE_2_1 = 'dotnetcore2.1'
RUNTIME_DOTNETCORE_3_1 = 'dotnetcore3.1'
RUNTIME_NODEJS_4_3_EDGE = 'nodejs4.3-edge'
RUNTIME_GO_1_X = 'go1.x'
RUNTIME_RUBY_2_5 = 'ruby2.5'
RUNTIME_RUBY_2_7 = 'ruby2.7'
RUNTIME_PROVIDED = 'provided'


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
    """
    Returns information about the function or function version.
    :param name: The name or ARN of the Lambda function, version, or alias.
    :return: An object representing the current configuration of a Lambda.
    """
    return Lambda.get(name)


def get_if_exists(name):
    """
    Returns information about the function or function version if it exists,
    None if it does not.
    :param name: The name or ARN of the Lambda function, version, or alias.
    :return: An object representing the current configuration of a Lambda.
    """
    try:
        return get(name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            return None
        else:
            raise e


def create(name, package, handler, role, runtime=RUNTIME_PYTHON_3_8, timeout=None, memory_size=None, publish=True,
           description=None):
    """
    Creates a Lambda function from the provided deployment package.
    :param name: The name of the Lambda function.
    :param package: A zip package or S3 uri containing the package
    :param handler: The name of the method within your code that Lambda calls to execute your function.
    :param role: The Amazon Resource Name (ARN) of the function's execution role.
    :param runtime: The identifier for the runtime to use.
    :param timeout: The amount of time that Lambda allows a function to run before stopping it.
    The default is 3 seconds. The maximum allowed value is 900 seconds.
    :param memory_size:  The amount of memory that your function has access to. Increasing the function's memory also
    increases its CPU allocation. The default value is 128 MB. The value must be a multiple of 64 MB.
    :param publish: Set to true to publish the first version of the function during creation.
    :param description: A description of the function.
    :return: An object representing the configuration of the created Lambda
    """
    params = {
        'FunctionName': name,
        'Runtime': runtime,
        'Role': role,
        'Handler': handler,
        'Publish': publish
    }
    # If the package is an S3 URI use that, else treat it as a zipfile
    (bucket, key) = decompose_uri(package)
    if bucket is None or key is None:
        params['Code'] = {'ZipFile': package}
    else:
        params['Code'] = {'S3Bucket': bucket, 'S3Key': key}

    if description:
        params['Description'] = description
    if timeout:
        params['Timeout'] = timeout
    if memory_size:
        params['MemorySize'] = memory_size
    return Lambda.from_create(client.create_function(**params))


def generate_code_from_function(handler, imports=None):
    """
    Retrieves the code for a local function and builds a string containing the code and imports. Note that this is
    an experimental feature and should be used with caution. This will pull in the code for the specified
    function from your current python environment, as well any local functions that it relies on. It will not
    pick up any globally defined variables so use care not to include those in your code.
    :param handler: A local Python function
    :param imports: List of imports to include in the package code. Values can include import statements
    ('import boto3'), packages ('boto3'), import-as shorthand ('larry:lry'), or from-package-import-function
    shorthand ('urllib>parse').
    :return: A str containing the code
    """
    code = ''

    if imports is None:
        imports = ['json', 'boto3']

    # build the list of imports
    for val in imports:
        if val.startswith('import') or val.startswith('from'):
            code += val + '\n'
        elif val.count(':') == 1:
            code += 'import {} as {}\n'.format(*val.split(':'))
        elif val.count('>') == 1:
            code += 'from {} import {}\n'.format(*val.split('>'))
        else:
            code += 'import ' + val + '\n'

    code += '\n' + inspect.getsource(handler)

    frame = inspect.currentframe()
    try:
        namespace = globals()
        current_frame = frame
        for fn in _get_function_calls(handler, False):
            while fn not in namespace:
                current_frame = current_frame.f_back
                if current_frame:
                    namespace = current_frame.f_globals
                else:
                    raise Exception('Unable to locate the function "{}"'.format(fn))
            code += '\n' + inspect.getsource(namespace[fn])
    finally:
        del frame
    return code


def package_function(function, imports=None):
    """
    Retrieves the code for a Python function and packages it into a zipfile for upload to Lambda. Note that this is
    an experimental feature and should be used with caution. This will pull in the code for the specified
    function from your current python environment, as well any local functions that it relies on. It will not
    pick up any globally defined variables so use care not to include those in your code.
    :param function: A local Python function
    :param imports: List of imports to include in the package code. Values can include import statements
    ('import boto3'), packages ('boto3'), import-as shorthand ('larry:lry'), or from-package-import-function
    shorthand ('urllib>parse').
    :return: Zip package
    """
    obj = BytesIO()
    zf = zipfile.ZipFile(obj, "a")
    zf.writestr('handler.py', generate_code_from_function(function, imports=imports), zipfile.ZIP_DEFLATED)
    for zfile in zf.filelist:
        zfile.create_system = 0
        zfile.external_attr = 0
    zf.close()
    obj.seek(0)
    return obj.read(), 'handler.' + function.__name__


def create_or_update(name, package=None, handler=None, role=None, runtime=None, timeout=None, memory_size=None,
                     publish=True, description=None):
    """
    Creates or updates a Lambda function with the provided deployment package or configuration.
    :param name: The name of the Lambda function.
    :param package: A zip package or S3 uri containing the package
    :param handler: The name of the method within your code that Lambda calls to execute your function.
    :param role: The Amazon Resource Name (ARN) of the function's execution role.
    :param runtime: The identifier for the runtime to use.
    :param timeout: The amount of time that Lambda allows a function to run before stopping it.
    The default is 3 seconds. The maximum allowed value is 900 seconds.
    :param memory_size:  The amount of memory that your function has access to. Increasing the function's memory also
    increases its CPU allocation. The default value is 128 MB. The value must be a multiple of 64 MB.
    :param publish: Set to true to publish the first version of the function during creation.
    :param description: A description of the function.
    :return: An object representing the configuration of the Lambda
    """
    # TODO: Add create_role=True parameter that will generate a service role with the same name
    # TODO: Ideally also inspect the code to see what boto clients it creates. In the generate step with imports?
    existing = get_if_exists(name)
    if existing:
        if package is not None:
            update_code(name, package, publish=publish)
        if not (handler is None and role is None and runtime is None and timeout is None and memory_size is None):
            update_config(name, handler=handler, role=role, runtime=runtime, timeout=timeout, memory_size=memory_size)
        return existing
    else:
        if runtime is None:
            runtime = 'python3.8'
        return create(name, package, handler, role, runtime=runtime, timeout=timeout, memory_size=memory_size,
                      publish=publish, description=description)


def update_code(name, package, publish=True, dry_run=False):
    """
    Updates a Lambda code with the provided code.
    :param name: The name of the Lambda function.
    :param package: A zip package or S3 uri containing the package
    :param publish: Set to true to publish a new version of the function after updating the code.
    :param dry_run: Set to true to validate the request parameters and access permissions without modifying the
    function code.
    :return: An object representing the configuration of the Lambda
    """
    params = utils.map_parameters(locals(), {
        'publish': 'Publish',
        'dry_run': 'DryRun',
        'name': 'FunctionName'
    })

    # If the package is an S3 URI use that, else treat it as a zipfile
    (bucket, key) = decompose_uri(package)
    if bucket is None or key is None:
        params['ZipFile'] = package
    else:
        params['S3Bucket'] = bucket
        params['S3Key'] = key

    return Lambda.from_create(client.update_function_code(**params))


def update_config(name, handler=None, role=None, runtime='python3.8', timeout=None, memory_size=None):
    """
    Modify the version-specific settings of a Lambda function.
    :param name: The name of the Lambda function.
    :param handler: The name of the method within your code that Lambda calls to execute your function.
    :param role: The Amazon Resource Name (ARN) of the function's execution role.
    :param runtime: The identifier for the runtime to use.
    :param timeout: The amount of time that Lambda allows a function to run before stopping it.
    The default is 3 seconds. The maximum allowed value is 900 seconds.
    :param memory_size:  The amount of memory that your function has access to. Increasing the function's memory also
    increases its CPU allocation. The default value is 128 MB. The value must be a multiple of 64 MB.
    :return: An object representing the configuration of the Lambda
    """
    config_params = utils.map_parameters(locals(), {
        'name': 'FunctionName',
        'handler': 'Handler',
        'role': 'Role',
        'runtime': 'Runtime',
        'timeout': 'Timeout',
        'memory_size': 'MemorySize'
    })
    return Lambda.from_create(client.update_function_configuration(**config_params))


def delete(name):
    """
    Delete the function with the provided name
    :param name: The name or ARN of the function
    """
    client.delete_function(FunctionName=name)


def as_function(name, o_type=types.TYPE_DICT):
    """
    Creates a python function that will invoke the Lambda and return the results in the specified format.
    :param name: The name or ARN of the function
    :param o_type: A value defined in larry.types to specify how the Lambda response will be read
    :return: A function object
    """
    def func(**kwargs):
        return invoke_as(name, o_type, payload=kwargs, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False)
    return func


def invoke(name, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
    """
    Invokes a Lambda function.
    :param name: The name or ARN of the function
    :param payload: A dict or JSON string that you want to provide to the Lambda function
    :param invoke_type: The invocation type
    :param logs: Set to true to include the execution log in the response
    :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
    function in the context object.
    :return: The response payload, also the logs if requested
    """
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


def invoke_as(name, o_type, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
    """
    Invokes a Lambda function and formats the response into the requested type.
    :param name: The name or ARN of the function
    :param o_type: A value defined in larry.types to specify how the Lambda response will be read
    :param payload: A dict or JSON string that you want to provide to the Lambda function
    :param invoke_type: The invocation type
    :param logs: Set to true to include the execution log in the response
    :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
    function in the context object.
    :return: The response in the requested type, also the logs if requested
    """
    result = invoke(name, payload=payload, invoke_type=invoke_type, logs=logs, context=context)
    log = None
    if logs:
        payload, log = result
    else:
        payload = result
    if o_type == types.TYPE_STRING:
        result = payload.read().decode('utf-8')
    elif o_type == types.TYPE_DICT:
        result = json.loads(payload.read(), object_hook=utils.JSONDecoder)
    else:
        raise Exception('Unhandled type')
    if logs:
        return result, log
    else:
        return result


def invoke_as_string(name, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
    """
    Invokes a Lambda function and formats the response as a string
    :param name: The name or ARN of the function
    :param payload: A dict or JSON string that you want to provide to the Lambda function
    :param invoke_type: The invocation type
    :param logs: Set to true to include the execution log in the response
    :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
    function in the context object.
    :return: The response as a string, also the logs if requested
    """
    return invoke_as(name, types.TYPE_STRING, payload=payload, invoke_type=invoke_type, logs=logs, context=context)


def invoke_as_dict(name, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
    """
    Invokes a Lambda function and loads the JSON response into a dict
    :param name: The name or ARN of the function
    :param payload: A dict or JSON string that you want to provide to the Lambda function
    :param invoke_type: The invocation type
    :param logs: Set to true to include the execution log in the response
    :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
    function in the context object.
    :return: The response as a dict, also the logs if requested
    """
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


class Lambda(UserDict):

    def __init__(self, config):
        UserDict.__init__(self)
        self.update(config)

    @classmethod
    def get(cls, name):
        response = client.get_function(FunctionName=name)
        result = cls(response['Configuration'])
        if 'Code' in response:
            result['Code'] = response['Code']
        if 'Tags' in response:
            result['Tags'] = response['Tags']
        if 'Concurrency' in response:
            result['Concurrency'] = response['Concurrency']
        return result

    @classmethod
    def from_create(cls, response):
        return cls(response)

    def as_function(self, o_type=types.TYPE_DICT):
        """
        Creates a python function that will invoke the Lambda and return the results in the specified format.
        :param o_type: A value defined in larry.types to specify how the Lambda response will be read
        :return: A function object
        """
        return as_function(self.arn, o_type=o_type)

    def invoke(self, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
        """
        Invokes the function.
        :param payload: A dict or JSON string that you want to provide to the Lambda function
        :param invoke_type: The invocation type
        :param logs: Set to true to include the execution log in the response
        :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
        function in the context object.
        :return: The response payload, also the logs if requested
        """
        return invoke(self.arn, payload=payload, invoke_type=invoke_type, logs=logs, context=context)

    def invoke_as(self, o_type, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
        """
        Invokes the function and formats the response into the requested type.
        :param o_type: A value defined in larry.types to specify how the Lambda response will be read
        :param payload: A dict or JSON string that you want to provide to the Lambda function
        :param invoke_type: The invocation type
        :param logs: Set to true to include the execution log in the response
        :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
        function in the context object.
        :return: The response in the requested type, also the logs if requested
        """
        return invoke_as(self.arn, o_type, payload=payload, invoke_type=invoke_type, logs=logs, context=context)

    def invoke_as_string(self, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
        """
        Invokes the function and formats the response as a string
        :param payload: A dict or JSON string that you want to provide to the Lambda function
        :param invoke_type: The invocation type
        :param logs: Set to true to include the execution log in the response
        :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
        function in the context object.
        :return: The response as a string, also the logs if requested
        """
        return invoke_as_string(self.arn, payload=payload, invoke_type=invoke_type, logs=logs, context=context)

    def invoke_as_dict(self, payload=None, invoke_type=INVOKE_TYPE_REQUEST_RESPONSE, logs=False, context=None):
        """
        Invokes the function and loads the JSON response into a dict
        :param payload: A dict or JSON string that you want to provide to the Lambda function
        :param invoke_type: The invocation type
        :param logs: Set to true to include the execution log in the response
        :param context: Up to 3583 bytes of base64-encoded data about the invoking client to pass to the
        function in the context object.
        :return: The response as a dict, also the logs if requested
        """
        return invoke_as_dict(self.arn, payload=payload, invoke_type=invoke_type, logs=logs, context=context)

    @property
    def name(self):
        return self['FunctionName']

    @property
    def arn(self):
        return self['FunctionArn']

    @property
    def runtime(self):
        return self['Runtime']

    @property
    def role(self):
        return self['Role']

    @property
    def handler(self):
        return self['Handler']

    @property
    def code_size(self):
        return self['CodeSize']

    @property
    def description(self):
        return self['Description']

    @property
    def timeout(self):
        return self['Timeout']

    @property
    def memory_size(self):
        return self['MemorySize']

    @property
    def last_modified(self):
        return self['LastModified']

    @property
    def code_sha256(self):
        return self['CodeSha256']

    @property
    def version(self):
        return self['Version']

    @property
    def dead_letter_arn(self):
        return self['DeadLetterConfig'].get('TargetArn')

    @property
    def environment_variables(self):
        return self['Environment'].get('Variables', {})

    @property
    def environment_error(self):
        return self['Environment'].get('Error', {})

    @property
    def master_arn(self):
        return self['MasterArn']

    @property
    def revision_id(self):
        return self['RevisionId']

    @property
    def layers(self):
        return self['Layers']

    @property
    def state(self):
        return self['State']

    @property
    def state_reason(self):
        return self['StateReason']

    @property
    def state_reason_code(self):
        return self['StateReasonCode']

    @property
    def last_update_status(self):
        return self['LastUpdateStatus']

    @property
    def last_update_status_reason(self):
        return self['LastUpdateStatusReason']

    @property
    def last_update_status_reason_code(self):
        return self['LastUpdateStatusReasonCode']

    @property
    def code_repository_type(self):
        return self['Code'].get('RepositoryType')

    @property
    def code_location(self):
        return self['Code'].get('Location')

    @property
    def tags(self):
        return self['Tags']

    @property
    def reserved_concurrent_executions(self):
        return self['Concurrency'].get('ReservedConcurrentExecutions')
