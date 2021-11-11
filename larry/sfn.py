import json
import larry.core
from larry import utils
import boto3
from collections.abc import Mapping


client = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()


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
    __session = boto_session if boto_session is not None else boto3.session.Session(**larry.core.copy_non_null_keys(locals()))
    client = __session.client('stepfunctions')


def start_execution(state_machine_arn, input_=None, name=None, trace_header=None, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    params = larry.core.map_parameters(locals(), {
        'state_machine_arn': 'stateMachineArn',
        'input_': 'input',
        'name': 'name',
        'trace_header': 'TraceHeader'
    })
    if 'input' in params and isinstance(params['input'], Mapping):
        params['input'] = json.dumps(params['input'])
    return sfn_client.start_execution(**params).get('executionArn')


def execution_history(execution_arn, reverse=False, include_execution_data=True, sfn_client=None):
    """
    Returns the history of an execution as an iterator of events. Does not support EXPRESS state machines.
    :param execution_arn: The Amazon Resource Name of the execution
    :param reverse: List events in descending order
    :param include_execution_data: Include execution data (input/output)
    :param sfn_client: Boto3 client to use if you don't wish to use the default client
    :return: An iterator of the events
    """
    sfn_client = sfn_client if sfn_client else client
    params = {
        "executionArn": execution_arn,
        "reverseOrder": reverse,
        "includeExecutionData": include_execution_data
    }
    results_to_retrieve = True
    while results_to_retrieve:
        response = sfn_client.get_execution_history(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for event in response['events']:
            yield event


def executions(state_machine_arn, status_filter=None, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    params = larry.core.map_parameters(locals(), {
        'state_machine_arn': 'stateMachineArn',
        'status_filter': 'statusFilter'
    })
    results_to_retrieve = True
    while results_to_retrieve:
        response = sfn_client.list_executions(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for execution in response['executions']:
            yield execution


def state_machines(sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    params = {}
    results_to_retrieve = True
    while results_to_retrieve:
        response = sfn_client.list_state_machines(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for state_machine in response['stateMachines']:
            yield state_machine


def describe_execution(execution_arn, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    response = sfn_client.describe_execution(executionArn=execution_arn)
    return {k: json.loads(v) if k in ['input', 'output'] else v
            for k, v in response.items() if k not in ['ResponseMetadata', 'inputDetails', 'outputDetails']}


def describe_state_machine(state_machine_arn, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    response = sfn_client.describe_execution(stateMachineArn=state_machine_arn)
    return {k: json.loads(v) if k in ['definition'] else v
            for k, v in response.items() if k not in ['ResponseMetadata']}


def stop_execution(execution_arn, error=None, cause=None, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    params = larry.core.map_parameters(locals(), {
        'execution_arn': 'executionArn',
        'error': 'error',
        'cause': 'cause'
    })
    return sfn_client.stop_execution_execution(**params).get('stopDate')


def send_task_success(task_token, output, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    sfn_client.send_task_success(taskToken=task_token, output=output)


def send_task_heartbeat(task_token, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    sfn_client.send_task_heartbeat(taskToken=task_token)


def send_task_failure(task_token, error=None, cause=None, sfn_client=None):
    sfn_client = sfn_client if sfn_client else client
    params = larry.core.map_parameters(locals(), {
        'task_token': 'taskToken',
        'error': 'error',
        'cause': 'cause'
    })
    sfn_client.send_task_failure(**params)
