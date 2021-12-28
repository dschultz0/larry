import json
from collections import UserDict

import larry.core
import boto3
from collections.abc import Mapping


# A local instance of the boto3 session to use
__session = boto3.session.Session()
client = __session.client('stepfunctions')


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


def start_execution(state_machine_arn, input_=None, name=None, trace_header=None):
    params = larry.core.map_parameters(locals(), {
        'state_machine_arn': 'stateMachineArn',
        'input_': 'input',
        'name': 'name',
        'trace_header': 'TraceHeader'
    })
    if 'input' in params and isinstance(params['input'], Mapping):
        params['input'] = json.dumps(params['input'])
    return client.start_execution(**params).get('executionArn')


def execution_history(execution_arn, reverse=False, include_execution_data=True):
    """
    Returns the history of an execution as an iterator of events. Does not support EXPRESS state machines.
    :param execution_arn: The Amazon Resource Name of the execution
    :param reverse: List events in descending order
    :param include_execution_data: Include execution data (input/output)
    :return: An iterator of the events
    """
    params = {
        "executionArn": execution_arn,
        "reverseOrder": reverse,
        "includeExecutionData": include_execution_data
    }
    results_to_retrieve = True
    previous_events = {}
    while results_to_retrieve:
        response = client.get_execution_history(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for event in response['events']:
            event_obj = Event(event, previous_events)
            previous_events[event_obj.id] = event_obj
            yield event_obj


def trace_execution_failure(execution_arn):
    for step in execution_history(execution_arn):
        if "Failed" in step.event_type:
            traced_input = __find_input(step)
            print(f"Task failed in step {step.id} of execution {execution_arn}")
            print(step)
            if step.input is None and traced_input:
                print("Input:")
                for k, v in traced_input.items():
                    print(f" - {k}: {v}")
            if step.cause and "ExecutionArn" in step.cause:
                trace_execution_failure(step.cause["ExecutionArn"])
            break


def __find_input(step):
    while step:
        if step.input:
            return step.input
        step = step.previous_event


def executions(state_machine_arn, status_filter=None):
    params = larry.core.map_parameters(locals(), {
        'state_machine_arn': 'stateMachineArn',
        'status_filter': 'statusFilter'
    })
    results_to_retrieve = True
    while results_to_retrieve:
        response = client.list_executions(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for execution in response['executions']:
            yield execution


def state_machines():
    params = {}
    results_to_retrieve = True
    while results_to_retrieve:
        response = client.list_state_machines(**params)
        if response.get('nextToken'):
            params['nextToken'] = response.get('nextToken')
        else:
            results_to_retrieve = False
        for state_machine in response['stateMachines']:
            yield state_machine


def describe_execution(execution_arn):
    response = client.describe_execution(executionArn=execution_arn)
    return {k: json.loads(v) if k in ['input', 'output'] else v
            for k, v in response.items() if k not in ['ResponseMetadata', 'inputDetails', 'outputDetails']}


def describe_state_machine(state_machine_arn):
    response = client.describe_execution(stateMachineArn=state_machine_arn)
    return {k: json.loads(v) if k in ['definition'] else v
            for k, v in response.items() if k not in ['ResponseMetadata']}


def stop_execution(execution_arn, error=None, cause=None):
    params = larry.core.map_parameters(locals(), {
        'execution_arn': 'executionArn',
        'error': 'error',
        'cause': 'cause'
    })
    return client.stop_execution_execution(**params).get('stopDate')


def send_task_success(task_token, output):
    client.send_task_success(taskToken=task_token, output=output)


def send_task_heartbeat(task_token):
    client.send_task_heartbeat(taskToken=task_token)


def send_task_failure(task_token, error=None, cause=None):
    params = larry.core.map_parameters(locals(), {
        'task_token': 'taskToken',
        'error': 'error',
        'cause': 'cause'
    })
    client.send_task_failure(**params)


class StateMachine:
    def __init__(self, arn):
        self._arn = arn

    def _name_to_arn(self, name):
        if name.startswith("arn"):
            return name
        else:
            return self._arn.replace("stateMachine", "execution") + ":" + name

    def start_execution(self, input_=None, name=None, trace_header=None):
        execution_arn = start_execution(self._arn, input_, name, trace_header)
        return execution_arn.split(":")[-1]

    def has_finished(self, name):
        arn = self._name_to_arn(name)
        status = describe_execution(arn)["status"]
        return status != "RUNNING"

    def has_succeeded(self, name):
        arn = self._name_to_arn(name)
        status = describe_execution(arn)["status"]
        return status == "SUCCEEDED"

    def trace_execution_failure(self, name):
        arn = self._name_to_arn(name)
        trace_execution_failure(arn)


class Event:
    def __init__(self, event, previous_events=None):
        self._event = event
        t = event["type"]
        self._details = event.get(t[0].lower() + t[1:] + "EventDetails", {})
        if previous_events and event.get("previousEventId") in previous_events:
            self._previous_event = previous_events[event["previousEventId"]]
        else:
            self._previous_event = None

    @property
    def event_type(self):
        return self._event["type"]

    @property
    def id(self):
        return self._event["id"]

    @property
    def previous_event_id(self):
        return self._event["previousEventId"]

    @property
    def previous_event(self):
        return self._previous_event

    @property
    def timestamp(self):
        return self._event["timestamp"]

    @property
    def details(self):
        return self._details

    @property
    def error(self):
        return self._details.get("error")

    @property
    def cause(self):
        c = self._details.get("cause")
        if isinstance(c, str) and "{" in c:
            try:
                start = c.find("{")
                end = c.rfind("}") + 1
                pre = c[:start]
                post = c[end:]
                obj = {}
                if pre:
                    obj["preMessage"] = pre
                obj.update(json.loads(c[start:end]))
                if post:
                    obj["postMessage"] = post
                return obj
            except:
                pass
        return c

    @property
    def input(self):
        if "input" in self._details:
            try:
                return json.loads(self._details.get("input"))
            except:
                return self._details.get("input")
        elif self.cause and "Input" in self.cause:
            try:
                return json.loads(self.cause.get("Input"))
            except:
                return self.cause.get("Input")
        return None

    @property
    def output(self):
        try:
            return json.loads(self._details.get("output"))
        except:
            return self._details.get("output")

    @property
    def resource(self):
        return self._details.get("resource")

    @property
    def resource_type(self):
        return self._details.get("resourceType")

    @property
    def timeout(self):
        return self._details.get("timeoutInSeconds")

    @property
    def heartbeat(self):
        return self._details.get("heartbeatInSeconds")

    @property
    def input_truncated(self):
        return self._details.get("inputDetails", {}).get("truncated", False)

    @property
    def output_truncated(self):
        return self._details.get("outputDetails", {}).get("truncated", False)

    @property
    def region(self):
        return self._details.get("region")

    @property
    def parameters(self):
        return self._details.get("parameters")

    @property
    def name(self):
        return self._details.get("name")

    @property
    def index(self):
        return self._details.get("index")

    @property
    def length(self):
        return self._details.get("length")

    def __repr__(self):
        lines = []
        lines = [f"<Event {self.id}: {self.event_type} at {self.timestamp.strftime('%Y/%m/%d %H:%M:%S')} ({self.previous_event_id})"]
        buried_input = None
        for key, value in self._details.items():
            if key in ["cause", "input", "output"]:
                value = getattr(self, key)
                if isinstance(value, dict):
                    lines.append(f"   - {key}:")
                    for k, v in value.items():
                        if key == "cause" and k == "Input":
                            buried_input = self.input
                        elif key == "cause" and k == "stackTrace":
                            lines.append("      * stackTrace:")
                            for stack_trace_entry in v:
                                lines.extend(["        " + e for e in stack_trace_entry.strip("\n").split("\n")])
                        else:
                            lines.append(f"      * {k}: {v}")
                else:
                    lines.append(f"   - {key}: {value}")
            elif key in ["inputDetails", "outputDetails"]:
                if value.get("truncated"):
                    lines.append(f"   - {key.replace('Details', '')} truncated: True")
            else:
                lines.append(f"   - {key}: {value}")
        if buried_input:
            if isinstance(buried_input, dict):
                lines.append(f"   - input:")
                for k, v in buried_input.items():
                    lines.append(f"      * {k}: {v}")
            else:
                lines.append(f"   - input: {buried_input}")
        lines.append(">")
        return "\n".join(lines)
