import larry.core
from larry import utils
import boto3


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
    client = __session.client('sqs')


def send_message(message, destination, sqs_client=None):
    """
    Sends a message to the specified queue.
    :param message: The message to send.
    :param destination: The URL of the queue to send the message to
    :param sqs_client: Boto3 client to use if you don't wish to use the default client
    :return: The message id assigned to the message
    """
    sqs_client = sqs_client if sqs_client else client
    if type(message) == dict:
        message = utils.json_dumps(message)
    return sqs_client.send_message(QueueUrl=destination, MessageBody=message)['MessageId']
