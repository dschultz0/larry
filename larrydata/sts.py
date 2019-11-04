import larrydata.utils
import boto3


_client = None
# A local instance of the boto3 session to use
_session = boto3.session.Session()


def set_session(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_session_token=None,
                region_name=None,
                profile_name=None,
                session=None):
    """
    Sets the boto3 session for this module to use a specified configuration state.
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS temporary session token
    :param region_name: Default region when creating new connections
    :param profile_name: The name of a profile to use
    :param session: An existing session to use
    :return: None
    """
    global _session, _client
    _session = session if session is not None else boto3.session.Session(**larrydata.utils.copy_non_null_keys(locals()))
    _client = None


def client():
    global _client, _session
    if _client is None:
        _client = _session.client('sts')
    return _client


def account_id(sts_client=client()):
    return sts_client.get_caller_identity()['Account']
