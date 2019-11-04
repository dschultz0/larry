import larrydata.s3
import larrydata.mturk
import larrydata.sqs
import larrydata.sts
import larrydata.sagemaker
import boto3
import larrydata.utils


def version():
    return "0.2.19"


def _propagate_session(_session):
    larrydata.s3.set_session(session=_session)
    larrydata.mturk.set_session(session=_session)
    larrydata.sqs.set_session(session=_session)
    larrydata.sts.set_session(session=_session)
    larrydata.sagemaker.set_session(session=_session)


# A local instance of the boto3 session to use
_session = boto3.session.Session()
_propagate_session(_session)


def session():
    """
    Retrieves the current boto3 session for this module
    :return: Boto3 session
    """
    global _session
    if _session is None:
        _session = boto3.session.Session()
    return _session


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
    global _session
    _session=session if session is not None else boto3.session.Session(**larrydata.utils.copy_non_null_keys(locals()))
    _propagate_session(_session)

