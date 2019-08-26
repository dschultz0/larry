import boto3


_session = None


def session():
    global _session
    if _session is None:
        _session = boto3.session.Session()
    return _session


def set_session_parameters(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_session_token=None,
                region_name=None,
                profile_name=None):
    global _session
    _session = boto3.session.Session(**_copy_non_null(locals()))


def _copy_non_null(param_list):
    result = {}
    for key, val in param_list.items():
        if val is not None:
            result[key] = val
    return result
