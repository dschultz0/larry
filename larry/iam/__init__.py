from larry import utils
import boto3
import json
import larry.iam.policies as policies

# Local IAM resource object
resource = None
# A local instance of the boto3 session to use
__session = boto3.session.Session()


def __assume_role_service_policy(service):
    return json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "{}.amazonaws.com".format(service)
                },
                "Action": "sts:AssumeRole"}
        ]
    })


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
    global __session, resource
    __session = boto_session if boto_session is not None else boto3.session.Session(**utils.copy_non_null_keys(locals()))
    resource = __session.resource('iam')


def create_service_role(name, service, policy=None):
    role = resource.create_role(RoleName=name,
                                AssumeRolePolicyDocument=__assume_role_service_policy(service))
    if policy:
        if isinstance(policy, list):
            for p in policy:
                role.attach_policy(PolicyArn=p)
        else:
            role.attach_policy(PolicyArn=policy)
    return role.arn
