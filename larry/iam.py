import larry.core
import boto3
import json
from collections.abc import Mapping
from botocore.exceptions import ClientError
from larry import sts


# A local instance of the boto3 session to use
__session = boto3.session.Session()
# Local IAM resource object
__resource = __session.resource('iam')


def __getattr__(name):
    if name == 'session':
        return __session
    elif name == 'client':
        return __resource.meta.client
    elif name == 'resource':
        return __resource
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def set_session(aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_session_token=None,
                region_name=None,
                profile_name=None,
                boto_session=None):
    """
    Sets the boto3 session for this module to use a specified configuration state.
    :param aws_access_key_id: AWS access key ID
    :param aws_secret_access_key: AWS secret access key
    :param aws_session_token: AWS temporary session token
    :param region_name: Default region when creating new connections
    :param profile_name: The name of a profile to use
    :param boto_session: An existing session to use
    :return: None
    """
    global __session, __resource
    __session = boto_session if boto_session is not None else boto3.session.Session(**larry.core.copy_non_null_keys(locals()))
    __resource = __session.resource('iam')


def __assume_role_service_policy(service):
    """
    Generates a policy document to use as the AssumeRole policy for a service role.
    :param service: The service that will be able to use the role
    :return: A policy document
    """
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


def iter_roles(path_prefix=None):
    params = {}
    if path_prefix:
        params['PathPrefix'] = path_prefix
    remaining_results = True
    while remaining_results:
        response = __resource.meta.client.list_roles(**params)
        for rl in response.get('Roles', []):
            yield rl
        remaining_results = response.get('IsTruncated', False)
        params['Marker'] = response.get('Marker')


def policy(name_or_arn):
    """
    Retrieves the policy object associated with the provided name or ARN.
    :param name_or_arn: A name or ARN associated with the policy
    :return: A boto3 Policy object
    """
    if name_or_arn.startswith('arn:aws:iam'):
        return __resource.Policy(name_or_arn)
    else:
        return __resource.Policy('arn:aws:iam::{}:policy/{}'.format(sts.account_id(), name_or_arn))


def get_policy_if_exists(name):
    """
    Attempts to load the requested policy and returns None if it does not exist.
    :param name: The name or ARN associated with the policy
    :return: A boto3 Policy object or None
    """
    try:
        p = policy(name)
        p.load()
        return p
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            return None
        else:
            raise e


def role(name):
    """
    Retrieves the role object associated with the provided name
    :param name: A name associated with the role
    :return: A boto3 Role object
    """
    return __resource.Role(name)


def get_role_if_exists(name):
    """
    Attempts to load the requested role and returns None if it does not exist.
    :param name: A name associated with the role
    :return: A boto3 Role object or None
    """
    try:
        r = role(name)
        r.load()
        return r
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            return None
        else:
            raise e


def create_service_role(name, service, policies=None):
    """
    Creates a service role with assume-role permissions for the specified service.
    :param name: Name of the role
    :param service: Service to allow to use the role
    :param policies: Policy or policies to attach to the role
    :return: ARN for the created role
    """
    r = __resource.create_role(RoleName=name,
                               AssumeRolePolicyDocument=__assume_role_service_policy(service))
    if policies:
        if isinstance(policies, list):
            for p in policies:
                r.attach_policy(PolicyArn=p)
        else:
            r.attach_policy(PolicyArn=policies)
    return r.arn


def create_or_update_service_role(name, service, policies=None):
    """
    Creates or updates a service role with assume-role permissions for the specified service.
    :param name: Name of the role
    :param service: Service to allow to use the role
    :param policies: Policy or policies to attach to the role
    :return: ARN for the role
    """
    existing = get_role_if_exists(name)
    if existing:
        if policies is None:
            policies = []
        if not isinstance(policies, list):
            policies = [policies]
        existing_policies = [p.arn for p in list(existing.attached_policies.all())]
        for p in existing_policies:
            if p not in policies:
                existing.detach_policy(PolicyArn=p)
        for p in policies:
            if p not in existing_policies:
                existing.attach_policy(PolicyArn=p)
        existing_service = existing.assume_role_policy_document['Statement'][0]['Principal']['Service'].split('.')[0]
        if existing_service != service:
            existing.AssumeRolePolicy().update(PolicyDocument=__assume_role_service_policy(service))
        return existing.arn
    else:
        return create_service_role(name, service, policies=policies)


def create_policy(name, document, path=None, description=None):
    """
    Creates an IAM policy based on the provided document.
    :param name: The name of the policy
    :param document: The policy document to use (str or dict)
    :param path: The path for the policy.
    :param description: A friendly description of the policy.
    :return: ARN for the policy
    """
    params = larry.core.map_parameters(locals(), {
        'name': 'PolicyName',
        'document': 'PolicyDocument',
        'path': 'Path',
        'description': 'Description'
    })
    if isinstance(document, Mapping):
        params['PolicyDocument'] = json.dumps(document)
    return __resource.create_policy(**params).arn


def create_or_update_policy(name, document, path=None, description=None):
    """
    Creates or updates an IAM policy based on the provided document. Note that when updating it will create a
    new default version of the policy and may delete prior versions to remain with the limit of 5 active versions
    of a policy.
    :param name: The name of the policy
    :param document: The policy document to use (str or dict)
    :param path: The path for the policy, does not update.
    :param description: A friendly description of the policy, does not update.
    :return: ARN for the policy
    """
    existing = get_policy_if_exists(name)
    if existing:
        # you can have a max of 5 versions, this will remove the oldest if necessary
        versions = list(existing.versions.all())
        if len(versions) >= 5:
            for version in sorted(versions, key=lambda v: int(v.version_id[1:])):
                if not version.is_default_version:
                    version.delete()
                    break

        doc = json.dumps(document) if isinstance(document, Mapping) else document
        existing.create_version(PolicyDocument=doc, SetAsDefault=True)
        return existing.arn
    else:
        return create_policy(name, document, path=path, description=description)


def delete_policy(name):
    """
    Deletes a given policy. Note that a policy cannot be deleted if it's attached to any roles.
    :param name: The name or ARN of the policy
    """
    policy(name).delete()


def delete_role(name):
    """
    Deletes a given role. Note that a role cannot be deleted if it's attached to any policies.
    :param name: The name of the role
    """
    role(name).delete()


def detach_roles_from_policy(name):
    """
    Detaches all roles that have been attached to a given policy.
    :param name: The name or ARN of the policy
    """
    p = policy(name)
    for r in p.attached_roles.all():
        p.detach_role(RoleName=r.name)


def detach_policies_from_role(name):
    """
    Detaches all policies that have been attached to a given role.
    :param name: The name of the role
    """
    r = role(name)
    for p in r.attached_policies.all():
        r.detach_policy(PolicyArn=p.arn)


def aws_policies():
    return __resource.policies.filter(Scope="AWS")


def aws_policy_defaults():
    for p in aws_policies():
        yield p.default_version


class AWSPolicies:
    AWSDirectConnectReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSDirectConnectReadOnlyAccess'
    AmazonGlacierReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonGlacierReadOnlyAccess'
    AWSMarketplaceFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceFullAccess'
    ClientVPNServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ClientVPNServiceRolePolicy'
    AWSSSODirectoryAdministrator = 'arn:aws:iam::aws:policy/AWSSSODirectoryAdministrator'
    AWSIoT1ClickReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSIoT1ClickReadOnlyAccess'
    AutoScalingConsoleReadOnlyAccess = 'arn:aws:iam::aws:policy/AutoScalingConsoleReadOnlyAccess'
    AmazonDMSRedshiftS3Role = 'arn:aws:iam::aws:policy/service-role/AmazonDMSRedshiftS3Role'
    AWSQuickSightListIAM = 'arn:aws:iam::aws:policy/service-role/AWSQuickSightListIAM'
    AWSHealthFullAccess = 'arn:aws:iam::aws:policy/AWSHealthFullAccess'
    AlexaForBusinessGatewayExecution = 'arn:aws:iam::aws:policy/AlexaForBusinessGatewayExecution'
    AmazonElasticTranscoder_ReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonElasticTranscoder_ReadOnlyAccess'
    AmazonRDSFullAccess = 'arn:aws:iam::aws:policy/AmazonRDSFullAccess'
    SupportUser = 'arn:aws:iam::aws:policy/job-function/SupportUser'
    AmazonEC2FullAccess = 'arn:aws:iam::aws:policy/AmazonEC2FullAccess'
    SecretsManagerReadWrite = 'arn:aws:iam::aws:policy/SecretsManagerReadWrite'
    AWSIoTThingsRegistration = 'arn:aws:iam::aws:policy/service-role/AWSIoTThingsRegistration'
    AmazonDocDBReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonDocDBReadOnlyAccess'
    AWSElasticBeanstalkReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkReadOnlyAccess'
    AmazonMQApiFullAccess = 'arn:aws:iam::aws:policy/AmazonMQApiFullAccess'
    AWSElementalMediaStoreReadOnly = 'arn:aws:iam::aws:policy/AWSElementalMediaStoreReadOnly'
    AWSCertificateManagerReadOnly = 'arn:aws:iam::aws:policy/AWSCertificateManagerReadOnly'
    AWSQuicksightAthenaAccess = 'arn:aws:iam::aws:policy/service-role/AWSQuicksightAthenaAccess'
    AWSCloudMapRegisterInstanceAccess = 'arn:aws:iam::aws:policy/AWSCloudMapRegisterInstanceAccess'
    AWSMarketplaceImageBuildFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceImageBuildFullAccess'
    AWSCodeCommitPowerUser = 'arn:aws:iam::aws:policy/AWSCodeCommitPowerUser'
    AWSCodeCommitFullAccess = 'arn:aws:iam::aws:policy/AWSCodeCommitFullAccess'
    IAMSelfManageServiceSpecificCredentials = 'arn:aws:iam::aws:policy/IAMSelfManageServiceSpecificCredentials'
    AmazonEMRCleanupPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonEMRCleanupPolicy'
    AWSCloud9EnvironmentMember = 'arn:aws:iam::aws:policy/AWSCloud9EnvironmentMember'
    AWSApplicationAutoscalingSageMakerEndpointPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingSageMakerEndpointPolicy'
    FMSServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/FMSServiceRolePolicy'
    AmazonSQSFullAccess = 'arn:aws:iam::aws:policy/AmazonSQSFullAccess'
    AlexaForBusinessReadOnlyAccess = 'arn:aws:iam::aws:policy/AlexaForBusinessReadOnlyAccess'
    AWSLambdaFullAccess = 'arn:aws:iam::aws:policy/AWSLambdaFullAccess'
    AWSIoTLogging = 'arn:aws:iam::aws:policy/service-role/AWSIoTLogging'
    AmazonEC2RoleforSSM = 'arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforSSM'
    AlexaForBusinessNetworkProfileServicePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AlexaForBusinessNetworkProfileServicePolicy'
    AWSCloudHSMRole = 'arn:aws:iam::aws:policy/service-role/AWSCloudHSMRole'
    AWSEnhancedClassicNetworkingMangementPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSEnhancedClassicNetworkingMangementPolicy'
    IAMFullAccess = 'arn:aws:iam::aws:policy/IAMFullAccess'
    AmazonInspectorFullAccess = 'arn:aws:iam::aws:policy/AmazonInspectorFullAccess'
    AmazonElastiCacheFullAccess = 'arn:aws:iam::aws:policy/AmazonElastiCacheFullAccess'
    AWSAgentlessDiscoveryService = 'arn:aws:iam::aws:policy/AWSAgentlessDiscoveryService'
    AWSXrayWriteOnlyAccess = 'arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess'
    AWSPriceListServiceFullAccess = 'arn:aws:iam::aws:policy/AWSPriceListServiceFullAccess'
    AWSKeyManagementServiceCustomKeyStoresServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSKeyManagementServiceCustomKeyStoresServiceRolePolicy'
    AutoScalingReadOnlyAccess = 'arn:aws:iam::aws:policy/AutoScalingReadOnlyAccess'
    AmazonForecastFullAccess = 'arn:aws:iam::aws:policy/AmazonForecastFullAccess'
    AmazonWorkLinkReadOnly = 'arn:aws:iam::aws:policy/AmazonWorkLinkReadOnly'
    TranslateFullAccess = 'arn:aws:iam::aws:policy/TranslateFullAccess'
    AutoScalingFullAccess = 'arn:aws:iam::aws:policy/AutoScalingFullAccess'
    AmazonEC2RoleforAWSCodeDeploy = 'arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforAWSCodeDeploy'
    AWSFMMemberReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSFMMemberReadOnlyAccess'
    AmazonElasticMapReduceEditorsRole = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceEditorsRole'
    AmazonEKSClusterPolicy = 'arn:aws:iam::aws:policy/AmazonEKSClusterPolicy'
    AmazonEKSWorkerNodePolicy = 'arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy'
    AWSMobileHub_ReadOnly = 'arn:aws:iam::aws:policy/AWSMobileHub_ReadOnly'
    CloudWatchEventsBuiltInTargetExecutionAccess = 'arn:aws:iam::aws:policy/service-role/CloudWatchEventsBuiltInTargetExecutionAccess'
    AutoScalingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AutoScalingServiceRolePolicy'
    AmazonElasticTranscoder_FullAccess = 'arn:aws:iam::aws:policy/AmazonElasticTranscoder_FullAccess'
    AmazonCloudDirectoryReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonCloudDirectoryReadOnlyAccess'
    CloudWatchAgentAdminPolicy = 'arn:aws:iam::aws:policy/CloudWatchAgentAdminPolicy'
    AWSOpsWorksFullAccess = 'arn:aws:iam::aws:policy/AWSOpsWorksFullAccess'
    AWSOpsWorksCMInstanceProfileRole = 'arn:aws:iam::aws:policy/AWSOpsWorksCMInstanceProfileRole'
    AWSBatchServiceEventTargetRole = 'arn:aws:iam::aws:policy/service-role/AWSBatchServiceEventTargetRole'
    AWSCodePipelineApproverAccess = 'arn:aws:iam::aws:policy/AWSCodePipelineApproverAccess'
    AWSApplicationDiscoveryAgentAccess = 'arn:aws:iam::aws:policy/AWSApplicationDiscoveryAgentAccess'
    ViewOnlyAccess = 'arn:aws:iam::aws:policy/job-function/ViewOnlyAccess'
    AmazonElasticMapReduceRole = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
    ElasticLoadBalancingFullAccess = 'arn:aws:iam::aws:policy/ElasticLoadBalancingFullAccess'
    AmazonRoute53DomainsReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRoute53DomainsReadOnlyAccess'
    AmazonSSMAutomationApproverAccess = 'arn:aws:iam::aws:policy/AmazonSSMAutomationApproverAccess'
    AWSOpsWorksRole = 'arn:aws:iam::aws:policy/service-role/AWSOpsWorksRole'
    AWSSecurityHubReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSSecurityHubReadOnlyAccess'
    AWSConfigRoleForOrganizations = 'arn:aws:iam::aws:policy/service-role/AWSConfigRoleForOrganizations'
    ApplicationAutoScalingForAmazonAppStreamAccess = 'arn:aws:iam::aws:policy/service-role/ApplicationAutoScalingForAmazonAppStreamAccess'
    AmazonEC2ContainerRegistryFullAccess = 'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryFullAccess'
    AmazonFSxFullAccess = 'arn:aws:iam::aws:policy/AmazonFSxFullAccess'
    SimpleWorkflowFullAccess = 'arn:aws:iam::aws:policy/SimpleWorkflowFullAccess'
    GreengrassOTAUpdateArtifactAccess = 'arn:aws:iam::aws:policy/service-role/GreengrassOTAUpdateArtifactAccess'
    AmazonS3FullAccess = 'arn:aws:iam::aws:policy/AmazonS3FullAccess'
    AWSStorageGatewayReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSStorageGatewayReadOnlyAccess'
    Billing = 'arn:aws:iam::aws:policy/job-function/Billing'
    QuickSightAccessForS3StorageManagementAnalyticsReadOnly = 'arn:aws:iam::aws:policy/service-role/QuickSightAccessForS3StorageManagementAnalyticsReadOnly'
    AmazonEC2ContainerRegistryReadOnly = 'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly'
    AWSRoboMakerFullAccess = 'arn:aws:iam::aws:policy/AWSRoboMakerFullAccess'
    AmazonElasticMapReduceforEC2Role = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role'
    DatabaseAdministrator = 'arn:aws:iam::aws:policy/job-function/DatabaseAdministrator'
    AmazonRedshiftReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRedshiftReadOnlyAccess'
    AmazonEC2ReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonEC2ReadOnlyAccess'
    CloudWatchAgentServerPolicy = 'arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy'
    AWSXrayReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSXrayReadOnlyAccess'
    AWSElasticBeanstalkEnhancedHealth = 'arn:aws:iam::aws:policy/service-role/AWSElasticBeanstalkEnhancedHealth'
    WellArchitectedConsoleFullAccess = 'arn:aws:iam::aws:policy/WellArchitectedConsoleFullAccess'
    AmazonElasticMapReduceReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonElasticMapReduceReadOnlyAccess'
    AWSDirectoryServiceReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSDirectoryServiceReadOnlyAccess'
    AWSSSOMasterAccountAdministrator = 'arn:aws:iam::aws:policy/AWSSSOMasterAccountAdministrator'
    AmazonGuardDutyServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonGuardDutyServiceRolePolicy'
    AmazonVPCReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonVPCReadOnlyAccess'
    AWSElasticBeanstalkServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSElasticBeanstalkServiceRolePolicy'
    ServerMigrationServiceLaunchRole = 'arn:aws:iam::aws:policy/service-role/ServerMigrationServiceLaunchRole'
    AWSCodeDeployRoleForECS = 'arn:aws:iam::aws:policy/AWSCodeDeployRoleForECS'
    CloudWatchEventsReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudWatchEventsReadOnlyAccess'
    AWSLambdaReplicator = 'arn:aws:iam::aws:policy/aws-service-role/AWSLambdaReplicator'
    AmazonAPIGatewayInvokeFullAccess = 'arn:aws:iam::aws:policy/AmazonAPIGatewayInvokeFullAccess'
    AWSSSOServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSSSOServiceRolePolicy'
    AWSLicenseManagerMasterAccountRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSLicenseManagerMasterAccountRolePolicy'
    AmazonKinesisAnalyticsReadOnly = 'arn:aws:iam::aws:policy/AmazonKinesisAnalyticsReadOnly'
    AmazonMobileAnalyticsFullAccess = 'arn:aws:iam::aws:policy/AmazonMobileAnalyticsFullAccess'
    AWSMobileHub_FullAccess = 'arn:aws:iam::aws:policy/AWSMobileHub_FullAccess'
    AmazonAPIGatewayPushToCloudWatchLogs = 'arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs'
    AWSDataPipelineRole = 'arn:aws:iam::aws:policy/service-role/AWSDataPipelineRole'
    CloudWatchFullAccess = 'arn:aws:iam::aws:policy/CloudWatchFullAccess'
    AmazonMQApiReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMQApiReadOnlyAccess'
    AWSDeepLensLambdaFunctionAccessPolicy = 'arn:aws:iam::aws:policy/AWSDeepLensLambdaFunctionAccessPolicy'
    AmazonGuardDutyFullAccess = 'arn:aws:iam::aws:policy/AmazonGuardDutyFullAccess'
    AmazonRDSDirectoryServiceAccess = 'arn:aws:iam::aws:policy/service-role/AmazonRDSDirectoryServiceAccess'
    AWSCodePipelineReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCodePipelineReadOnlyAccess'
    ReadOnlyAccess = 'arn:aws:iam::aws:policy/ReadOnlyAccess'
    AWSAppSyncInvokeFullAccess = 'arn:aws:iam::aws:policy/AWSAppSyncInvokeFullAccess'
    AmazonMachineLearningBatchPredictionsAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningBatchPredictionsAccess'
    AWSIoTSiteWiseFullAccess = 'arn:aws:iam::aws:policy/AWSIoTSiteWiseFullAccess'
    AlexaForBusinessFullAccess = 'arn:aws:iam::aws:policy/AlexaForBusinessFullAccess'
    AWSEC2SpotFleetServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSEC2SpotFleetServiceRolePolicy'
    AmazonRekognitionReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRekognitionReadOnlyAccess'
    AWSCodeDeployReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCodeDeployReadOnlyAccess'
    CloudSearchFullAccess = 'arn:aws:iam::aws:policy/CloudSearchFullAccess'
    AWSLicenseManagerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSLicenseManagerServiceRolePolicy'
    AWSCloudHSMFullAccess = 'arn:aws:iam::aws:policy/AWSCloudHSMFullAccess'
    AmazonEC2SpotFleetAutoscaleRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetAutoscaleRole'
    AWSElasticLoadBalancingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSElasticLoadBalancingServiceRolePolicy'
    AWSCodeBuildDeveloperAccess = 'arn:aws:iam::aws:policy/AWSCodeBuildDeveloperAccess'
    ElastiCacheServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ElastiCacheServiceRolePolicy'
    AWSGlueServiceNotebookRole = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceNotebookRole'
    AWSDataPipeline_PowerUser = 'arn:aws:iam::aws:policy/AWSDataPipeline_PowerUser'
    AWSCodeStarServiceRole = 'arn:aws:iam::aws:policy/service-role/AWSCodeStarServiceRole'
    AmazonTranscribeFullAccess = 'arn:aws:iam::aws:policy/AmazonTranscribeFullAccess'
    AWSDirectoryServiceFullAccess = 'arn:aws:iam::aws:policy/AWSDirectoryServiceFullAccess'
    AmazonFreeRTOSOTAUpdate = 'arn:aws:iam::aws:policy/service-role/AmazonFreeRTOSOTAUpdate'
    AmazonWorkLinkServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonWorkLinkServiceRolePolicy'
    AmazonDynamoDBFullAccess = 'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess'
    AmazonSESReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonSESReadOnlyAccess'
    AmazonRedshiftQueryEditor = 'arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor'
    AWSWAFReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSWAFReadOnlyAccess'
    AutoScalingNotificationAccessRole = 'arn:aws:iam::aws:policy/service-role/AutoScalingNotificationAccessRole'
    AmazonMechanicalTurkReadOnly = 'arn:aws:iam::aws:policy/AmazonMechanicalTurkReadOnly'
    AmazonKinesisReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonKinesisReadOnlyAccess'
    AWSXRayDaemonWriteAccess = 'arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess'
    AWSCloudMapReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCloudMapReadOnlyAccess'
    AWSCloudFrontLogger = 'arn:aws:iam::aws:policy/aws-service-role/AWSCloudFrontLogger'
    AWSCodeDeployFullAccess = 'arn:aws:iam::aws:policy/AWSCodeDeployFullAccess'
    AWSBackupServiceRolePolicyForBackup = 'arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup'
    AWSRoboMakerServiceRolePolicy = 'arn:aws:iam::aws:policy/AWSRoboMakerServiceRolePolicy'
    CloudWatchActionsEC2Access = 'arn:aws:iam::aws:policy/CloudWatchActionsEC2Access'
    AWSLambdaDynamoDBExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaDynamoDBExecutionRole'
    AmazonRoute53DomainsFullAccess = 'arn:aws:iam::aws:policy/AmazonRoute53DomainsFullAccess'
    AmazonElastiCacheReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonElastiCacheReadOnlyAccess'
    AmazonRDSServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonRDSServiceRolePolicy'
    AmazonAthenaFullAccess = 'arn:aws:iam::aws:policy/AmazonAthenaFullAccess'
    AmazonElasticFileSystemReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonElasticFileSystemReadOnlyAccess'
    AWSCloudMapDiscoverInstanceAccess = 'arn:aws:iam::aws:policy/AWSCloudMapDiscoverInstanceAccess'
    CloudFrontFullAccess = 'arn:aws:iam::aws:policy/CloudFrontFullAccess'
    AmazonConnectFullAccess = 'arn:aws:iam::aws:policy/AmazonConnectFullAccess'
    AWSCloud9Administrator = 'arn:aws:iam::aws:policy/AWSCloud9Administrator'
    AWSApplicationAutoscalingEMRInstanceGroupPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingEMRInstanceGroupPolicy'
    AmazonTextractFullAccess = 'arn:aws:iam::aws:policy/AmazonTextractFullAccess'
    AWSOrganizationsServiceTrustPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSOrganizationsServiceTrustPolicy'
    AmazonDocDBFullAccess = 'arn:aws:iam::aws:policy/AmazonDocDBFullAccess'
    AmazonMobileAnalyticsNonfinancialReportAccess = 'arn:aws:iam::aws:policy/AmazonMobileAnalyticsNon-financialReportAccess'
    AWSCloudTrailFullAccess = 'arn:aws:iam::aws:policy/AWSCloudTrailFullAccess'
    AmazonCognitoDeveloperAuthenticatedIdentities = 'arn:aws:iam::aws:policy/AmazonCognitoDeveloperAuthenticatedIdentities'
    AWSConfigRole = 'arn:aws:iam::aws:policy/service-role/AWSConfigRole'
    AWSSSOMemberAccountAdministrator = 'arn:aws:iam::aws:policy/AWSSSOMemberAccountAdministrator'
    AWSApplicationAutoscalingAppStreamFleetPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingAppStreamFleetPolicy'
    AWSCertificateManagerPrivateCAFullAccess = 'arn:aws:iam::aws:policy/AWSCertificateManagerPrivateCAFullAccess'
    AWSGlueServiceRole = 'arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
    AmazonAppStreamServiceAccess = 'arn:aws:iam::aws:policy/service-role/AmazonAppStreamServiceAccess'
    AmazonRedshiftFullAccess = 'arn:aws:iam::aws:policy/AmazonRedshiftFullAccess'
    AWSTransferLoggingAccess = 'arn:aws:iam::aws:policy/service-role/AWSTransferLoggingAccess'
    AmazonZocaloReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonZocaloReadOnlyAccess'
    AWSCloudHSMReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCloudHSMReadOnlyAccess'
    ComprehendFullAccess = 'arn:aws:iam::aws:policy/ComprehendFullAccess'
    AmazonFSxConsoleFullAccess = 'arn:aws:iam::aws:policy/AmazonFSxConsoleFullAccess'
    SystemAdministrator = 'arn:aws:iam::aws:policy/job-function/SystemAdministrator'
    AmazonEC2ContainerServiceEventsRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceEventsRole'
    AmazonRoute53ReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRoute53ReadOnlyAccess'
    AWSMigrationHubDiscoveryAccess = 'arn:aws:iam::aws:policy/service-role/AWSMigrationHubDiscoveryAccess'
    AmazonEC2ContainerServiceAutoscaleRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceAutoscaleRole'
    AWSAppSyncSchemaAuthor = 'arn:aws:iam::aws:policy/AWSAppSyncSchemaAuthor'
    AlexaForBusinessDeviceSetup = 'arn:aws:iam::aws:policy/AlexaForBusinessDeviceSetup'
    AWSBatchServiceRole = 'arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole'
    AWSElasticBeanstalkWebTier = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkWebTier'
    AmazonSQSReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonSQSReadOnlyAccess'
    AmazonChimeFullAccess = 'arn:aws:iam::aws:policy/AmazonChimeFullAccess'
    AWSDeepRacerRoboMakerAccessPolicy = 'arn:aws:iam::aws:policy/AWSDeepRacerRoboMakerAccessPolicy'
    AWSElasticLoadBalancingClassicServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSElasticLoadBalancingClassicServiceRolePolicy'
    AWSMigrationHubDMSAccess = 'arn:aws:iam::aws:policy/service-role/AWSMigrationHubDMSAccess'
    WellArchitectedConsoleReadOnlyAccess = 'arn:aws:iam::aws:policy/WellArchitectedConsoleReadOnlyAccess'
    AmazonKinesisFullAccess = 'arn:aws:iam::aws:policy/AmazonKinesisFullAccess'
    AmazonGuardDutyReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonGuardDutyReadOnlyAccess'
    AmazonFSxServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonFSxServiceRolePolicy'
    AmazonECSServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonECSServiceRolePolicy'
    AmazonConnectReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonConnectReadOnlyAccess'
    AmazonMachineLearningReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningReadOnlyAccess'
    AmazonRekognitionFullAccess = 'arn:aws:iam::aws:policy/AmazonRekognitionFullAccess'
    RDSCloudHsmAuthorizationRole = 'arn:aws:iam::aws:policy/service-role/RDSCloudHsmAuthorizationRole'
    AmazonMachineLearningFullAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningFullAccess'
    AdministratorAccess = 'arn:aws:iam::aws:policy/AdministratorAccess'
    AmazonMachineLearningRealTimePredictionOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningRealTimePredictionOnlyAccess'
    AWSAppSyncPushToCloudWatchLogs = 'arn:aws:iam::aws:policy/service-role/AWSAppSyncPushToCloudWatchLogs'
    AWSMigrationHubSMSAccess = 'arn:aws:iam::aws:policy/service-role/AWSMigrationHubSMSAccess'
    AWSB9InternalServicePolicy = 'arn:aws:iam::aws:policy/AWSB9InternalServicePolicy'
    AWSConfigUserAccess = 'arn:aws:iam::aws:policy/AWSConfigUserAccess'
    AWSIoTConfigAccess = 'arn:aws:iam::aws:policy/AWSIoTConfigAccess'
    SecurityAudit = 'arn:aws:iam::aws:policy/SecurityAudit'
    AWSDiscoveryContinuousExportFirehosePolicy = 'arn:aws:iam::aws:policy/AWSDiscoveryContinuousExportFirehosePolicy'
    AmazonCognitoIdpEmailServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonCognitoIdpEmailServiceRolePolicy'
    AWSElementalMediaConvertFullAccess = 'arn:aws:iam::aws:policy/AWSElementalMediaConvertFullAccess'
    AWSRoboMakerReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSRoboMakerReadOnlyAccess'
    AWSResourceGroupsReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSResourceGroupsReadOnlyAccess'
    AWSCodeStarFullAccess = 'arn:aws:iam::aws:policy/AWSCodeStarFullAccess'
    AmazonSSMServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonSSMServiceRolePolicy'
    AWSDataPipeline_FullAccess = 'arn:aws:iam::aws:policy/AWSDataPipeline_FullAccess'
    NeptuneFullAccess = 'arn:aws:iam::aws:policy/NeptuneFullAccess'
    AmazonSSMManagedInstanceCore = 'arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore'
    AWSAutoScalingPlansEC2AutoScalingPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSAutoScalingPlansEC2AutoScalingPolicy'
    AmazonDynamoDBReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess'
    AutoScalingConsoleFullAccess = 'arn:aws:iam::aws:policy/AutoScalingConsoleFullAccess'
    AWSElementalMediaPackageFullAccess = 'arn:aws:iam::aws:policy/AWSElementalMediaPackageFullAccess'
    AmazonKinesisVideoStreamsFullAccess = 'arn:aws:iam::aws:policy/AmazonKinesisVideoStreamsFullAccess'
    AmazonSNSReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonSNSReadOnlyAccess'
    AmazonRDSPreviewServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonRDSPreviewServiceRolePolicy'
    AWSEC2SpotServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSEC2SpotServiceRolePolicy'
    AmazonElasticMapReduceFullAccess = 'arn:aws:iam::aws:policy/AmazonElasticMapReduceFullAccess'
    AWSCloudMapFullAccess = 'arn:aws:iam::aws:policy/AWSCloudMapFullAccess'
    AWSDataLifecycleManagerServiceRole = 'arn:aws:iam::aws:policy/service-role/AWSDataLifecycleManagerServiceRole'
    AmazonS3ReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    AWSElasticBeanstalkFullAccess = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkFullAccess'
    AmazonWorkSpacesAdmin = 'arn:aws:iam::aws:policy/AmazonWorkSpacesAdmin'
    AWSCodeDeployRole = 'arn:aws:iam::aws:policy/service-role/AWSCodeDeployRole'
    AmazonSESFullAccess = 'arn:aws:iam::aws:policy/AmazonSESFullAccess'
    CloudWatchLogsReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudWatchLogsReadOnlyAccess'
    AmazonRDSBetaServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonRDSBetaServiceRolePolicy'
    AmazonKinesisFirehoseReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonKinesisFirehoseReadOnlyAccess'
    GlobalAcceleratorFullAccess = 'arn:aws:iam::aws:policy/GlobalAcceleratorFullAccess'
    AmazonDynamoDBFullAccesswithDataPipeline = 'arn:aws:iam::aws:policy/AmazonDynamoDBFullAccesswithDataPipeline'
    AWSIoTAnalyticsReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSIoTAnalyticsReadOnlyAccess'
    AmazonEC2RoleforDataPipelineRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforDataPipelineRole'
    CloudWatchLogsFullAccess = 'arn:aws:iam::aws:policy/CloudWatchLogsFullAccess'
    AWSSecurityHubFullAccess = 'arn:aws:iam::aws:policy/AWSSecurityHubFullAccess'
    AWSElementalMediaPackageReadOnly = 'arn:aws:iam::aws:policy/AWSElementalMediaPackageReadOnly'
    AWSElasticBeanstalkMulticontainerDocker = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkMulticontainerDocker'
    AmazonPersonalizeFullAccess = 'arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess'
    AWSMigrationHubFullAccess = 'arn:aws:iam::aws:policy/AWSMigrationHubFullAccess'
    AmazonFSxReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonFSxReadOnlyAccess'
    IAMUserChangePassword = 'arn:aws:iam::aws:policy/IAMUserChangePassword'
    LightsailExportAccess = 'arn:aws:iam::aws:policy/aws-service-role/LightsailExportAccess'
    AmazonAPIGatewayAdministrator = 'arn:aws:iam::aws:policy/AmazonAPIGatewayAdministrator'
    AmazonVPCCrossAccountNetworkInterfaceOperations = 'arn:aws:iam::aws:policy/AmazonVPCCrossAccountNetworkInterfaceOperations'
    AmazonMacieSetupRole = 'arn:aws:iam::aws:policy/service-role/AmazonMacieSetupRole'
    AmazonPollyReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonPollyReadOnlyAccess'
    AmazonRDSDataFullAccess = 'arn:aws:iam::aws:policy/AmazonRDSDataFullAccess'
    AmazonMobileAnalyticsWriteOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMobileAnalyticsWriteOnlyAccess'
    AmazonEC2SpotFleetTaggingRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole'
    DataScientist = 'arn:aws:iam::aws:policy/job-function/DataScientist'
    AWSMarketplaceMeteringFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceMeteringFullAccess'
    AWSOpsWorksCMServiceRole = 'arn:aws:iam::aws:policy/service-role/AWSOpsWorksCMServiceRole'
    FSxDeleteServiceLinkedRoleAccess = 'arn:aws:iam::aws:policy/aws-service-role/FSxDeleteServiceLinkedRoleAccess'
    WorkLinkServiceRolePolicy = 'arn:aws:iam::aws:policy/WorkLinkServiceRolePolicy'
    AmazonConnectServiceLinkedRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonConnectServiceLinkedRolePolicy'
    AWSPrivateMarketplaceAdminFullAccess = 'arn:aws:iam::aws:policy/AWSPrivateMarketplaceAdminFullAccess'
    AWSConnector = 'arn:aws:iam::aws:policy/AWSConnector'
    AWSCodeDeployRoleForECSLimited = 'arn:aws:iam::aws:policy/AWSCodeDeployRoleForECSLimited'
    AmazonElasticTranscoder_JobsSubmitter = 'arn:aws:iam::aws:policy/AmazonElasticTranscoder_JobsSubmitter'
    AmazonMacieHandshakeRole = 'arn:aws:iam::aws:policy/service-role/AmazonMacieHandshakeRole'
    AWSIoTAnalyticsFullAccess = 'arn:aws:iam::aws:policy/AWSIoTAnalyticsFullAccess'
    AWSBatchFullAccess = 'arn:aws:iam::aws:policy/AWSBatchFullAccess'
    AmazonSSMDirectoryServiceAccess = 'arn:aws:iam::aws:policy/AmazonSSMDirectoryServiceAccess'
    AmazonECS_FullAccess = 'arn:aws:iam::aws:policy/AmazonECS_FullAccess'
    AWSSupportServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSSupportServiceRolePolicy'
    AWSApplicationAutoscalingRDSClusterPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingRDSClusterPolicy'
    AWSServiceRoleForEC2ScheduledInstances = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForEC2ScheduledInstances'
    AWSCodeDeployRoleForLambda = 'arn:aws:iam::aws:policy/service-role/AWSCodeDeployRoleForLambda'
    AWSFMAdminReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSFMAdminReadOnlyAccess'
    AmazonSSMFullAccess = 'arn:aws:iam::aws:policy/AmazonSSMFullAccess'
    AWSCodeCommitReadOnly = 'arn:aws:iam::aws:policy/AWSCodeCommitReadOnly'
    AmazonEC2ContainerServiceFullAccess = 'arn:aws:iam::aws:policy/AmazonEC2ContainerServiceFullAccess'
    AmazonFreeRTOSFullAccess = 'arn:aws:iam::aws:policy/AmazonFreeRTOSFullAccess'
    AmazonTextractServiceRole = 'arn:aws:iam::aws:policy/service-role/AmazonTextractServiceRole'
    AmazonCognitoReadOnly = 'arn:aws:iam::aws:policy/AmazonCognitoReadOnly'
    AmazonDMSCloudWatchLogsRole = 'arn:aws:iam::aws:policy/service-role/AmazonDMSCloudWatchLogsRole'
    AWSApplicationDiscoveryServiceFullAccess = 'arn:aws:iam::aws:policy/AWSApplicationDiscoveryServiceFullAccess'
    AmazonRoute53AutoNamingReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRoute53AutoNamingReadOnlyAccess'
    AWSSSOReadOnly = 'arn:aws:iam::aws:policy/AWSSSOReadOnly'
    AmazonVPCFullAccess = 'arn:aws:iam::aws:policy/AmazonVPCFullAccess'
    AWSCertificateManagerPrivateCAUser = 'arn:aws:iam::aws:policy/AWSCertificateManagerPrivateCAUser'
    AWSAppSyncAdministrator = 'arn:aws:iam::aws:policy/AWSAppSyncAdministrator'
    AWSEC2FleetServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSEC2FleetServiceRolePolicy'
    AmazonRoute53AutoNamingFullAccess = 'arn:aws:iam::aws:policy/AmazonRoute53AutoNamingFullAccess'
    AWSImportExportFullAccess = 'arn:aws:iam::aws:policy/AWSImportExportFullAccess'
    DynamoDBReplicationServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/DynamoDBReplicationServiceRolePolicy'
    AmazonMechanicalTurkFullAccess = 'arn:aws:iam::aws:policy/AmazonMechanicalTurkFullAccess'
    AmazonEC2ContainerRegistryPowerUser = 'arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryPowerUser'
    AWSSSODirectoryReadOnly = 'arn:aws:iam::aws:policy/AWSSSODirectoryReadOnly'
    AmazonMachineLearningCreateOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningCreateOnlyAccess'
    AmazonKinesisVideoStreamsReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonKinesisVideoStreamsReadOnlyAccess'
    AWSCloudTrailReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCloudTrailReadOnlyAccess'
    WAFRegionalLoggingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/WAFRegionalLoggingServiceRolePolicy'
    AWSLambdaExecute = 'arn:aws:iam::aws:policy/AWSLambdaExecute'
    AWSGlueConsoleSageMakerNotebookFullAccess = 'arn:aws:iam::aws:policy/AWSGlueConsoleSageMakerNotebookFullAccess'
    AmazonMSKFullAccess = 'arn:aws:iam::aws:policy/AmazonMSKFullAccess'
    AWSIoTRuleActions = 'arn:aws:iam::aws:policy/service-role/AWSIoTRuleActions'
    AmazonEKSServicePolicy = 'arn:aws:iam::aws:policy/AmazonEKSServicePolicy'
    AWSQuickSightDescribeRedshift = 'arn:aws:iam::aws:policy/service-role/AWSQuickSightDescribeRedshift'
    AmazonElasticsearchServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonElasticsearchServiceRolePolicy'
    AmazonMQReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMQReadOnlyAccess'
    VMImportExportRoleForAWSConnector = 'arn:aws:iam::aws:policy/service-role/VMImportExportRoleForAWSConnector'
    AWSCodePipelineCustomActionAccess = 'arn:aws:iam::aws:policy/AWSCodePipelineCustomActionAccess'
    AWSLambdaSQSQueueExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole'
    AWSCloud9ServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSCloud9ServiceRolePolicy'
    AWSApplicationAutoscalingECSServicePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingECSServicePolicy'
    AWSOpsWorksInstanceRegistration = 'arn:aws:iam::aws:policy/AWSOpsWorksInstanceRegistration'
    AmazonCloudDirectoryFullAccess = 'arn:aws:iam::aws:policy/AmazonCloudDirectoryFullAccess'
    AmazonECSTaskExecutionRolePolicy = 'arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy'
    AWSStorageGatewayFullAccess = 'arn:aws:iam::aws:policy/AWSStorageGatewayFullAccess'
    AWSIoTEventsFullAccess = 'arn:aws:iam::aws:policy/AWSIoTEventsFullAccess'
    AmazonLexReadOnly = 'arn:aws:iam::aws:policy/AmazonLexReadOnly'
    TagPoliciesServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/TagPoliciesServiceRolePolicy'
    AmazonChimeUserManagement = 'arn:aws:iam::aws:policy/AmazonChimeUserManagement'
    AmazonMSKReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMSKReadOnlyAccess'
    AWSDataSyncFullAccess = 'arn:aws:iam::aws:policy/AWSDataSyncFullAccess'
    AWSServiceRoleForIoTSiteWise = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForIoTSiteWise'
    CloudwatchApplicationInsightsServiceLinkedRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudwatchApplicationInsightsServiceLinkedRolePolicy'
    AWSTrustedAdvisorServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSTrustedAdvisorServiceRolePolicy'
    AWSIoTConfigReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSIoTConfigReadOnlyAccess'
    AmazonWorkMailReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonWorkMailReadOnlyAccess'
    AmazonDMSVPCManagementRole = 'arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole'
    AWSLambdaKinesisExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole'
    ComprehendDataAccessRolePolicy = 'arn:aws:iam::aws:policy/service-role/ComprehendDataAccessRolePolicy'
    AmazonDocDBConsoleFullAccess = 'arn:aws:iam::aws:policy/AmazonDocDBConsoleFullAccess'
    ResourceGroupsandTagEditorReadOnlyAccess = 'arn:aws:iam::aws:policy/ResourceGroupsandTagEditorReadOnlyAccess'
    AmazonRekognitionServiceRole = 'arn:aws:iam::aws:policy/service-role/AmazonRekognitionServiceRole'
    AmazonSSMAutomationRole = 'arn:aws:iam::aws:policy/service-role/AmazonSSMAutomationRole'
    CloudHSMServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudHSMServiceRolePolicy'
    ComprehendReadOnly = 'arn:aws:iam::aws:policy/ComprehendReadOnly'
    AWSStepFunctionsConsoleFullAccess = 'arn:aws:iam::aws:policy/AWSStepFunctionsConsoleFullAccess'
    AWSQuickSightIoTAnalyticsAccess = 'arn:aws:iam::aws:policy/AWSQuickSightIoTAnalyticsAccess'
    AWSCodeBuildReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCodeBuildReadOnlyAccess'
    LexBotPolicy = 'arn:aws:iam::aws:policy/aws-service-role/LexBotPolicy'
    AmazonMacieFullAccess = 'arn:aws:iam::aws:policy/AmazonMacieFullAccess'
    AmazonMachineLearningManageRealTimeEndpointOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMachineLearningManageRealTimeEndpointOnlyAccess'
    CloudWatchEventsInvocationAccess = 'arn:aws:iam::aws:policy/service-role/CloudWatchEventsInvocationAccess'
    CloudFrontReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudFrontReadOnlyAccess'
    AWSDeepLensServiceRolePolicy = 'arn:aws:iam::aws:policy/service-role/AWSDeepLensServiceRolePolicy'
    AmazonSNSRole = 'arn:aws:iam::aws:policy/service-role/AmazonSNSRole'
    AmazonInspectorServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonInspectorServiceRolePolicy'
    AmazonMobileAnalyticsFinancialReportAccess = 'arn:aws:iam::aws:policy/AmazonMobileAnalyticsFinancialReportAccess'
    AWSElasticBeanstalkService = 'arn:aws:iam::aws:policy/service-role/AWSElasticBeanstalkService'
    IAMReadOnlyAccess = 'arn:aws:iam::aws:policy/IAMReadOnlyAccess'
    AmazonRDSReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRDSReadOnlyAccess'
    AWSIoTDeviceDefenderAudit = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderAudit'
    AmazonCognitoPowerUser = 'arn:aws:iam::aws:policy/AmazonCognitoPowerUser'
    AmazonRoute53AutoNamingRegistrantAccess = 'arn:aws:iam::aws:policy/AmazonRoute53AutoNamingRegistrantAccess'
    AmazonElasticFileSystemFullAccess = 'arn:aws:iam::aws:policy/AmazonElasticFileSystemFullAccess'
    LexChannelPolicy = 'arn:aws:iam::aws:policy/aws-service-role/LexChannelPolicy'
    ServerMigrationConnector = 'arn:aws:iam::aws:policy/ServerMigrationConnector'
    AmazonESCognitoAccess = 'arn:aws:iam::aws:policy/AmazonESCognitoAccess'
    AWSFMAdminFullAccess = 'arn:aws:iam::aws:policy/AWSFMAdminFullAccess'
    AmazonChimeReadOnly = 'arn:aws:iam::aws:policy/AmazonChimeReadOnly'
    AmazonZocaloFullAccess = 'arn:aws:iam::aws:policy/AmazonZocaloFullAccess'
    AWSLambdaReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSLambdaReadOnlyAccess'
    AWSIoTSiteWiseReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSIoTSiteWiseReadOnlyAccess'
    AWSAccountUsageReportAccess = 'arn:aws:iam::aws:policy/AWSAccountUsageReportAccess'
    AWSIoTOTAUpdate = 'arn:aws:iam::aws:policy/service-role/AWSIoTOTAUpdate'
    AmazonMQFullAccess = 'arn:aws:iam::aws:policy/AmazonMQFullAccess'
    AWSMarketplaceGetEntitlements = 'arn:aws:iam::aws:policy/AWSMarketplaceGetEntitlements'
    AWSGreengrassReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSGreengrassReadOnlyAccess'
    AmazonEC2ContainerServiceforEC2Role = 'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role'
    AmazonAppStreamFullAccess = 'arn:aws:iam::aws:policy/AmazonAppStreamFullAccess'
    AWSIoTDataAccess = 'arn:aws:iam::aws:policy/AWSIoTDataAccess'
    AmazonWorkLinkFullAccess = 'arn:aws:iam::aws:policy/AmazonWorkLinkFullAccess'
    AmazonTranscribeReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonTranscribeReadOnlyAccess'
    AmazonESFullAccess = 'arn:aws:iam::aws:policy/AmazonESFullAccess'
    ServerMigrationServiceRole = 'arn:aws:iam::aws:policy/service-role/ServerMigrationServiceRole'
    ApplicationDiscoveryServiceContinuousExportServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ApplicationDiscoveryServiceContinuousExportServiceRolePolicy'
    AmazonSumerianFullAccess = 'arn:aws:iam::aws:policy/AmazonSumerianFullAccess'
    AWSWAFFullAccess = 'arn:aws:iam::aws:policy/AWSWAFFullAccess'
    ElasticLoadBalancingReadOnly = 'arn:aws:iam::aws:policy/ElasticLoadBalancingReadOnly'
    AWSArtifactAccountSync = 'arn:aws:iam::aws:policy/service-role/AWSArtifactAccountSync'
    AmazonKinesisFirehoseFullAccess = 'arn:aws:iam::aws:policy/AmazonKinesisFirehoseFullAccess'
    CloudWatchReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess'
    AWSLambdaBasicExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
    ResourceGroupsandTagEditorFullAccess = 'arn:aws:iam::aws:policy/ResourceGroupsandTagEditorFullAccess'
    AWSKeyManagementServicePowerUser = 'arn:aws:iam::aws:policy/AWSKeyManagementServicePowerUser'
    AWSApplicationAutoscalingEC2SpotFleetRequestPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingEC2SpotFleetRequestPolicy'
    AWSImportExportReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSImportExportReadOnlyAccess'
    CloudWatchEventsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudWatchEventsServiceRolePolicy'
    AmazonElasticTranscoderRole = 'arn:aws:iam::aws:policy/service-role/AmazonElasticTranscoderRole'
    AWSGlueConsoleFullAccess = 'arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess'
    AmazonEC2ContainerServiceRole = 'arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole'
    AWSDeviceFarmFullAccess = 'arn:aws:iam::aws:policy/AWSDeviceFarmFullAccess'
    AmazonSSMReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonSSMReadOnlyAccess'
    AWSStepFunctionsReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSStepFunctionsReadOnlyAccess'
    AWSMarketplaceReadonly = 'arn:aws:iam::aws:policy/AWSMarketplaceRead-only'
    AWSApplicationAutoscalingDynamoDBTablePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingDynamoDBTablePolicy'
    AWSCodePipelineFullAccess = 'arn:aws:iam::aws:policy/AWSCodePipelineFullAccess'
    AWSCloud9User = 'arn:aws:iam::aws:policy/AWSCloud9User'
    AWSGreengrassResourceAccessRolePolicy = 'arn:aws:iam::aws:policy/service-role/AWSGreengrassResourceAccessRolePolicy'
    AmazonMacieServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonMacieServiceRolePolicy'
    NetworkAdministrator = 'arn:aws:iam::aws:policy/job-function/NetworkAdministrator'
    AWSIoT1ClickFullAccess = 'arn:aws:iam::aws:policy/AWSIoT1ClickFullAccess'
    AmazonWorkSpacesApplicationManagerAdminAccess = 'arn:aws:iam::aws:policy/AmazonWorkSpacesApplicationManagerAdminAccess'
    AmazonDRSVPCManagement = 'arn:aws:iam::aws:policy/AmazonDRSVPCManagement'
    AmazonRedshiftServiceLinkedRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonRedshiftServiceLinkedRolePolicy'
    AWSCertificateManagerPrivateCAReadOnly = 'arn:aws:iam::aws:policy/AWSCertificateManagerPrivateCAReadOnly'
    AWSXrayFullAccess = 'arn:aws:iam::aws:policy/AWSXrayFullAccess'
    AWSElasticBeanstalkWorkerTier = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkWorkerTier'
    AWSDirectConnectFullAccess = 'arn:aws:iam::aws:policy/AWSDirectConnectFullAccess'
    AWSCodeBuildAdminAccess = 'arn:aws:iam::aws:policy/AWSCodeBuildAdminAccess'
    AmazonKinesisAnalyticsFullAccess = 'arn:aws:iam::aws:policy/AmazonKinesisAnalyticsFullAccess'
    AWSSecurityHubServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSSecurityHubServiceRolePolicy'
    AWSElasticBeanstalkMaintenance = 'arn:aws:iam::aws:policy/aws-service-role/AWSElasticBeanstalkMaintenance'
    APIGatewayServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/APIGatewayServiceRolePolicy'
    AWSAccountActivityAccess = 'arn:aws:iam::aws:policy/AWSAccountActivityAccess'
    AmazonGlacierFullAccess = 'arn:aws:iam::aws:policy/AmazonGlacierFullAccess'
    AmazonFSxConsoleReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonFSxConsoleReadOnlyAccess'
    AmazonWorkMailFullAccess = 'arn:aws:iam::aws:policy/AmazonWorkMailFullAccess'
    DAXServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/DAXServiceRolePolicy'
    ComprehendMedicalFullAccess = 'arn:aws:iam::aws:policy/ComprehendMedicalFullAccess'
    AWSMarketplaceManageSubscriptions = 'arn:aws:iam::aws:policy/AWSMarketplaceManageSubscriptions'
    AWSElasticBeanstalkCustomPlatformforEC2Role = 'arn:aws:iam::aws:policy/AWSElasticBeanstalkCustomPlatformforEC2Role'
    AWSDataSyncReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSDataSyncReadOnlyAccess'
    AWSVPCTransitGatewayServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSVPCTransitGatewayServiceRolePolicy'
    NeptuneReadOnlyAccess = 'arn:aws:iam::aws:policy/NeptuneReadOnlyAccess'
    AWSSupportAccess = 'arn:aws:iam::aws:policy/AWSSupportAccess'
    AmazonElasticMapReduceforAutoScalingRole = 'arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforAutoScalingRole'
    AWSElementalMediaConvertReadOnly = 'arn:aws:iam::aws:policy/AWSElementalMediaConvertReadOnly'
    AWSLambdaInvocationDynamoDB = 'arn:aws:iam::aws:policy/AWSLambdaInvocation-DynamoDB'
    AWSServiceCatalogEndUserFullAccess = 'arn:aws:iam::aws:policy/AWSServiceCatalogEndUserFullAccess'
    IAMUserSSHKeys = 'arn:aws:iam::aws:policy/IAMUserSSHKeys'
    AWSDeepRacerServiceRolePolicy = 'arn:aws:iam::aws:policy/service-role/AWSDeepRacerServiceRolePolicy'
    AmazonSageMakerReadOnly = 'arn:aws:iam::aws:policy/AmazonSageMakerReadOnly'
    AWSIoTFullAccess = 'arn:aws:iam::aws:policy/AWSIoTFullAccess'
    AWSQuickSightDescribeRDS = 'arn:aws:iam::aws:policy/service-role/AWSQuickSightDescribeRDS'
    AWSResourceAccessManagerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSResourceAccessManagerServiceRolePolicy'
    AWSConfigRulesExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSConfigRulesExecutionRole'
    AWSConfigServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSConfigServiceRolePolicy'
    AmazonESReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonESReadOnlyAccess'
    AWSCodeDeployDeployerAccess = 'arn:aws:iam::aws:policy/AWSCodeDeployDeployerAccess'
    KafkaServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/KafkaServiceRolePolicy'
    AmazonPollyFullAccess = 'arn:aws:iam::aws:policy/AmazonPollyFullAccess'
    AmazonSSMMaintenanceWindowRole = 'arn:aws:iam::aws:policy/service-role/AmazonSSMMaintenanceWindowRole'
    AmazonRDSEnhancedMonitoringRole = 'arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole'
    AmazonLexFullAccess = 'arn:aws:iam::aws:policy/AmazonLexFullAccess'
    AWSLambdaVPCAccessExecutionRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
    AmazonMacieServiceRole = 'arn:aws:iam::aws:policy/service-role/AmazonMacieServiceRole'
    AmazonLexRunBotsOnly = 'arn:aws:iam::aws:policy/AmazonLexRunBotsOnly'
    AWSCertificateManagerPrivateCAAuditor = 'arn:aws:iam::aws:policy/AWSCertificateManagerPrivateCAAuditor'
    AmazonSNSFullAccess = 'arn:aws:iam::aws:policy/AmazonSNSFullAccess'
    AmazonEKS_CNI_Policy = 'arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy'
    AWSServiceCatalogAdminFullAccess = 'arn:aws:iam::aws:policy/AWSServiceCatalogAdminFullAccess'
    AWSShieldDRTAccessPolicy = 'arn:aws:iam::aws:policy/service-role/AWSShieldDRTAccessPolicy'
    CloudSearchReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudSearchReadOnlyAccess'
    AWSGreengrassFullAccess = 'arn:aws:iam::aws:policy/AWSGreengrassFullAccess'
    NeptuneConsoleFullAccess = 'arn:aws:iam::aws:policy/NeptuneConsoleFullAccess'
    AWSCloudFormationReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSCloudFormationReadOnlyAccess'
    AmazonRoute53FullAccess = 'arn:aws:iam::aws:policy/AmazonRoute53FullAccess'
    AWSLambdaRole = 'arn:aws:iam::aws:policy/service-role/AWSLambdaRole'
    AWSLambdaENIManagementAccess = 'arn:aws:iam::aws:policy/service-role/AWSLambdaENIManagementAccess'
    AWSOpsWorksCloudWatchLogs = 'arn:aws:iam::aws:policy/AWSOpsWorksCloudWatchLogs'
    AmazonAppStreamReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonAppStreamReadOnlyAccess'
    AWSStepFunctionsFullAccess = 'arn:aws:iam::aws:policy/AWSStepFunctionsFullAccess'
    CloudTrailServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudTrailServiceRolePolicy'
    AmazonInspectorReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonInspectorReadOnlyAccess'
    AWSOrganizationsReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSOrganizationsReadOnlyAccess'
    TranslateReadOnly = 'arn:aws:iam::aws:policy/TranslateReadOnly'
    AWSCertificateManagerFullAccess = 'arn:aws:iam::aws:policy/AWSCertificateManagerFullAccess'
    AWSDeepRacerCloudFormationAccessPolicy = 'arn:aws:iam::aws:policy/AWSDeepRacerCloudFormationAccessPolicy'
    AWSIoTEventsReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSIoTEventsReadOnlyAccess'
    AWSRoboMakerServicePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSRoboMakerServicePolicy'
    PowerUserAccess = 'arn:aws:iam::aws:policy/PowerUserAccess'
    AWSApplicationAutoScalingCustomResourcePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoScalingCustomResourcePolicy'
    GlobalAcceleratorReadOnlyAccess = 'arn:aws:iam::aws:policy/GlobalAcceleratorReadOnlyAccess'
    AmazonSageMakerFullAccess = 'arn:aws:iam::aws:policy/AmazonSageMakerFullAccess'
    WAFLoggingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/WAFLoggingServiceRolePolicy'
    AWSBackupServiceRolePolicyForRestores = 'arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores'
    AWSElementalMediaStoreFullAccess = 'arn:aws:iam::aws:policy/AWSElementalMediaStoreFullAccess'
    CloudWatchEventsFullAccess = 'arn:aws:iam::aws:policy/CloudWatchEventsFullAccess'
    AWSLicenseManagerMemberAccountRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSLicenseManagerMemberAccountRolePolicy'
    AWSOrganizationsFullAccess = 'arn:aws:iam::aws:policy/AWSOrganizationsFullAccess'
    AmazonFraudDetectorFullAccessPolicy = 'arn:aws:iam::aws:policy/AmazonFraudDetectorFullAccessPolicy'
    AmazonChimeSDK = 'arn:aws:iam::aws:policy/AmazonChimeSDK'
    AWSIoTDeviceTesterForFreeRTOSFullAccess = 'arn:aws:iam::aws:policy/AWSIoTDeviceTesterForFreeRTOSFullAccess'
    WAFV2LoggingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/WAFV2LoggingServiceRolePolicy'
    AWSNetworkManagerFullAccess = 'arn:aws:iam::aws:policy/AWSNetworkManagerFullAccess'
    AWSPrivateMarketplaceRequests = 'arn:aws:iam::aws:policy/AWSPrivateMarketplaceRequests'
    AmazonSageMakerMechanicalTurkAccess = 'arn:aws:iam::aws:policy/AmazonSageMakerMechanicalTurkAccess'
    AWSNetworkManagerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSNetworkManagerServiceRolePolicy'
    AWSAppMeshServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSAppMeshServiceRolePolicy'
    AWSConfigRemediationServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSConfigRemediationServiceRolePolicy'
    ConfigConformsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ConfigConformsServiceRolePolicy'
    AmazonEventBridgeReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonEventBridgeReadOnlyAccess'
    AWSCodeStarNotificationsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSCodeStarNotificationsServiceRolePolicy'
    AmazonKendraFullAccess = 'arn:aws:iam::aws:policy/AmazonKendraFullAccess'
    AWSSystemsManagerAccountDiscoveryServicePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSSystemsManagerAccountDiscoveryServicePolicy'
    AWSResourceAccessManagerReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSResourceAccessManagerReadOnlyAccess'
    AmazonEventBridgeFullAccess = 'arn:aws:iam::aws:policy/AmazonEventBridgeFullAccess'
    CloudWatchSyntheticsReadOnlyAccess = 'arn:aws:iam::aws:policy/CloudWatchSyntheticsReadOnlyAccess'
    AccessAnalyzerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AccessAnalyzerServiceRolePolicy'
    AmazonRoute53ResolverReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonRoute53ResolverReadOnlyAccess'
    AmazonEC2RolePolicyForLaunchWizard = 'arn:aws:iam::aws:policy/AmazonEC2RolePolicyForLaunchWizard'
    AmazonManagedBlockchainFullAccess = 'arn:aws:iam::aws:policy/AmazonManagedBlockchainFullAccess'
    ServiceQuotasFullAccess = 'arn:aws:iam::aws:policy/ServiceQuotasFullAccess'
    AWSIoTSiteWiseMonitorServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSIoTSiteWiseMonitorServiceRolePolicy'
    AWSCloudFormationFullAccess = 'arn:aws:iam::aws:policy/AWSCloudFormationFullAccess'
    ElementalAppliancesSoftwareFullAccess = 'arn:aws:iam::aws:policy/ElementalAppliancesSoftwareFullAccess'
    AmazonAugmentedAIHumanLoopFullAccess = 'arn:aws:iam::aws:policy/AmazonAugmentedAIHumanLoopFullAccess'
    AWSDataExchangeReadOnly = 'arn:aws:iam::aws:policy/AWSDataExchangeReadOnly'
    AWSMarketplaceSellerProductsFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceSellerProductsFullAccess'
    AWSIQContractServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSIQContractServiceRolePolicy'
    AmazonLaunchWizardFullaccess = 'arn:aws:iam::aws:policy/AmazonLaunchWizardFullaccess'
    AmazonWorkDocsReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonWorkDocsReadOnlyAccess'
    AWSGlobalAcceleratorSLRPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSGlobalAcceleratorSLRPolicy'
    EC2InstanceProfileForImageBuilder = 'arn:aws:iam::aws:policy/EC2InstanceProfileForImageBuilder'
    AWSServiceRoleForLogDeliveryPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForLogDeliveryPolicy'
    AmazonCodeGuruReviewerFullAccess = 'arn:aws:iam::aws:policy/AmazonCodeGuruReviewerFullAccess'
    AWSVPCS2SVpnServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSVPCS2SVpnServiceRolePolicy'
    AWSImageBuilderFullAccess = 'arn:aws:iam::aws:policy/AWSImageBuilderFullAccess'
    AWSCertificateManagerPrivateCAPrivilegedUser = 'arn:aws:iam::aws:policy/AWSCertificateManagerPrivateCAPrivilegedUser'
    AWSOpsWorksRegisterCLI_OnPremises = 'arn:aws:iam::aws:policy/AWSOpsWorksRegisterCLI_OnPremises'
    Health_OrganizationsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/Health_OrganizationsServiceRolePolicy'
    AmazonMCSReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonMCSReadOnlyAccess'
    AWSAppMeshPreviewServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSAppMeshPreviewServiceRolePolicy'
    ServiceQuotasServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ServiceQuotasServiceRolePolicy'
    ComputeOptimizerReadOnlyAccess = 'arn:aws:iam::aws:policy/ComputeOptimizerReadOnlyAccess'
    AlexaForBusinessPolyDelegatedAccessPolicy = 'arn:aws:iam::aws:policy/AlexaForBusinessPolyDelegatedAccessPolicy'
    AWSMarketplaceProcurementSystemAdminFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceProcurementSystemAdminFullAccess'
    AmazonEKSFargatePodExecutionRolePolicy = 'arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy'
    IAMAccessAdvisorReadOnly = 'arn:aws:iam::aws:policy/IAMAccessAdvisorReadOnly'
    AmazonCodeGuruReviewerReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonCodeGuruReviewerReadOnlyAccess'
    AmazonCodeGuruProfilerFullAccess = 'arn:aws:iam::aws:policy/AmazonCodeGuruProfilerFullAccess'
    AmazonElasticFileSystemServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonElasticFileSystemServiceRolePolicy'
    AWSResourceAccessManagerFullAccess = 'arn:aws:iam::aws:policy/AWSResourceAccessManagerFullAccess'
    AWSIoTDeviceDefenderEnableIoTLoggingMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderEnableIoTLoggingMitigationAction'
    DynamoDBCloudWatchContributorInsightsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/DynamoDBCloudWatchContributorInsightsServiceRolePolicy'
    AmazonChimeVoiceConnectorServiceLinkedRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonChimeVoiceConnectorServiceLinkedRolePolicy'
    IAMAccessAnalyzerReadOnlyAccess = 'arn:aws:iam::aws:policy/IAMAccessAnalyzerReadOnlyAccess'
    AmazonEventBridgeSchemasServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonEventBridgeSchemasServiceRolePolicy'
    AWSIoTDeviceDefenderPublishFindingsToSNSMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderPublishFindingsToSNSMitigationAction'
    AmazonQLDBConsoleFullAccess = 'arn:aws:iam::aws:policy/AmazonQLDBConsoleFullAccess'
    AmazonElasticFileSystemClientReadWriteAccess = 'arn:aws:iam::aws:policy/AmazonElasticFileSystemClientReadWriteAccess'
    AWSApplicationAutoscalingComprehendEndpointPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingComprehendEndpointPolicy'
    AWSIoTDeviceDefenderAddThingsToThingGroupMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderAddThingsToThingGroupMitigationAction'
    AmazonQLDBFullAccess = 'arn:aws:iam::aws:policy/AmazonQLDBFullAccess'
    AmazonAugmentedAIFullAccess = 'arn:aws:iam::aws:policy/AmazonAugmentedAIFullAccess'
    AWSIoTDeviceDefenderReplaceDefaultPolicyMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderReplaceDefaultPolicyMitigationAction'
    AWSAppMeshReadOnly = 'arn:aws:iam::aws:policy/AWSAppMeshReadOnly'
    ComputeOptimizerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/ComputeOptimizerServiceRolePolicy'
    AWSElasticBeanstalkManagedUpdatesServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSElasticBeanstalkManagedUpdatesServiceRolePolicy'
    AmazonQLDBReadOnly = 'arn:aws:iam::aws:policy/AmazonQLDBReadOnly'
    AWSChatbotServiceLinkedRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSChatbotServiceLinkedRolePolicy'
    AWSAppSyncServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSAppSyncServiceRolePolicy'
    AWSAppMeshFullAccess = 'arn:aws:iam::aws:policy/AWSAppMeshFullAccess'
    AWSServiceRoleForGammaInternalAmazonEKSNodegroup = 'arn:aws:iam::aws:policy/AWSServiceRoleForGammaInternalAmazonEKSNodegroup'
    ServiceQuotasReadOnlyAccess = 'arn:aws:iam::aws:policy/ServiceQuotasReadOnlyAccess'
    EC2FleetTimeShiftableServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/EC2FleetTimeShiftableServiceRolePolicy'
    MigrationHubDMSAccessServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/MigrationHubDMSAccessServiceRolePolicy'
    AWSServiceCatalogEndUserReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSServiceCatalogEndUserReadOnlyAccess'
    AWSIQPermissionServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSIQPermissionServiceRolePolicy'
    AmazonEKSForFargateServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonEKSForFargateServiceRolePolicy'
    MigrationHubSMSAccessServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/MigrationHubSMSAccessServiceRolePolicy'
    CloudFormationStackSetsOrgAdminServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudFormationStackSetsOrgAdminServiceRolePolicy'
    AmazonEventBridgeSchemasFullAccess = 'arn:aws:iam::aws:policy/AmazonEventBridgeSchemasFullAccess'
    AWSMarketplaceSellerFullAccess = 'arn:aws:iam::aws:policy/AWSMarketplaceSellerFullAccess'
    CloudWatchAutomaticDashboardsAccess = 'arn:aws:iam::aws:policy/CloudWatchAutomaticDashboardsAccess'
    AmazonWorkMailEventsServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonWorkMailEventsServiceRolePolicy'
    AmazonEventBridgeSchemasReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonEventBridgeSchemasReadOnlyAccess'
    AWSMarketplaceSellerProductsReadOnly = 'arn:aws:iam::aws:policy/AWSMarketplaceSellerProductsReadOnly'
    AmazonMCSFullAccess = 'arn:aws:iam::aws:policy/AmazonMCSFullAccess'
    AWSIoTSiteWiseConsoleFullAccess = 'arn:aws:iam::aws:policy/AWSIoTSiteWiseConsoleFullAccess'
    AmazonElasticFileSystemClientFullAccess = 'arn:aws:iam::aws:policy/AmazonElasticFileSystemClientFullAccess'
    AWSIoTDeviceDefenderUpdateDeviceCertMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderUpdateDeviceCertMitigationAction'
    AWSForWordPressPluginPolicy = 'arn:aws:iam::aws:policy/AWSForWordPressPluginPolicy'
    AWSServiceRoleForAmazonEKSNodegroup = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForAmazonEKSNodegroup'
    AWSBackupOperatorAccess = 'arn:aws:iam::aws:policy/AWSBackupOperatorAccess'
    AWSApplicationAutoscalingLambdaConcurrencyPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSApplicationAutoscalingLambdaConcurrencyPolicy'
    AmazonMachineLearningRoleforRedshiftDataSourceV2 = 'arn:aws:iam::aws:policy/service-role/AmazonMachineLearningRoleforRedshiftDataSourceV2'
    AWSIoTDeviceDefenderUpdateCACertMitigationAction = 'arn:aws:iam::aws:policy/service-role/AWSIoTDeviceDefenderUpdateCACertMitigationAction'
    AmazonWorkSpacesServiceAccess = 'arn:aws:iam::aws:policy/AmazonWorkSpacesServiceAccess'
    AmazonEKSServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonEKSServiceRolePolicy'
    AWSConfigMultiAccountSetupPolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSConfigMultiAccountSetupPolicy'
    AmazonElasticFileSystemClientReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonElasticFileSystemClientReadOnlyAccess'
    CloudFormationStackSetsOrgMemberServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/CloudFormationStackSetsOrgMemberServiceRolePolicy'
    AWSResourceAccessManagerResourceShareParticipantAccess = 'arn:aws:iam::aws:policy/AWSResourceAccessManagerResourceShareParticipantAccess'
    AWSBackupFullAccess = 'arn:aws:iam::aws:policy/AWSBackupFullAccess'
    AmazonCodeGuruProfilerReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonCodeGuruProfilerReadOnlyAccess'
    AWSNetworkManagerReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSNetworkManagerReadOnlyAccess'
    CloudWatchSyntheticsFullAccess = 'arn:aws:iam::aws:policy/CloudWatchSyntheticsFullAccess'
    AWSDataExchangeSubscriberFullAccess = 'arn:aws:iam::aws:policy/AWSDataExchangeSubscriberFullAccess'
    IAMAccessAnalyzerFullAccess = 'arn:aws:iam::aws:policy/IAMAccessAnalyzerFullAccess'
    AWSServiceCatalogAdminReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSServiceCatalogAdminReadOnlyAccess'
    AWSQuickSightSageMakerPolicy = 'arn:aws:iam::aws:policy/service-role/AWSQuickSightSageMakerPolicy'
    AmazonWorkSpacesSelfServiceAccess = 'arn:aws:iam::aws:policy/AmazonWorkSpacesSelfServiceAccess'
    AmazonManagedBlockchainServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonManagedBlockchainServiceRolePolicy'
    AWSDataExchangeFullAccess = 'arn:aws:iam::aws:policy/AWSDataExchangeFullAccess'
    AWSDataExchangeProviderFullAccess = 'arn:aws:iam::aws:policy/AWSDataExchangeProviderFullAccess'
    AWSControlTowerServiceRolePolicy = 'arn:aws:iam::aws:policy/service-role/AWSControlTowerServiceRolePolicy'
    AmazonSageMakerNotebooksServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonSageMakerNotebooksServiceRolePolicy'
    AmazonRoute53ResolverFullAccess = 'arn:aws:iam::aws:policy/AmazonRoute53ResolverFullAccess'
    LakeFormationDataAccessServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/LakeFormationDataAccessServiceRolePolicy'
    AmazonChimeServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonChimeServiceRolePolicy'
    AWSTrustedAdvisorReportingServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AWSTrustedAdvisorReportingServiceRolePolicy'
    AWSOpsWorksRegisterCLI_EC2 = 'arn:aws:iam::aws:policy/AWSOpsWorksRegisterCLI_EC2'
    AWSSavingsPlansFullAccess = 'arn:aws:iam::aws:policy/AWSSavingsPlansFullAccess'
    AWSServiceRoleForImageBuilder = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForImageBuilder'
    AmazonCodeGuruReviewerServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/AmazonCodeGuruReviewerServiceRolePolicy'
    AWSAppMeshPreviewEnvoyAccess = 'arn:aws:iam::aws:policy/AWSAppMeshPreviewEnvoyAccess'
    MigrationHubServiceRolePolicy = 'arn:aws:iam::aws:policy/aws-service-role/MigrationHubServiceRolePolicy'
    AWSImageBuilderReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSImageBuilderReadOnlyAccess'
    AWSMarketplaceMeteringRegisterUsage = 'arn:aws:iam::aws:policy/AWSMarketplaceMeteringRegisterUsage'
    AmazonManagedBlockchainReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonManagedBlockchainReadOnlyAccess'
    AmazonRekognitionCustomLabelsFullAccess = 'arn:aws:iam::aws:policy/AmazonRekognitionCustomLabelsFullAccess'
    AmazonManagedBlockchainConsoleFullAccess = 'arn:aws:iam::aws:policy/AmazonManagedBlockchainConsoleFullAccess'
    AWSSavingsPlansReadOnlyAccess = 'arn:aws:iam::aws:policy/AWSSavingsPlansReadOnlyAccess'
    AWSIoTDeviceTesterForGreengrassFullAccess = 'arn:aws:iam::aws:policy/AWSIoTDeviceTesterForGreengrassFullAccess'
    AWSServiceRoleForSMS = 'arn:aws:iam::aws:policy/aws-service-role/AWSServiceRoleForSMS'
    CloudWatchCrossAccountAccess = 'arn:aws:iam::aws:policy/aws-service-role/CloudWatch-CrossAccountAccess'
    AWSLakeFormationDataAdmin = 'arn:aws:iam::aws:policy/AWSLakeFormationDataAdmin'
    AWSDenyAll = 'arn:aws:iam::aws:policy/AWSDenyAll'
    AWSIQFullAccess = 'arn:aws:iam::aws:policy/AWSIQFullAccess'
    EC2InstanceConnect = 'arn:aws:iam::aws:policy/EC2InstanceConnect'
    AWSAppMeshEnvoyAccess = 'arn:aws:iam::aws:policy/AWSAppMeshEnvoyAccess'
    AmazonKendraReadOnlyAccess = 'arn:aws:iam::aws:policy/AmazonKendraReadOnlyAccess'