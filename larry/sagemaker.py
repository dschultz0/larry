import json
import boto3
import posixpath
import base64

from larry.core import copy_non_null_keys
from larry import s3
from larry import lmbda
from larry.core.ipython import display_iframe
from larry.core import is_arn
from larry.types import ClientError
from larry.utils.image import scale_image_to_size
from collections.abc import Mapping

# A local instance of the boto3 session to use
__session = boto3.session.Session()
__client = __session.client('sagemaker')


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
    global __session, __client
    __session = boto_session if boto_session is not None else boto3.session.Session(**copy_non_null_keys(locals()))
    __client = __session.client('sagemaker')


def __getattr__(name):
    if name == 'session':
        return __session
    elif name == 'client':
        return __client
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


def _get_client():
    return __client


def _resolve_region(region):
    return __session.region_name if region is None else region


class notebook:

    @staticmethod
    def update_lifecycle_config(name, on_start=None, on_create=None):
        if on_create is None and on_start is None:
            raise TypeError('A value for OnCreate or OnStart must be provided')
        params = {
            'NotebookInstanceLifecycleConfigName': name
        }
        if on_create:
            params['OnCreate'] = {'Content': base64.b64encode(on_create.encode('ascii')).decode('ascii')}
        if on_start:
            params['OnStart'] = {'Content': base64.b64encode(on_start.encode('ascii')).decode('ascii')}
        _get_client().update_notebook_instance_lifecycle_config(**params)


class labeling:
    global __session

    @staticmethod
    def display_task_preview(template, role, template_input=None, pre_lambda=None, lambda_input=None,
                             width=None, height=600):
        """
        Renders the UI template in an iframe within Jupyter or JupyterLab so that you can preview the worker's
        experience. Either a template_input or the pre_lambda and lambda_input can be provided to pass to the template.
        :param template: Template HTML, S3 URI, or file path to use as the template
        :param role: ARN for the IAM role that has access to the necessary resources (typically your SageMaker
        Execution role)
        :param template_input: Data to pass to the template (contents of the taskInput value returned from the pre
        lambda)
        :param pre_lambda: The ARN of a Lambda function that is run before a data object is sent to a human worker.
        :param lambda_input: A data record that you plan to submit to Ground Truth
        :param width: The width in pixels of the iframe, defaults to the maximum width possible within the Jupyter cell
        :param height: The height in pixels of the iframe
        :return: A tuple containing the rendered HTML and any errors.
        """
        html, errors = labeling.render_ui_template(template, role=role, template_input=template_input,
                                                   pre_lambda=pre_lambda, lambda_input=lambda_input)
        if errors and len(errors) > 0:
            if len(errors) == 1:
                raise Exception('An error occurred in rendering: {}'.format(errors[0]))
            else:
                raise Exception('{} errors occurred in rendering: {}'.format(len(errors), json.dumps(errors)))
        else:
            display_iframe(html=html, width=width, height=height)

    @staticmethod
    def render_ui_template(template, role, template_input=None, pre_lambda=None, lambda_input=None):
        """
        Renders the UI template to HTML so that you can preview the worker's experience. Either a template_input or
        the pre_lambda and lambda_input can be provided to pass to the template.
        :param template: Template HTML, S3 URI, or file path to use as the template
        :param role: ARN for the IAM role that has access to the necessary resources (typically your SageMaker
        Execution role)
        :param template_input: Data to pass to the template (contents of the taskInput value returned from the pre
        lambda)
        :param pre_lambda: The ARN of a Lambda function that is run before a data object is sent to a human worker.
        :param lambda_input: A data record that you plan to submit to Ground Truth
        :param client: Sagemaker client to use in place of the default value
        :return: A tuple containing the rendered HTML and any errors.
        """

        # if this isn't template html attempt to treat it as a file (s3 or local)
        if '<' not in template or '>' not in template:

            # If the template is a uri, use that
            parts = s3.split_uri(template)
            if parts[0] is not None and parts[1] is not None:
                template = s3.read_as(str, template)

            # else try to open it like a file
            else:
                try:
                    with open(template, 'r') as fp:
                        template = fp.read()
                except IOError:
                    pass

        if pre_lambda:
            template_input = lmbda.invoke_as_dict(pre_lambda, {'dataObject': lambda_input}).get('taskInput')

        if isinstance(template_input, Mapping):
            template_input = json.dumps(template_input)

        result = _get_client().render_ui_template(UiTemplate={'Content': template},
                                                  Task={'Input': template_input},
                                                  RoleArn=role)
        return result['RenderedContent'], result.get('Errors')

    @staticmethod
    def _input_config(manifest_uri, free_of_pii=False, free_of_adult_content=True):
        config = {
            'DataSource': {
                'S3DataSource': {
                    'ManifestS3Uri': manifest_uri
                }
            }
        }
        content_classifiers = []
        if free_of_adult_content:
            content_classifiers.append('FreeOfAdultContent')
        if free_of_pii:
            content_classifiers.append('FreeOfPersonallyIdentifiableInformation')
        if len(content_classifiers) > 0:
            config['DataAttributes'] = {'ContentClassifiers': content_classifiers}
        return config

    @staticmethod
    def _output_config(output_uri, kms_key=None):
        config = {
            'S3OutputPath': output_uri
        }
        if kms_key:
            config['KmsKeyId'] = kms_key
        return config

    @staticmethod
    def build_human_task_config(template_uri, pre_lambda, consolidation_lambda, title, description, workers=1,
                                public=False, reward_in_cents=None, workteam_arn=None, time_limit=300, lifetime=345600,
                                max_concurrent_tasks=None, keywords=None, region=None):

        region = _resolve_region(region)
        if not is_arn(pre_lambda):
            pre_lambda = lmbda.get(pre_lambda)['FunctionArn']
        if not is_arn(consolidation_lambda):
            consolidation_lambda = lmbda.get(consolidation_lambda)['FunctionArn']
        config = {
            'UiConfig': {
                'UiTemplateS3Uri': template_uri.uri if isinstance(template_uri, s3.Object) else template_uri
            },
            'PreHumanTaskLambdaArn': pre_lambda,
            'TaskTitle': title,
            'TaskDescription': description,
            'NumberOfHumanWorkersPerDataObject': workers,
            'TaskTimeLimitInSeconds': time_limit,
            'TaskAvailabilityLifetimeInSeconds': lifetime,
            'AnnotationConsolidationConfig': {
                'AnnotationConsolidationLambdaArn': consolidation_lambda
            }
        }
        if public:
            config['WorkteamArn'] = 'arn:aws:sagemaker:{}:394669845002:workteam/public-crowd/default'.format(region)
            if reward_in_cents is None:
                raise Exception('You must provide a reward amount for a public labeling job')
            else:
                config['PublicWorkforceTaskPrice'] = {
                    'AmountInUsd': {
                        'Dollars': int(reward_in_cents // 100),
                        'Cents': int(reward_in_cents),
                        'TenthFractionsOfACent': round((reward_in_cents % 1) * 10)
                    }
                }
        elif workteam_arn is not None:
            config['WorkteamArn'] = workteam_arn
        else:
            raise Exception('Labeling job must be public or have a workteam ARN')
        if keywords:
            config['TaskKeywords'] = keywords
        if max_concurrent_tasks:
            config['MaxConcurrentTaskCount'] = max_concurrent_tasks
        return config

    @staticmethod
    def build_stopping_conditions(max_human_labeled_object_count=None, max_percentage_labeled=None):
        if max_human_labeled_object_count is None or max_percentage_labeled is None:
            return None
        else:
            config = {}
            if max_human_labeled_object_count:
                config['MaxHumanLabeledObjectCount'] = max_human_labeled_object_count
            if max_percentage_labeled:
                config['MaxPercentageOfInputDatasetLabeled'] = max_percentage_labeled
            return config

    @staticmethod
    def build_algorithms_config(algorithm_specification_arn, initial_active_learning_model_arn=None, kms_key=None):
        if algorithm_specification_arn is None:
            return None
        config = {
            'LabelingJobAlgorithmSpecificationArn': algorithm_specification_arn
        }
        if initial_active_learning_model_arn:
            config['InitialActiveLearningModelArn'] = initial_active_learning_model_arn
        if kms_key:
            config['LabelingJobResourceConfig'] = {'VolumeKmsKeyId': kms_key}
        return config

    @staticmethod
    def create_job(name,
                   manifest_uri,
                   output_uri,
                   role_arn,
                   task_config,
                   category_config_uri=None,
                   label_attribute_name=None,
                   free_of_pii=False,
                   free_of_adult_content=True,
                   algorithms_config=None,
                   stopping_conditions=None):
        if label_attribute_name is None:
            label_attribute_name = name
        params = {
            'LabelingJobName': name,
            'LabelAttributeName': label_attribute_name,
            'InputConfig': labeling._input_config(
                manifest_uri.uri if isinstance(manifest_uri, s3.Object) else manifest_uri,
                free_of_pii,
                free_of_adult_content),
            'OutputConfig': labeling._output_config(
                output_uri.uri if isinstance(output_uri, s3.Object) else output_uri),
            'RoleArn': role_arn,
            'HumanTaskConfig': task_config
        }
        if category_config_uri:
            params['LabelCategoryConfigS3Uri'] = category_config_uri.uri if isinstance(category_config_uri,
                                                                                       s3.Object) else category_config_uri
        if algorithms_config:
            params['LabelingJobAlgorithmsConfig'] = algorithms_config
        if stopping_conditions:
            params['StoppingConditions'] = stopping_conditions
        return _get_client().create_labeling_job(**params)['LabelingJobArn']

    @staticmethod
    def describe_job(name):
        return _get_client().describe_labeling_job(LabelingJobName=name)

    @staticmethod
    def get_job_state(name):
        response = labeling.describe_job(name)
        status = response['LabelingJobStatus']
        labeled = response['LabelCounters']['TotalLabeled']
        unlabeled = response['LabelCounters']['Unlabeled']
        failed = response['LabelCounters']['FailedNonRetryableError']
        fail_message = ' {} failed'.format(failed) if failed > 0 else ''
        return "{} ({}/{})".format(status, labeled, unlabeled + labeled) + fail_message

    @staticmethod
    def get_worker_responses(output_uri, job_name):
        by_worker = {}
        by_item = {}
        output_uri = output_uri.uri if isinstance(output_uri, s3.Object) else output_uri
        bucket_name, k = s3.split_uri(output_uri)
        for response_key in s3.list_objects(uri=posixpath.join(output_uri, job_name, 'annotations/worker-response')):
            response_obj = s3.read_as(dict, bucket_name, response_key)
            item_id = response_key.split('/')[-2]
            by_item[item_id] = response_obj
            for response in response_obj['answers']:
                worker_id = response['workerId']
                responses = by_worker.get(worker_id, [])
                response['itemId'] = item_id
                responses.append(response)
                by_worker[worker_id] = responses
        return by_item, by_worker

    @staticmethod
    def stop_job(name):
        _get_client().stop_labeling_job(LabelingJobName=name)

    @staticmethod
    def get_results(output_uri, job_name):
        output_uri = output_uri.uri if isinstance(output_uri, s3.Object) else output_uri
        try:
            return s3.read_as([dict], uri=posixpath.join(output_uri, job_name, 'manifests/output/output.manifest'))
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return []
            else:
                raise

    @staticmethod
    def get_multiple_results(output_uri, job_names, rename_label_to=None, exclude_failures=True):
        cumulative_results = []
        for job_name in job_names:
            results = labeling.get_results(output_uri, job_name)
            for item in results:
                if item[job_name + '-metadata'].get('failure-reason') is None or exclude_failures is False:
                    if rename_label_to:
                        item[rename_label_to] = item.pop(job_name)
                        item[rename_label_to + '-metadata'] = item.pop(job_name + '-metadata')
                    cumulative_results.append(item)
        return cumulative_results

    @staticmethod
    def find_failures(manifest, attribute_name):
        failures = []
        reasons = {}
        for item in manifest:
            failure_reason = item[attribute_name + '-metadata'].get('failure-reason')
            if failure_reason:
                failures.append(item)
                failure_reason = failure_reason.replace(item['source-ref'], '<file>')
                failure_reason = failure_reason.replace(item['source-ref'].replace('/', '\\/'), '<file>')
                cnt = reasons.get(failure_reason, 0)
                reasons[failure_reason] = cnt + 1
        return failures, reasons

    @staticmethod
    def built_in_pre_lambda_bounding_box(region=None):
        return labeling._built_in_lambda('PRE', _resolve_region(region), 'BoundingBox')

    @staticmethod
    def built_in_pre_lambda_image_multi_class(region=None):
        return labeling._built_in_lambda('PRE', _resolve_region(region), 'ImageMultiClass')

    @staticmethod
    def built_in_pre_lambda_semantic_segmentation(region=None):
        return labeling._built_in_lambda('PRE', _resolve_region(region), 'SemanticSegmentation')

    @staticmethod
    def built_in_pre_lambda_text_multi_class(region=None):
        return labeling._built_in_lambda('PRE', _resolve_region(region), 'TextMultiClass')

    @staticmethod
    def built_in_pre_lambda_named_entity_recognition(region=None):
        return labeling._built_in_lambda('PRE', _resolve_region(region), 'NamedEntityRecognition')

    @staticmethod
    def built_in_acs_lambda_bounding_box(region=None):
        return labeling._built_in_lambda('ACS', _resolve_region(region), 'BoundingBox')

    @staticmethod
    def built_in_acs_lambda_image_multi_class(region=None):
        return labeling._built_in_lambda('ACS', _resolve_region(region), 'ImageMultiClass')

    @staticmethod
    def built_in_acs_lambda_semantic_segmentation(region=None):
        return labeling._built_in_lambda('ACS', _resolve_region(region), 'SemanticSegmentation')

    @staticmethod
    def built_in_acs_lambda_text_multi_class(region=None):
        return labeling._built_in_lambda('ACS', _resolve_region(region), 'TextMultiClass')

    @staticmethod
    def built_in_acs_lambda_named_entity_recognition(region=None):
        return labeling._built_in_lambda('ACS', _resolve_region(region), 'NamedEntityRecognition')

    @staticmethod
    def _built_in_lambda(mode, region, task):
        accounts = {
            'us-east-1': '432418664414',
            'us-east-2': '266458841044',
            'us-west-2': '081040173940',
            'ca-central-1': '918755190332',
            'eu-west-1': '568282634449',
            'eu-west-2': '487402164563',
            'eu-central-1': '203001061592',
            'ap-northeast-1': '477331159723',
            'ap-northeast-2': '845288260483',
            'ap-south-1': '565803892007',
            'ap-southeast-1': '377565633583',
            'ap-southeast-2': '454466003867'
        }
        account_id = accounts.get(region)
        if account_id:
            return 'arn:aws:lambda:{}:{}:function:{}-{}'.format(region, account_id, mode.upper(), task)
        else:
            raise Exception('Unrecognized region')

    @staticmethod
    def scale_oversized_images_in_manifest(manifest, bucket=None, key_prefix=None, uri_prefix=None):
        new_manifest = []
        for item in manifest:
            new_item = item.copy()
            img, scalar = scale_image_to_size(uri=new_item['source-ref'])
            if scalar is not None:
                if uri_prefix:
                    (bucket, key_prefix) = s3.split_uri(uri_prefix)
                if key_prefix is None:
                    key_prefix = 'labeling_temp_images/'
                uri = s3.write_temp(img, key_prefix, bucket=bucket).uri
                new_item['original-source-ref'] = new_item.pop('source-ref')
                new_item['source-ref'] = uri
                new_item['scalar'] = scalar
            new_manifest.append(new_item)
        return new_manifest

    @staticmethod
    def reverse_scaling_of_annotation(manifest, label_attribute_name, delete_scaled_images=True):
        new_manifest = []
        for item in manifest:
            new_item = item.copy()
            if 'scalar' in new_item:
                source_image = new_item.pop('old-source-ref')
                scalar = new_item.pop('scalar')
                scaled_image = new_item['source-ref']
                if delete_scaled_images:
                    s3.delete(uri=scaled_image)
                new_item['source-ref'] = source_image
                for annotation in new_item[label_attribute_name]['annotations']:
                    annotation['width'] = int(annotation['width'] / scalar)
                    annotation['height'] = int(annotation['height'] / scalar)
                    annotation['top'] = int(annotation['top'] / scalar)
                    annotation['left'] = int(annotation['left'] / scalar)
            new_manifest.append(new_item)
        return new_manifest
