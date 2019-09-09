# LarryData
LarryData is a library of utilities for common data tasks using AWS for data science and data engineering projects. 
While boto3 is a great interface for interacting with AWS services, it can be overly complex for data scientists and 
others who want to perform straightforward operations on data. Boto3 is powerful but often requires you spend time
worrying about API-specific interactions and parameters. LarryData makes it easy to use services like S3, MTurk, 
and other data-oriented AWS services in a far more **functional** manner to let you focus on the data rather than 
syntax. This library is designed to make getting tasks completed in Jupyter Notebooks or AWS Lambda functions as 
easy as possible by providing simplified interfaces while still giving you access to the underlying boto3 libraries
when you need them.

## Installation
```
pip install LarryData
```
In addition, you can easily add LarryData to your AWS Lambda functions by adding one of the following public Layers:
* LarryData: `arn:aws:lambda:us-west-2:981332165467:layer:LarryData:18`
* LarryData with Jinja2: `arn:aws:lambda:us-west-2:981332165467:layer:LarryDataWithJinja:18`
* LarryData with Jinja2 and Pillow: `arn:aws:lambda:us-west-2:981332165467:layer:LarryDataWithJinjaPillow:18`


## Configuring your AWS session
By default LarryData creates a boto3 session using your default AWS credentials that can be configured using the 
[AWS CLI](https://aws.amazon.com/cli/). To use a different profile, you can change using the following:
```python
import LarryData as ld
ld.set_session(profile_name='my_profile')
```
Alternatively, you can pass AWS credentials directly
```python
import LarryData as ld
ld.set_session(aws_access_key_id='XXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXX')
```

## Functional S3 interactions
When using boto3 alone, the following is how you would read in a JSON formatted object from S3 into a dict:
```python
import boto3
import json

resource = boto3.resource('s3')
obj = resource.Bucket('mybucket').Object(key='myfile.json').get()
contents = obj['Body'].read()
my_dict = json.loads(contents.decode('utf-8'))
```

In contrast, LarryData takes care of all those steps for you and let's you simply call one function to get your data.
```python
import LarryData as ld

my_dict = ld.s3.read_dict(bucket='mybucket', key='myfile.json')
```
In addition to accessing data using bucket/key pairs, you can S3 URIs like those commonly used in SageMaker.
```python
my_dict2 = ld.s3.read_dict(uri='s3://mybucket/myfile.json')
```

To write files to S3, simply call `write_object` to write your object out in the appropriate format:
```python
# Write json to S3
my_dict = {'key': 'val'}
ld.s3.write_object(my_dict, bucket='mybucket', key='myfile.json')

# Write a list of strings to S3 as rows
my_list = ['a','b','c','d']
ld.s3.write_object(my_list, bucket='mybucket', key='myfile.txt')

# Write a JSON lines file to S3 (commonly used for SageMaker manifest files)
my_dictlist = [{'a': 1}, {'b': 2}, {'c': 3}]
ld.s3.write_object(my_dictlist, bucket='mybucket', key='myfile.jsonl')
```

## Powerful MTurk extensions
LarryData is especially useful for services like MTurk which have more complex interaction patterns and legacy aspects
of their APIs. The MTurk module includes a number of features to make using MTurk much easier:
* Easy toggling between sandbox and production clients (no more copy/pasting in endpoint urls)
* Worker answers are converted into easy to access dict objects (no more QuestionFormAnswer XML!)
* Potentially expensive operations such as list_hits and list_assignments_for_hit return generators
* Utilities are included to easily generate HTMLQuestion and ExternalQuestion XML objects with integrated Jinja2 templating
* Helpers to enable state data in the RequesterAnnotation field

The combination of these features means that creating a HIT in MTurk is as easy as the following:
```python
import LarryData as ld

# Indicate we want to use the production environment
ld.mturk.use_production()

# Load a template from S3 and populate it with values
task_data = {'image_url': 'https://mywebsite.com/images/233.jpg'}
question_xml = ld.mturk.render_jinja_template_question(task_data, template_uri='s3://mybucket/templates/imageCat.html')

# Format the source data so it can be stored in the RequesterAnnotation field for use in tracking
task_data['request_id'] = 'MY_TRACKING_ID'
annotation_payload = ld.mturk.prepare_requester_annotation(task_data)

# Create a HIT
hit = ld.mturk.create_hit(title='Test task', description='Categorize images', reward='0.05', max_assignments=5,
                       lifetime=86400, assignment_duration=600, question=question_xml, annotation=annotation_payload)

# Display where the HIT can be viewed on the Worker website
hit_id = hit['HITId']
hit_type_id = hit['HITTypeId']
print('HIT {} created, preview at {}'.format(hit_id, ld.mturk.preview_url(hit_type_id)))
```
Getting the results from that task is as simple as the following:
```python
import LarryData as ld

hit_id = 'HIT_ID_FROM_EARLIER'

# Indicate we want to use the production environment
ld.mturk.use_production()

# retrieve the HIT
hit = ld.mturk.get_hit(hit_id)

# retrieve the requester annotation data
task_data = ld.mturk.retrieve_requester_annotation(hit=hit)

# get the results
for assignment in ld.mturk.list_assignments_for_hit(hit_id):
    print('Worker {} responded with {}'.format(assignment['WorkerId'], assignment['Answer']['category']))
```

More features will be added over time, feel free to submit your feature suggestions.
