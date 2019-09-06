# larrydata
Larrydata is a library of utilities for common data tasks using AWS for data science and data engineering projects. 
While boto3 is a great interface for interacting with AWS services, it can be overly complex for data scientists and 
others who want to perform straightforward operations on data. Boto3 is powerful but often requires you spend time
worrying about API-specific interactions and parameters. Larrydata makes it easy to use services like S3, MTurk, 
and other data-oriented AWS services in a far more **functional** manner to let you focus on the data rather than 
syntax. This library is designed to make getting tasks completed in Jupyter Notebooks or AWS Lambda functions as 
easy as possible by providing simplified interfaces while still giving you access to the underlying boto3 libraries
when you need them.

## Installation
```
pip install larrydata
```
In addition, you can easily add larrydata to your AWS Lambda functions by adding one of the following public Layers:
* larrydata: `arn:aws:lambda:us-west-2:981332165467:layer:LarryData:16`
* larrydata with Jinja2: `arn:aws:lambda:us-west-2:981332165467:layer:LarryDataWithJinja:16`
* larrydata with Jinja2 and Pillow: `arn:aws:lambda:us-west-2:981332165467:layer:LarryDataWithJinjaPillow:16`

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

In contrast, larrydata takes care of all those steps for you and let's you simply call one function to get your data.
In addition to accessing data using bucket/key pairs, you can S3 URIs like those commonly used in SageMaker.
```python
from larrydata import s3

my_dict = s3.read_dict(bucket='mybucket', key='myfile.json')

# Alternatively, you can use S3 URIs to access your data
my_dict2 = s3.read_dict(uri='s3://mybucket/myfile.json')
```

To write files to S3, simply call `write_object` to write your object out in the appropriate format:
```python
from larrydata import s3

# Write json to S3
my_dict = {'key': 'val'}
s3.write_object(my_dict, bucket='mybucket', key='myfile.json')

# Write a list of strings to S3 as rows
my_list = ['a','b','c','d']
s3.write_object(my_list, bucket='mybucket', key='myfile.txt')

# Write a JSON lines file to S3 (commonly used for SageMaker manifest files)
my_dictlist = [{'a': 1}, {'b': 2}, {'c': 3}]
s3.write_object(my_dictlist, bucket='mybucket', key='myfile.jsonl')
```

## Powerful MTurk extensions
Larrydata is especially useful for services like MTurk which have more complex interaction patterns and legacy aspects
of their APIs. The MTurk module includes a number of features to make using MTurk much easier:
* Easy toggling between sandbox and production clients (no more copy/pasting in endpoint urls)
* Worker answers are converted into easy to access dict objects (no more QuestionFormAnswer XML!)
* Potentially expensive operations such as list_hits and list_assignments_for_hit return generators
* Utilities are included to easily generate HTMLQuestion and ExternalQuestion XML objects with integrated Jinja2 templating
* Helpers to enable state data in the RequesterAnnotation field

The combination of these features means that creating a HIT in MTurk is as easy as the following:
```python
import larrydata.mturk as mturk

# Indicate we want to use the production environment
mturk.use_production()

# Load a template from S3 and populate it with values
task_data = {'image_url': 'https://mywebsite.com/images/233.jpg'}
question_xml = mturk.render_jinja_template_question(task_data, template_uri='s3://mybucket/templates/imageCat.html')

# Format the source data so it can be stored in the RequesterAnnotation field for use in tracking
task_data['request_id'] = 'MY_TRACKING_ID'
annotation_payload = mturk.prepare_requester_annotation(task_data)

# Create a HIT
hit = mturk.create_hit(title='Test task', description='Categorize images', reward='0.05', max_assignments=5,
                       lifetime=86400, assignment_duration=600, question=question_xml, annotation=annotation_payload)

# Display where the HIT can be viewed on the Worker website
hit_id = hit['HITId']
hit_type_id = hit['HITTypeId']
print('HIT {} created, preview at {}'.format(hit_id, mturk.preview_url(hit_type_id)))
```
Getting the results from that task is as simple as the following:
```python
import larrydata.mturk as mturk

hit_id = 'HIT_ID_FROM_EARLIER'

# Indicate we want to use the production environment
mturk.use_production()

# retrieve the HIT
hit = mturk.get_hit(hit_id)

# retrieve the requester annotation data
task_data = mturk.retrieve_requester_annotation(hit=hit)

# get the results
for assignment in mturk.list_assignments_for_hit(hit_id):
    print('Worker {} responded with {}'.format(assignment['WorkerId'], assignment['Answer']['category']))
```

More features will be added over time, feel free to submit your feature suggestions.
