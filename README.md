# larrydata
Larrydata is a library of utilities for common data tasks using AWS for data science and data engineering projects. 
While boto3 is a great interface for interacting with AWS services, it can be overly complex for data scientists and 
others who want to perform simple operations on data without worrying about API-specific interactions and parameters. 
Larrydata makes it easy to use services like S3, MTurk, and other data-oriented AWS services in a far more functional
manner to let you focus on the data rather than syntax. This library is designed to make getting tasks completed
in Jupyter Notebooks or AWS Lambda functions as easy as possible.

For example, using boto3 the following is how you would read in a JSON formatted object from S3 into a dict:
```python
import boto3
import json

resource = boto3.resource('s3')
obj = resource.Bucket('mybucket').Object(key='myfile.json').get()
contents = obj['Body'].read()
my_dict = json.loads(contents.decode('utf-8'))
```

In contrast, larrydata takes care of all those steps for you and let's you simply call one function to get your data.
In addition to accessing data using bucket/key pairs, you can S3 URIs like those commonly used in SageMaker Ground Truth.
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

# Write a JSON lines file to S3
my_dictlist = [{'a': 1}, {'b': 2}, {'c': 3}]
s3.write_object(my_dictlist, bucket='mybucket', key='myfile.jsonl')
```

Larrydata is especially useful for services like MTurk which have more complex interaction patterns and legacy aspects
of their APIs. The MTurk module includes a number of features to make using MTurk much easier:
* Easy toggling between sandbox and production clients (no more copy/pasting in endpoint urls)
* Worker answers are converted into easy to access dict objects (no more QuestionFormAnswer XML!)
* Potentially expensive operations such as list_hits and list_assignments_for_hit return generators
* Utilities are included to easily generate HTMLQuestion and ExternalQuestion XML objects with integrated Jinja2 templating
* Helpers to enable state data in the RequesterAnnotation field

More features will be added over time, feel free to submit your feature suggestions.
