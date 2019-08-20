# larrydata
Library of utilities for common data tasks using AWS. While boto3 is a great interface for interacting with
AWS services, it can be overly complex for data scientists and others who want to perform simple operations 
on data without worrying about API-specific interactions and parameters. Larrydata provides a simple wrapper for S3,
MTurk, and other data-oriented services to let you focus on the data rather than syntax.

For example, the following is all it takes to read a JSON formatted object from S3 into a dict:
```python
from larrydata import s3

my_dict = s3.read_dict(bucket='mybucket', key='myfile.json')

# Alternatively, you can use S3 URIs to access your data
my_dict2 = s3.read_dict(uri='s3://mybucket/myfile.json')
```

To write files to S3, simply calling `write_object` will write your object out in the appropriate format:
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
