larry
=================================

**larry** is library of AWS utilities for Python that, quite simply, makes it easier to get data science and
engineering projects done. It allows you to interact with AWS services with the least overhead possible so you
can stay focused on the task at hand, instead of mucking around with AWS APIs.

Using **larry** you can accomplish the same tasks you would normally tackle with boto3 (it's built on boto3), but with a
simpler API that is geared working in Jupyter or IPython. For example, suppose you wanted to write a dict to S3
as a JSON file. This is all you need to do:

.. code-block:: python

    import larry as lry
    lry.s3.write(my_dict, 'mybucket', 'myfile.json')

Similarly, reading this file back is just as easy. We simply make sure we've imported **larry** and then make a
single call to retrieve the S3 object and return it as a *dict*.

.. code-block:: python

    import larry as lry
    my_dict = lry.s3.read_dict('mybucket', 'myfile.json')

Let's compare that to how we would perform the same operation using the AWS boto3 library itself. As you can see below,
we have to go through a number of steps to retrieve the contents of the S3 object and load it into a *dict*
object.

.. code-block:: python

    import boto3
    import json

    resource = boto3.resource('s3')
    obj = resource.Bucket('mybucket').Object(key='myfile.json').get()
    contents = obj['Body'].read()
    my_dict = json.loads(contents.decode('utf-8'))

**larry** let's you spend less time running to StackOverflow for sample code to perform basic operations so you can
focus on the task at hand.

.. toctree::
    :maxdepth: 2

    installation
    configuration
    exceptions
    reference/index.rst

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
