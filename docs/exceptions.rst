Exception Handling
=================================

To catch and handle exceptions you should generally use the following syntax to catch errors of type
``larry.types.ClientError``. This will catch the errors that are commonly raised when accessing AWS services.

.. code-block:: python

    import larry as lry
    from larry.types import ClientError

    try:
        lry.s3.write(my_dict, 'mybucket', 'myfile.json')
    except ClientError as e:
        print("AWS raised error code {} and message {}".format(e.code, e.message))

This type wraps the botocore.exceptions.ClientError raised by boto3 and provides some simplifications to make it
easier to work with. Of note, it truncates the stack trace to only show relevant portions of the stack, rather than
exposing the complexity of the boto3 library. This makes it easier to diagnose problems without scrolling through a
the verbose error stack boto3 typically displays.
