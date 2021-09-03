Installation
=================================
To begin using *larry* start by installing it using pip:

.. code-block:: shell

    pip install larry

This will install the larry package as well as the boto3 package if you don't have installed already. If they're
installed, larry can also make use of additional packages such as Pillow and Numpy which you may wish to
install and/or upgrade.

Boto3 and by extension, larry, work best when you've setup your local AWS configuration with the necessary
credentials to access your account. On most systems this involves setting your credentials in your ``~/.aws`` folder.
It's easiest to create this configuration by installing the `AWS CLI
<https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html>`_ and then running the ``aws configure`` command.
This will prompt you for your credentials and can be used to create one or more profiles to use when connecting to
different AWS accounts.

If you prefer not to define credentials on your machine, you can specify them in your Python code like this:

.. code-block:: python

    import larry as lry
    lry.set_session(aws_access_key_id='<my access key>',
                    aws_secret_access_key='<my secret key>')

Similarly, you can select specific connection profiles that you've defined in your credentials
by running the following:

.. code-block:: python

    import larry as lry
    lry.set_session(profile='<my profile>')

