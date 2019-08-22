import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="larrydata",
    version="0.1.0",
    author="Dave Schultz",
    author_email="djschult@gmail.com",
    description="Library of helper modules for common data tasks using AWS resources such as S3, SQS, MTurk and others",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dschultz0/larrydata",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
