import setuptools
import os
from pkg_resources import parse_version

mode = os.environ.get("LARRY_RELEASE_MODE", "pre")
print(mode)

pre_version = parse_version(os.listdir("pre")[0].split("-")[1])
rel_version = parse_version(os.listdir("rel")[0].split("-")[1])

if mode == "pre":
    if pre_version > rel_version:
        p = pre_version.pre
        version = pre_version.base_version + p[0] + str(p[1]+1)
    else:
        version = f"{rel_version.major}.{rel_version.minor}.{rel_version.micro+1}a0"
else:
    version = f"{rel_version.major}.{rel_version.minor}.{rel_version.micro+1}"

with open("larry/__init__.py") as fp:
    text = fp.read()
with open("larry/__init__.py", "w") as fp:
    fp.write(text.replace("{VERSION}", version))

with open("README.md", "r") as fh:
    long_description = fh.read()
setuptools.setup(
    name="larry",
    version=version,
    author="Dave Schultz",
    author_email="djschult@gmail.com",
    description="Library of helper reference for common data tasks using AWS resources such as S3, MTurk and others",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dschultz0/larry",
    packages=setuptools.find_packages(exclude=['test']),
    include_package_data=False,
    keywords="larry data aws boto3 mturk s3",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
