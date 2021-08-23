import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type(u"")
    with io.open(filename, mode="r", encoding='utf-8') as fd:
        return re.sub(text_type(r':[a-z]+:`~?(.*?)`'), text_type(r'``\1``'), fd.read())

DEPENDENCIES = [
'matplotlib>=3.0.3',
'numpy>=1.19.4',
'pandas>=1.0.3',
'pyspark>=3.0.0',
'python-dateutil>=2.8.1',
'python-editor>=1.0.4',
'python-json-logger>=0.1.11'
'requests>=2.23.0',
]

setup(
    name="ssb_spark_tools",
    version="0.1.6",
    url="https://github.com/statisticsnorway/ssb_spark_tools",
    license='MIT',

    author="Statistics Norway",
    author_email="ssv@ssb.no",

    description="A collection of data processing Spark functions for the use in Statistics Norway",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",

    packages=find_packages(exclude=('tests',)),

    install_requires=DEPENDENCIES,

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
