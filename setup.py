# -*- coding: utf-8 -*-
"""setup.py -- setup file for duo module.
"""
from setuptools import setup

setup(
    name = "crossway-duo",
    py_modules = ['duo'],
    install_requires = [
        'boto>=2.5.2',
        ],

    package_data = {
        '': ['*.txt', '*.html'],
        },
    zip_safe = False,

    version = "0.1",
    description = "A powerful, dynamic, pythonic interface to AWS DynamoDB.",
    author = "David Eyk",
    author_email = "deyk@crossway.org",
    url = "http://www.crossway.org",
    long_description = """\
Duo: A powerful, dynamic, pythonic interface to AWS DynamoDB
------------------------------------------------------------

...
"""
    )
