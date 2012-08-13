# -*- coding: utf-8 -*-
"""setup.py -- setup file for duo module.
"""
import sys

from setuptools import setup

PYVERSION = float('%s.%s' % (sys.version_info[0], sys.version_info[1]))

INSTALL_REQUIRES = [
    'boto>=2.5.2',
    ]

TESTS_REQUIRE = [
    'nose',
    'mock',
    ]

SETUP = dict(
    name = "duo",
    py_modules = ['duo', 'test_duo'],
    install_requires = INSTALL_REQUIRES,
    tests_require = TESTS_REQUIRE,
    test_suite = 'nose.collector',
    
    package_data = {
        '': ['*.txt', '*.html'],
        },
    zip_safe = False,

    version = "0.1.2",
    description = "A powerful, dynamic, pythonic interface to AWS DynamoDB.",
    long_description = open('README.rst').read(),
    author = "David Eyk",
    author_email = "deyk@crossway.org",
    url = "http://www.crossway.org",
    )

if PYVERSION < 2.7:
    INSTALL_REQUIRES.append('importlib')
    TESTS_REQUIRE.append('unittest2==0.5.1')
    SETUP['test_suite'] = 'unittest2.collector'


setup(**SETUP)
