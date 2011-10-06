#!/usr/bin/env python

import sys

setupdict = {
    'name' : 'zkproto',
    'version' : '0.1',
    'description' : 'CEI Zookeeper prototype',
    'license' : 'Apache 2.0',
    'author' : 'CEI',
    'author_email' : 'labisso@uchicago.edu',
    'keywords': ['ooici','cei'],
    'classifiers' : [
    'Development Status :: 3 - Alpha',
    'Environment :: Console',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: Apache Software License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering'],
}

from setuptools import setup, find_packages
setupdict['packages'] = find_packages()

setupdict['dependency_links'] = ['http://ooici.net/releases']

# ssl package won't install on 2.6+, but is required otherwise.
# also, somehow the order matters and ssl needs to be before ioncore
# in this list (at least with setuptools 0.6c11).

setupdict['install_requires'] = ['supervisor', 'zkpython']

setupdict['entry_points'] = {
        'console_scripts': [
            'zkproto-worker=zkproto.worker:main'
            ]
        }

setup(**setupdict)
