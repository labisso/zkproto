#!/usr/bin/env python

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
setupdict['install_requires'] = ['supervisor', 'zkpython', 'gevent']

setupdict['entry_points'] = {
        'console_scripts': [
            'zkproto-worker=zkproto.worker:main',
            'zkproto-trials=zkproto.trials:main'
            ]
        }

setup(**setupdict)
