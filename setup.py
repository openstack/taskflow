#!/usr/bin/env python

import os
import setuptools


def read_requires(base):
    path = os.path.join('tools', base)
    requires = []
    if not os.path.isfile(path):
        return requires
    with open(path, 'rb') as h:
        for line in h.read.splitlines():
            line = line.strip()
            if len(line) == 0 or line.startswith("#"):
                continue
            requires.append(line)
    return requires


setuptools.setup(name='taskflow',
    version='0.0.1',
    author='OpenStack',
    license='Apache Software License',
    description='Taskflow state management library.',
    long_description='The taskflow library provides core functionality that '
                     'can be used to build [resumable, reliable, '
                     'easily understandable, ...] highly available '
                     'systems which process workflows in a structured manner.',
    author_email='openstack-dev@lists.openstack.org',
    url='http://www.openstack.org/',
    tests_require=read_requires('test-requires'),
    install_requires=read_requires('pip-requires'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6', ],
)