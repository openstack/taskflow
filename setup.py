#!/usr/bin/env python

import os
import setuptools


def _clean_line(line):
    line = line.strip()
    line = line.split("#")[0]
    line = line.strip()
    return line


def read_requires(base):
    path = os.path.join('tools', base)
    requires = []
    if not os.path.isfile(path):
        return requires
    with open(path, 'rb') as h:
        for line in h.read().splitlines():
            line = _clean_line(line)
            if not line:
                continue
            requires.append(line)
    return requires


setuptools.setup(
    name='taskflow',
    version='0.0.1',
    author='OpenStack',
    license='Apache Software License',
    description='Taskflow structured state management library.',
    long_description='The taskflow library provides core functionality that '
                     'can be used to build [resumable, reliable, '
                     'easily understandable, ...] highly available '
                     'systems which process workflows in a structured manner.',
    author_email='openstack-dev@lists.openstack.org',
    url='http://www.openstack.org/',
    packages=setuptools.find_packages(),
    tests_require=read_requires('test-requires'),
    install_requires=read_requires('pip-requires'),
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6', ],
)
