# -*- coding: utf-8 -*-

#    Copyright (C) 2013 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import pkg_resources

TASK_VENDOR = "OpenStack Foundation"
TASK_PRODUCT = "OpenStack TaskFlow"
TASK_PACKAGE = None  # OS distro package version suffix

try:
    from pbr import version as pbr_version
    _version_info = pbr_version.VersionInfo('taskflow')
    version_string = _version_info.version_string
except ImportError:
    _version_info = pkg_resources.get_distribution('taskflow')
    version_string = lambda: _version_info.version


def version_string_with_package():
    if TASK_PACKAGE is None:
        return version_string()
    else:
        return "%s-%s" % (version_string(), TASK_PACKAGE)
