# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

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


import abc

import six


class EngineBase(six.with_metaclass(abc.ABCMeta)):
    """Base for all engines implementations"""

    def __init__(self, flow, flow_detail, backend, conf):
        self._flow = flow
        self._flow_detail = flow_detail
        self.storage = self._storage_cls(flow_detail, backend)

    @abc.abstractproperty
    def _storage_cls(self):
        """Storage class"""

    @abc.abstractmethod
    def compile(self):
        """Check the flow and convert it to internal representation"""

    @abc.abstractmethod
    def run(self):
        """Run the flow"""
