# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2012 Yahoo! Inc. All Rights Reserved.
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
import contextlib

from oslo.config import cfg

from nova.openstack.common import importutils
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)
jobboard_driver_opt = cfg.StrOpt('job_board_driver',
                                 default='memory',
                                 help='The driver that satisfies the '
                                      'job providing service ( '
                                      'valid options are: db, mq, zk, memory)')

CONF = cfg.CONF
CONF.register_opts([jobboard_driver_opt])


class API(object):

    _driver = None
    _driver_name_class_mapping = {
        # TBD
    }

    def __new__(cls, *args, **kwargs):
        '''Create an instance of the lock provider API.

        args and kwargs are passed down to the job board driver when it gets
        created.
        '''
        if not cls._driver:
            LOG.debug(_('Job board driver defined as an instance of %s'),
                      str(CONF.job_board_driver))
            driver_name = CONF.job_board_driver
            try:
                driver_class = cls._driver_name_class_mapping[driver_name]
            except KeyError:
                raise TypeError(_("Unknown job board driver name: %s")
                                % driver_name)
            cls._driver = importutils.import_object(driver_class,
                                                    *args, **kwargs)
            utils.check_isinstance(cls._driver, JobBoardDriver)
        return super(API, cls).__new__(cls)


class JobBoardDriver(object):
    """Base class for job board drivers."""

    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self._listeners = []

    @abc.abstractmethod
    def post(self, job):
        raise NotImplementedError()

    def _notify_posted(self, job):
        for i in self._listeners:
            i.notify_posted(job)

    @abc.abstractmethod
    def await(self, blocking=True):
        raise NotImplementedError()

    def subscribe(self, listener):
        self._listeners.append(listener)

    def unsubscribe(self, listener):
        if listener in self._listeners:
            self._listeners.remove(listener)

    def close(self):
        """Allows the job board provider to free any resources that it has."""
        pass
