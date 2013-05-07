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
from nova import utils


LOG = logging.getLogger(__name__)
lockprovider_driver_opt = cfg.StrOpt('lock_provider_driver',
                                     default='memory',
                                     help='The driver that satisfies the '
                                          'lock providing service ('
                                          'valid options are: db, file, '
                                          'memory, zk)')

CONF = cfg.CONF
CONF.register_opts([lockprovider_driver_opt])


class API(object):

    _driver = None
    _driver_name_class_mapping = {
        # This can attempt to provide a distributed lock but extreme! care
        # has to be taken to know when to expire a database lock, as well as
        # extreme! care has to be taken when acquiring said lock. It is likely
        # not possible to guarantee locking consistently and correctness when
        # using a database to do locking.
        'db': 'nova.locks.drivers.db.LockProvider',
        # This can provide per system level locks but can not provide across
        # system level locks.
        'file': 'nova.locks.drivers.file.LockProvider',
        # Note this driver can be used for distributed locking of resources.
        'zk': 'nova.locks.drivers.zk.LockProvider',
        # This driver is pretty much only useful for testing since it can
        # only provide per-process locking using greenlet/thread level locking.
        'memory': 'nova.locks.drivers.memory.LockProvider',
    }

    def __new__(cls, *args, **kwargs):
        '''Create an instance of the lock provider API.

        args and kwargs are passed down to the lock provider driver when it
        gets created (if applicable).
        '''
        if not cls._driver:
            LOG.debug(_('Lock provider driver defined as an instance of %s'),
                      str(CONF.lock_provider_driver))
            driver_name = CONF.lock_provider_driver
            try:
                driver_class = cls._driver_name_class_mapping[driver_name]
            except KeyError:
                raise TypeError(_("Unknown lock provider driver name: %s")
                                % driver_name)
            cls._driver = importutils.import_object(driver_class,
                                                    *args, **kwargs)
            utils.check_isinstance(cls._driver, LockProvider)
        return super(API, cls).__new__(cls)


class Lock(object):
    """Base class for what a lock (distributed or local or in-between) should
    provide"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, resource_uri, blocking=True):
        self.uri = resource_uri
        self.blocking = blocking

    @abc.abstractmethod
    def acquire(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def release(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_locked(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def cancel(self):
        raise NotImplementedError()

    def __enter__(self):
        self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


class LockProvider(object):
    """Base class for lock provider drivers."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def provide(self, resource_uri, blocking=True):
        """Returns a single lock object which can be used to acquire the lock
        on the given resource uri"""
        raise NotImplementedError()

    def close(self):
        """Allows the lock provider to free any resources that it has."""
        pass
