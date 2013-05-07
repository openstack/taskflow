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

from oslo.config import cfg

"""Define APIs for the logbook providers."""

from nova.openstack.common import importutils
from nova.openstack.common import log as logging

LOG = logging.getLogger(__name__)
logprovider_driver_opt = cfg.StrOpt('logbook_provider_driver',
                                    default='memory',
                                    help='The driver for that satisfies the '
                                         'remote lock providing service ( '
                                         'valid options are: db, memory, zk)')

CONF = cfg.CONF
CONF.register_opts([logprovider_driver_opt])


class API(object):

    _driver = None
    _driver_name_class_mapping = {
         # Note these drivers can be used for persistant logging.
         'zk': 'nova.logbook.drivers.zk.ZooKeeperProviderDriver',
         'db': 'nova.logbook.drivers.db.DBProviderDriver',

         # This driver is pretty much only useful for testing.
         'memory': 'nova.logbook.drivers.memory.MemoryProviderDriver',
    }

    def __new__(cls, *args, **kwargs):
        '''Create an instance of the logbook provider API.

        args and kwargs are passed down to the logbook provider driver when it
        gets created.
        '''
        if not cls._driver:
            LOG.debug(_('Logbook provider driver defined as an instance of %s'),
                      str(CONF.logbook_provider_driver))
            driver_name = CONF.logbook_provider_driver
            try:
                driver_class = cls._driver_name_class_mapping[driver_name]
            except KeyError:
                raise TypeError(_("Unknown logbook provider driver name: %s")
                                % driver_name)
            cls._driver = importutils.import_object(driver_class,
                                                    *args, **kwargs)
            utils.check_isinstance(cls._driver, LogBookProviderDriver)
        return super(API, cls).__new__(cls)


class RecordNotFound(Exception):
    pass


class LogBook(object):
    """Base class for what a logbook (distributed or local or in-between)
    should provide"""

    __metaclass__ = abc.ABCMeta

    def __init__(self, resource_uri):
        self.uri = resource_uri

    @abc.abstractmethod
    def add_record(self, name, metadata=None):
        """Atomically adds a new entry to the given logbook with the supplied
        metadata (if any)."""
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_record(self, name):
        """Fetchs a record with the given name and returns any metadata about
        said record."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, name):
        """Determines if any entry with the given name exists in this
        logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def mark(self, name, metadata, merge_functor=None):
        """Marks the given logbook entry (which must exist) with the given
        metadata, if said entry already exists then the provided merge functor
        or a default function, will be activated to merge the existing metadata
        with the supplied metadata."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all names and metadata and provides back both of these
        via a (name, metadata) tuple. The order will be in the same order that
        they were added."""
        raise NotImplementedError()


class LogBookProvider(object):
    """Base class for logbook provider drivers."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def provide(self, resource_uri):
        """Returns a new (if not existent) logbook object or used logbook
        object (if already existent) which can be used to determine the actions
        performed on a given resource uri"""
        raise NotImplementedError()

    def close(self):
        """Allows the log provider to free any resources that it has."""
        pass
