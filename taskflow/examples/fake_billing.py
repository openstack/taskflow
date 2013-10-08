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

import json
import logging
import os
import sys
import time

logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)


from taskflow.openstack.common import uuidutils

from taskflow import engines
from taskflow.listeners import printing
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow.utils import misc


class AttrDict(object):
    def __init__(self, **kwargs):
        self._attrs = {}
        for (k, v) in kwargs.iteritems():
            if ' ' in k or k in ('self',) or not len(k):
                raise AttributeError("Invalid attribute name")
            self._attrs[k] = v

    def __getattr__(self, name):
        try:
            return self._attrs[name]
        except KeyError:
            raise AttributeError("No attributed named '%s'" % (name))


class DB(object):
    def query(self, sql):
        print("Querying with: %s" % (sql))


class UrlCaller(object):
    def __init__(self):
        self._send_time = 0.5

    def send(self, url, data, status_cb=None):
        sleep_time = float(self._send_time) / 25
        for i in xrange(0, len(data)):
            time.sleep(sleep_time)
            if status_cb:
                status_cb(float(i) / len(data))


class ResourceFetcher(object):
    def __init__(self):
        self._db_handle = None
        self._url_handle = None

    @property
    def db_handle(self):
        if self._db_handle is None:
            self._db_handle = DB()
        return self._db_handle

    @property
    def url_handle(self):
        if self._url_handle is None:
            self._url_handle = UrlCaller()
        return self._url_handle


class ExtractInputRequest(task.Task):
    def __init__(self, resources):
        super(ExtractInputRequest, self).__init__(provides="parsed_request")
        self._resources = resources

    def execute(self, request):
        return {
            'user': request.user,
            'user_id': misc.as_int(request.id),
            'request_id': uuidutils.generate_uuid(),
        }


class MakeDBEntry(task.Task):
    def __init__(self, resources):
        super(MakeDBEntry, self).__init__()
        self._resources = resources

    def execute(self, parsed_request):
        db_handle = self._resources.db_handle
        db_handle.query("INSERT %s INTO mydb" % (parsed_request))

    def revert(self, result, parsed_request):
        db_handle = self._resources.db_handle
        db_handle.query("DELETE %s FROM mydb IF EXISTS" % (parsed_request))


class ActivateDriver(task.Task):
    def __init__(self, resources):
        super(ActivateDriver, self).__init__(provides='sent_to')
        self._resources = resources
        self._url = "http://blahblah.com"

    def execute(self, parsed_request):
        print("Sending billing data to %s" % (self._url))
        url_sender = self._resources.url_handle
        url_sender.send(self._url, json.dumps(parsed_request),
                        status_cb=self.update_progress)
        return self._url

    def update_progress(self, progress, **kwargs):
        super(ActivateDriver, self).update_progress(progress, **kwargs)
        print("%s is %0.2f%% done" % (self.name, progress * 100))


class DeclareSuccess(task.Task):
    def execute(self, sent_to):
        print("Done!")
        print("All data processed and sent to %s" % (sent_to))


SERIAL = False
if SERIAL:
    engine_conf = {
        'engine': 'serial',
    }
else:
    engine_conf = {
        'engine': 'parallel',
    }


# Resources (db handles and similar) of course can't be persisted so we need
# to make sure that we pass this resource fetcher to the tasks constructor so
# that the tasks have access to any needed resources (lazily loaded).
resources = ResourceFetcher()
flow = lf.Flow("initialize-me")

# 1. First we extract the api request into a useable format.
# 2. Then we go ahead and make a database entry for our request.
flow.add(ExtractInputRequest(resources),
         MakeDBEntry(resources))

# 3. Then we activate our payment method and finally declare success
sub_flow = gf.Flow("after-initialize")
sub_flow.add(ActivateDriver(resources), DeclareSuccess())
flow.add(sub_flow)

store = {
    'request': AttrDict(user="bob", id="1.35"),
}
eng = engines.load(flow, engine_conf=engine_conf, store=store)
with printing.PrintingListener(eng):
    eng.run()
