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

from oslo_utils import uuidutils

from taskflow import engines
from taskflow.listeners import printing
from taskflow.patterns import graph_flow as gf
from taskflow.patterns import linear_flow as lf
from taskflow import task
from taskflow.utils import misc

# INTRO: This example walks through a miniature workflow which simulates
# the reception of an API request, creation of a database entry, driver
# activation (which invokes a 'fake' webservice) and final completion.
#
# This example also shows how a function/object (in this class the url sending)
# that occurs during driver activation can update the progress of a task
# without being aware of the internals of how to do this by associating a
# callback that the url sending can update as the sending progresses from 0.0%
# complete to 100% complete.


class DB(object):
    def query(self, sql):
        print("Querying with: %s" % (sql))


class UrlCaller(object):
    def __init__(self):
        self._send_time = 0.5
        self._chunks = 25

    def send(self, url, data, status_cb=None):
        sleep_time = float(self._send_time) / self._chunks
        for i in range(0, len(data)):
            time.sleep(sleep_time)
            # As we send the data, each chunk we 'fake' send will progress
            # the sending progress that much further to 100%.
            if status_cb:
                status_cb(float(i) / len(data))


# Since engines save the output of tasks to a optional persistent storage
# backend resources have to be dealt with in a slightly different manner since
# resources are transient and can *not* be persisted (or serialized). For tasks
# that require access to a set of resources it is a common pattern to provide
# a object (in this case this object) on construction of those tasks via the
# task constructor.
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
        # Note that here we attach our update_progress function (which is a
        # function that the engine also 'binds' to) to the progress function
        # that the url sending helper class uses. This allows the task progress
        # to be tied to the url sending progress, which is very useful for
        # downstream systems to be aware of what a task is doing at any time.
        url_sender.send(self._url, json.dumps(parsed_request),
                        status_cb=self.update_progress)
        return self._url

    def update_progress(self, progress, **kwargs):
        # Override the parent method to also print out the status.
        super(ActivateDriver, self).update_progress(progress, **kwargs)
        print("%s is %0.2f%% done" % (self.name, progress * 100))


class DeclareSuccess(task.Task):
    def execute(self, sent_to):
        print("Done!")
        print("All data processed and sent to %s" % (sent_to))


class DummyUser(object):
    def __init__(self, user, id_):
        self.user = user
        self.id = id_


# Resources (db handles and similar) of course can *not* be persisted so we
# need to make sure that we pass this resource fetcher to the tasks constructor
# so that the tasks have access to any needed resources (the resources are
# lazily loaded so that they are only created when they are used).
resources = ResourceFetcher()
flow = lf.Flow("initialize-me")

# 1. First we extract the api request into a usable format.
# 2. Then we go ahead and make a database entry for our request.
flow.add(ExtractInputRequest(resources), MakeDBEntry(resources))

# 3. Then we activate our payment method and finally declare success.
sub_flow = gf.Flow("after-initialize")
sub_flow.add(ActivateDriver(resources), DeclareSuccess())
flow.add(sub_flow)

# Initially populate the storage with the following request object,
# prepopulating this allows the tasks that dependent on the 'request' variable
# to start processing (in this case this is the ExtractInputRequest task).
store = {
    'request': DummyUser(user="bob", id_="1.35"),
}
eng = engines.load(flow, engine='serial', store=store)

# This context manager automatically adds (and automatically removes) a
# helpful set of state transition notification printing helper utilities
# that show you exactly what transitions the engine is going through
# while running the various billing related tasks.
with printing.PrintingListener(eng):
    eng.run()
