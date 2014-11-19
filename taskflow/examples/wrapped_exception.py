# -*- coding: utf-8 -*-

#    Copyright (C) 2012-2013 Yahoo! Inc. All Rights Reserved.
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

import logging
import os
import sys
import time


logging.basicConfig(level=logging.ERROR)

top_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                       os.pardir,
                                       os.pardir))
sys.path.insert(0, top_dir)


import taskflow.engines
from taskflow import exceptions
from taskflow.patterns import unordered_flow as uf
from taskflow import task
from taskflow.tests import utils
from taskflow.types import failure

import example_utils as eu  # noqa

# INTRO: In this example we create two tasks which can trigger exceptions
# based on various inputs to show how to analyze the thrown exceptions for
# which types were thrown and handle the different types in different ways.
#
# This is especially important if a set of tasks run in parallel and each of
# those tasks may fail while running. This creates a scenario where multiple
# exceptions have been thrown and those exceptions need to be handled in a
# unified manner. Since an engine does not currently know how to resolve
# those exceptions (someday it could) the code using that engine and activating
# the flows and tasks using that engine will currently have to deal with
# catching those exceptions (and silencing them if this is desired).
#
# NOTE(harlowja): The engine *will* trigger rollback even under multiple
# exceptions being thrown, but at the end of that rollback the engine will
# rethrow these exceptions to the code that called the run() method; allowing
# that code to do further cleanups (if desired).


class FirstException(Exception):
    """Exception that first task raises."""


class SecondException(Exception):
    """Exception that second task raises."""


class FirstTask(task.Task):
    def execute(self, sleep1, raise1):
        time.sleep(sleep1)
        if not isinstance(raise1, bool):
            raise TypeError('Bad raise1 value: %r' % raise1)
        if raise1:
            raise FirstException('First task failed')


class SecondTask(task.Task):
    def execute(self, sleep2, raise2):
        time.sleep(sleep2)
        if not isinstance(raise2, bool):
            raise TypeError('Bad raise2 value: %r' % raise2)
        if raise2:
            raise SecondException('Second task failed')


def run(**store):
    # Creates a flow, each task in the flow will examine the kwargs passed in
    # here and based on those kwargs it will behave in a different manner
    # while executing; this allows for the calling code (see below) to show
    # different usages of the failure catching and handling mechanism.
    flow = uf.Flow('flow').add(
        FirstTask(),
        SecondTask()
    )
    try:
        with utils.wrap_all_failures():
            taskflow.engines.run(flow, store=store,
                                 engine='parallel')
    except exceptions.WrappedFailure as ex:
        unknown_failures = []
        for a_failure in ex:
            if a_failure.check(FirstException):
                print("Got FirstException: %s" % a_failure.exception_str)
            elif a_failure.check(SecondException):
                print("Got SecondException: %s" % a_failure.exception_str)
            else:
                print("Unknown failure: %s" % a_failure)
                unknown_failures.append(a_failure)
        failure.Failure.reraise_if_any(unknown_failures)


eu.print_wrapped("Raise and catch first exception only")
run(sleep1=0.0, raise1=True,
    sleep2=0.0, raise2=False)

# NOTE(imelnikov): in general, sleeping does not guarantee that we'll have both
# task running before one of them fails, but with current implementation this
# works most of times, which is enough for our purposes here (as an example).
eu.print_wrapped("Raise and catch both exceptions")
run(sleep1=1.0, raise1=True,
    sleep2=1.0, raise2=True)

eu.print_wrapped("Handle one exception, and re-raise another")
try:
    run(sleep1=1.0, raise1=True,
        sleep2=1.0, raise2='boom')
except TypeError as ex:
    print("As expected, TypeError is here: %s" % ex)
else:
    assert False, "TypeError expected"
