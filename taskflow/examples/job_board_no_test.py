# -*- encoding: utf-8 -*-
#
# Copyright © 2013 eNovance <licensing@enovance.com>
#
# Authors: Dan Krause <dan@dankrause.net>
#          Cyril Roelandt <cyril.roelandt@enovance.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This example shows how to use the job board feature.
#
# Let's start by creating some jobs:
# $ python job_board_no_test.py create my-board my-job '{}'
# $ python job_board_no_test.py create my-board my-job '{"foo": "bar"}'
# $ python job_board_no_test.py create my-board my-job '{"foo": "baz"}'
# $ python job_board_no_test.py create my-board my-job '{"foo": "barbaz"}'
#
# Make sure they were registered:
# $ python job_board_no_test.py list my-board
# 7277181a-1f83-473d-8233-f361615bae9e - {}
# 84a396e8-d02e-450d-8566-d93cb68550c0 - {u'foo': u'bar'}
# 4d355d6a-2c72-44a2-a558-19ae52e8ae2c - {u'foo': u'baz'}
# cd9aae2c-fd64-416d-8ba0-426fa8e3d59c - {u'foo': u'barbaz'}
#
# Perform one job:
# $ python job_board_no_test.py consume my-board \
#       84a396e8-d02e-450d-8566-d93cb68550c0
# Performing job 84a396e8-d02e-450d-8566-d93cb68550c0 with args \
#     {u'foo': u'bar'}
# $ python job_board_no_test.py list my-board
# 7277181a-1f83-473d-8233-f361615bae9e - {}
# 4d355d6a-2c72-44a2-a558-19ae52e8ae2c - {u'foo': u'baz'}
# cd9aae2c-fd64-416d-8ba0-426fa8e3d59c - {u'foo': u'barbaz'}
#
# Delete a job:
# $ python job_board_no_test.py delete my-board \
#       cd9aae2c-fd64-416d-8ba0-426fa8e3d59c
# $ python job_board_no_test.py list my-board
# 7277181a-1f83-473d-8233-f361615bae9e - {}
# 4d355d6a-2c72-44a2-a558-19ae52e8ae2c - {u'foo': u'baz'}
#
# Delete all the remaining jobs
# $ python job_board_no_test.py clear my-board
# $ python job_board_no_test.py list my-board
# $

import argparse
import contextlib
import json
import os
import sys
import tempfile

import taskflow.jobs.backends as job_backends
from taskflow.persistence import logbook

import example_utils  # noqa


@contextlib.contextmanager
def jobboard(*args, **kwargs):
    jb = job_backends.fetch(*args, **kwargs)
    jb.connect()
    yield jb
    jb.close()


conf = {
    'board': 'zookeeper',
    'hosts': ['127.0.0.1:2181']
}


def consume_job(args):
    def perform_job(job):
        print("Performing job %s with args %s" % (job.uuid, job.details))

    with jobboard(args.board_name, conf) as jb:
        for job in jb.iterjobs(ensure_fresh=True):
            if job.uuid == args.job_uuid:
                jb.claim(job, "test-client")
                perform_job(job)
                jb.consume(job, "test-client")


def clear_jobs(args):
    with jobboard(args.board_name, conf) as jb:
        for job in jb.iterjobs(ensure_fresh=True):
            jb.claim(job, "test-client")
            jb.consume(job, "test-client")


def create_job(args):
    store = json.loads(args.details)
    book = logbook.LogBook(args.job_name)
    if example_utils.SQLALCHEMY_AVAILABLE:
        persist_path = os.path.join(tempfile.gettempdir(), "persisting.db")
        backend_uri = "sqlite:///%s" % (persist_path)
    else:
        persist_path = os.path.join(tempfile.gettempdir(), "persisting")
        backend_uri = "file:///%s" % (persist_path)
    with example_utils.get_backend(backend_uri) as backend:
        backend.get_connection().save_logbook(book)
        with jobboard(args.board_name, conf, persistence=backend) as jb:
            jb.post(args.job_name, book, details=store)


def list_jobs(args):
    with jobboard(args.board_name, conf) as jb:
        for job in jb.iterjobs(ensure_fresh=True):
            print("%s - %s" % (job.uuid, job.details))


def delete_job(args):
    with jobboard(args.board_name, conf) as jb:
        for job in jb.iterjobs(ensure_fresh=True):
            if job.uuid == args.job_uuid:
                jb.claim(job, "test-client")
                jb.consume(job, "test-client")


def main(argv):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                       description='valid subcommands',
                                       help='additional help')

    # Consume command
    parser_consume = subparsers.add_parser('consume')
    parser_consume.add_argument('board_name')
    parser_consume.add_argument('job_uuid')
    parser_consume.set_defaults(func=consume_job)

    # Clear command
    parser_consume = subparsers.add_parser('clear')
    parser_consume.add_argument('board_name')
    parser_consume.set_defaults(func=clear_jobs)

    # Create command
    parser_create = subparsers.add_parser('create')
    parser_create.add_argument('board_name')
    parser_create.add_argument('job_name')
    parser_create.add_argument('details')
    parser_create.set_defaults(func=create_job)

    # Delete command
    parser_delete = subparsers.add_parser('delete')
    parser_delete.add_argument('board_name')
    parser_delete.add_argument('job_uuid')
    parser_delete.set_defaults(func=delete_job)

    # List command
    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('board_name')
    parser_list.set_defaults(func=list_jobs)

    args = parser.parse_args(argv)
    args.func(args)

if __name__ == '__main__':
    main(sys.argv[1:])
