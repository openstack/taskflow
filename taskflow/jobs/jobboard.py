# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2013 Rackspace Hosting Inc. All Rights Reserved.
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


class JobBoard(six.with_metaclass(abc.ABCMeta)):
    """A jobboard is an abstract representation of a place where jobs
    can be posted, reposted, claimed and transferred. There can be multiple
    implementations of this job board, depending on the desired semantics and
    capabilities of the underlying jobboard implementation.
    """

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        """The non-uniquely identifying name of this jobboard"""
        return self._name

    @abc.abstractmethod
    def consume(self, job):
        """Permanently (and atomically) removes a job from the jobboard,
        signaling that this job has been completed by the entity assigned
        to that job.

        Only the entity that has claimed that job is able to consume a job.

        A job that has been consumed can not be reclaimed or reposted by
        another entity (job postings are immutable). Any entity consuming
        a unclaimed job (or a job they do not own) will cause an exception.
        """

    @abc.abstractmethod
    def post(self, job):
        """Atomically posts a given job to the jobboard, allowing others to
        attempt to claim that job (and subsequently work on that job).

        Once a job has been posted it can only be removed by consuming that
        job (after that job is claimed). Any entity can post or propose jobs
        to the jobboard (in the future this may be restricted).
        """

    @abc.abstractmethod
    def claim(self, job, who):
        """Atomically attempts to claim the given job for the entity and either
        succeeds or fails at claiming by throwing corresponding exceptions.

        If a job is claimed it is expected that the entity that claims that job
        will at sometime in the future work on that jobs flows and either fail
        at completing them (resulting in a reposting) or consume that job from
        the jobboard (signaling its completion).
        """

    @abc.abstractmethod
    def repost(self, job):
        """Atomically reposts the given job on the jobboard, allowing that job
        to be reclaimed by others. This would typically occur if the entity
        that has claimed the job has failed or is unable to complete the job
        or jobs it has claimed.

        Only the entity that has claimed that job can repost a job. Any entity
        reposting a unclaimed job (or a job they do not own) will cause an
        exception.
        """
