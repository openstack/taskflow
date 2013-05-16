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
import weakref

from datetime import datetime


class Page(object):
    """A logbook page has the bare minimum of these fields."""

    def __init__(self, name, metadata=None):
        self.date_created = datetime.utcnow()
        self.name = name
        self.metadata = metadata

    def __str__(self):
        return "Page (%s, %s): %s" % (self.name, self.date_created,
                                      self.metadata)


class Chapter(object):
    """Base class for what a chapter of a logbook should provide."""

    __metaclass__ = abc.ABCMeta

    def __init__(self, book, name):
        self.book = weakref.proxy(book)
        self.name = name

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all pages in the given chapter.

        The order will be in the same order that they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, page_name):
        """Determines if any page with the given name exists in this
        chapter."""
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_pages(self, page_name):
        """Fetch any pages that match the given page name."""
        raise NotImplementedError()

    @abc.abstractmethod
    def add_page(self, page):
        """Adds a page to the underlying chapter."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many pages the underlying chapter has."""
        raise NotImplementedError()

    def __str__(self):
        return "Chapter (%s): %s pages" % (self.name, len(self))


class LogBook(object):
    """Base class for what a logbook should provide"""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def add_chapter(self, chapter_name):
        """Atomically adds a new chapter to the given logbook
        or raises an exception if that chapter (or a chapter with
        that name) already exists.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def fetch_chapter(self, chapter_name):
        """Fetches the given chapter or raises an exception if that chapter
        does not exist."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __contains__(self, chapter_name):
        """Determines if a chapter with the given name exists in this
        logbook."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __iter__(self):
        """Iterates over all the chapters.

        The order will be in the same order that they were added."""
        raise NotImplementedError()

    @abc.abstractmethod
    def __len__(self):
        """Returns how many pages the underlying chapter has."""
        raise NotImplementedError()

    def close(self):
        """Allows the logbook to free any resources that it has."""
        pass
