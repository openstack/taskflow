# -*- coding: utf-8 -*-

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
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

import warnings

from taskflow.utils import reflection


def deprecation(message, stacklevel=2):
    """Warns about some type of deprecation that has been made."""
    warnings.warn(message, category=DeprecationWarning, stacklevel=stacklevel)


# Helper accessors for the moved proxy (since it will not have easy access
# to its own getattr and setattr functions).
_setattr = object.__setattr__
_getattr = object.__getattribute__


class MovedClassProxy(object):
    """Acts as a proxy to a class that was moved to another location.

    Partially based on:

    http://code.activestate.com/recipes/496741-object-proxying/ and other
    various examination of how to make a good enough proxy for our usage to
    move the various types we want to move during the deprecation process.

    And partially based on the wrapt object proxy (which we should just use
    when it becomes available @ http://review.openstack.org/#/c/94754/).
    """

    __slots__ = [
        '__wrapped__', '__message__', '__stacklevel__',
        # Ensure weakrefs can be made,
        # https://docs.python.org/2/reference/datamodel.html#slots
        '__weakref__',
    ]

    def __init__(self, wrapped, message, stacklevel):
        # We can't assign to these directly, since we are overriding getattr
        # and setattr and delattr so we have to do this hoop jump to ensure
        # that we don't invoke those methods (and cause infinite recursion).
        _setattr(self, '__wrapped__', wrapped)
        _setattr(self, '__message__', message)
        _setattr(self, '__stacklevel__', stacklevel)
        try:
            _setattr(self, '__qualname__', wrapped.__qualname__)
        except AttributeError:
            pass

    def __instancecheck__(self, instance):
        deprecation(
            _getattr(self, '__message__'), _getattr(self, '__stacklevel__'))
        return isinstance(instance, _getattr(self, '__wrapped__'))

    def __subclasscheck__(self, instance):
        deprecation(
            _getattr(self, '__message__'), _getattr(self, '__stacklevel__'))
        return issubclass(instance, _getattr(self, '__wrapped__'))

    def __call__(self, *args, **kwargs):
        deprecation(
            _getattr(self, '__message__'), _getattr(self, '__stacklevel__'))
        return _getattr(self, '__wrapped__')(*args, **kwargs)

    def __getattribute__(self, name):
        return getattr(_getattr(self, '__wrapped__'), name)

    def __setattr__(self, name, value):
        setattr(_getattr(self, '__wrapped__'), name, value)

    def __delattr__(self, name):
        delattr(_getattr(self, '__wrapped__'), name)

    def __repr__(self):
        wrapped = _getattr(self, '__wrapped__')
        return "<%s at 0x%x for %r at 0x%x>" % (
            type(self).__name__, id(self), wrapped, id(wrapped))


def moved_class(new_class, old_class_name, old_module_name, message=None,
                version=None, removal_version=None):
    """Deprecates a class that was moved to another location.

    This will emit warnings when the old locations class is initialized,
    telling where the new and improved location for the old class now is.
    """
    old_name = ".".join((old_module_name, old_class_name))
    new_name = reflection.get_class_name(new_class)
    message_components = [
        "Class '%s' has moved to '%s'" % (old_name, new_name),
    ]
    if version:
        message_components.append(" in version '%s'" % version)
    if removal_version:
        if removal_version == "?":
            message_components.append(" and will be removed in a future"
                                      " version")
        else:
            message_components.append(" and will be removed in version '%s'"
                                      % removal_version)
    if message:
        message_components.append(": %s" % message)
    return MovedClassProxy(new_class, "".join(message_components), 3)
