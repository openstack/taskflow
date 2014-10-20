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

import wrapt

from taskflow.utils import reflection


def deprecation(message, stacklevel=2):
    """Warns about some type of deprecation that has been made."""
    warnings.warn(message, category=DeprecationWarning, stacklevel=stacklevel)


class MovedClassProxy(wrapt.ObjectProxy):
    """Acts as a proxy to a class that was moved to another location."""

    def __init__(self, wrapped, message, stacklevel):
        super(MovedClassProxy, self).__init__(wrapped)
        self._self_message = message
        self._self_stacklevel = stacklevel

    def __call__(self, *args, **kwargs):
        deprecation(self._self_message, stacklevel=self._self_stacklevel)
        return self.__wrapped__(*args, **kwargs)

    def __instancecheck__(self, instance):
        deprecation(self._self_message, stacklevel=self._self_stacklevel)
        return isinstance(instance, self.__wrapped__)

    def __subclasscheck__(self, instance):
        deprecation(self._self_message, stacklevel=self._self_stacklevel)
        return issubclass(instance, self.__wrapped__)


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
