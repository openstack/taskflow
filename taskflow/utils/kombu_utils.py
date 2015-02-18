# -*- coding: utf-8 -*-

#    Copyright (C) 2015 Yahoo! Inc. All Rights Reserved.
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

# Keys extracted from the message properties when formatting...
_MSG_PROPERTIES = tuple([
    'correlation_id',
    'delivery_info/routing_key',
    'type',
])


class DelayedPretty(object):
    """Wraps a message and delays prettifying it until requested.

    TODO(harlowja): remove this when https://github.com/celery/kombu/pull/454/
    is merged and a release is made that contains it (since that pull
    request is equivalent and/or better than this).
    """

    def __init__(self, message):
        self._message = message
        self._message_pretty = None

    def __str__(self):
        if self._message_pretty is None:
            self._message_pretty = _prettify_message(self._message)
        return self._message_pretty


def _get_deep(properties, *keys):
    """Get a final key among a list of keys (each with its own sub-dict)."""
    for key in keys:
        properties = properties[key]
    return properties


def _prettify_message(message):
    """Kombu doesn't currently have a useful ``__str__()`` or ``__repr__()``.

    This provides something decent(ish) for debugging (or other purposes) so
    that messages are more nice and understandable....
    """
    if message.content_type is not None:
        properties = {
            'content_type': message.content_type,
        }
    else:
        properties = {}
    for name in _MSG_PROPERTIES:
        segments = name.split("/")
        try:
            value = _get_deep(message.properties, *segments)
        except (KeyError, ValueError, TypeError):
            pass
        else:
            if value is not None:
                properties[segments[-1]] = value
    if message.body is not None:
        properties['body_length'] = len(message.body)
    return "%(delivery_tag)s: %(properties)s" % {
        'delivery_tag': message.delivery_tag,
        'properties': properties,
    }
