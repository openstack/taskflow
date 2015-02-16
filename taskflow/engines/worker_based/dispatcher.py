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

from kombu import exceptions as kombu_exc

from taskflow import exceptions as excp
from taskflow import logging
from taskflow.utils import kombu_utils as ku

LOG = logging.getLogger(__name__)


class Handler(object):
    """Component(s) that will be called on reception of messages."""

    __slots__ = ['_process_message', '_validator']

    def __init__(self, process_message, validator=None):
        self._process_message = process_message
        self._validator = validator

    @property
    def process_message(self):
        """Main callback that is called to process a received message.

        This is only called after the format has been validated (using
        the ``validator`` callback if applicable) and only after the message
        has been acknowledged.
        """
        return self._process_message

    @property
    def validator(self):
        """Optional callback that will be activated before processing.

        This callback if present is expected to validate the message and
        raise :py:class:`~taskflow.exceptions.InvalidFormat` if the message
        is not valid.
        """
        return self._validator


class TypeDispatcher(object):
    """Receives messages and dispatches to type specific handlers."""

    def __init__(self, type_handlers=None, requeue_filters=None):
        if type_handlers is not None:
            self._type_handlers = dict(type_handlers)
        else:
            self._type_handlers = {}
        if requeue_filters is not None:
            self._requeue_filters = list(requeue_filters)
        else:
            self._requeue_filters = []

    @property
    def type_handlers(self):
        """Dictionary of message type -> callback to handle that message.

        The callback(s) will be activated by looking for a message
        property 'type' and locating a callback in this dictionary that maps
        to that type; if one is found it is expected to be a callback that
        accepts two positional parameters; the first being the message data
        and the second being the message object. If a callback is not found
        then the message is rejected and it will be up to the underlying
        message transport to determine what this means/implies...
        """
        return self._type_handlers

    @property
    def requeue_filters(self):
        """List of filters (callbacks) to request a message to be requeued.

        The callback(s) will be activated before the message has been acked and
        it can be used to instruct the dispatcher to requeue the message
        instead of processing it. The callback, when called, will be provided
        two positional parameters; the first being the message data and the
        second being the message object. Using these provided parameters the
        filter should return a truthy object if the message should be requeued
        and a falsey object if it should not.
        """
        return self._requeue_filters

    def _collect_requeue_votes(self, data, message):
        # Returns how many of the filters asked for the message to be requeued.
        requeue_votes = 0
        for i, cb in enumerate(self._requeue_filters):
            try:
                if cb(data, message):
                    requeue_votes += 1
            except Exception:
                LOG.exception("Failed calling requeue filter %s '%s' to"
                              " determine if message %r should be requeued.",
                              i + 1, cb, message.delivery_tag)
        return requeue_votes

    def _requeue_log_error(self, message, errors):
        # TODO(harlowja): Remove when http://github.com/celery/kombu/pull/372
        # is merged and a version is released with this change...
        try:
            message.requeue()
        except errors as exc:
            # This was taken from how kombu is formatting its messages
            # when its reject_log_error or ack_log_error functions are
            # used so that we have a similar error format for requeuing.
            LOG.critical("Couldn't requeue %r, reason:%r",
                         message.delivery_tag, exc, exc_info=True)
        else:
            LOG.debug("Message '%s' was requeued.", ku.DelayedPretty(message))

    def _process_message(self, data, message, message_type):
        handler = self._type_handlers.get(message_type)
        if handler is None:
            message.reject_log_error(logger=LOG,
                                     errors=(kombu_exc.MessageStateError,))
            LOG.warning("Unexpected message type: '%s' in message"
                        " '%s'", message_type, ku.DelayedPretty(message))
        else:
            if handler.validator is not None:
                try:
                    handler.validator(data)
                except excp.InvalidFormat as e:
                    message.reject_log_error(
                        logger=LOG, errors=(kombu_exc.MessageStateError,))
                    LOG.warn("Message '%s' (%s) was rejected due to it being"
                             " in an invalid format: %s",
                             ku.DelayedPretty(message), message_type, e)
                    return
            message.ack_log_error(logger=LOG,
                                  errors=(kombu_exc.MessageStateError,))
            if message.acknowledged:
                LOG.debug("Message '%s' was acknowledged.",
                          ku.DelayedPretty(message))
                handler.process_message(data, message)
            else:
                message.reject_log_error(logger=LOG,
                                         errors=(kombu_exc.MessageStateError,))

    def on_message(self, data, message):
        """This method is called on incoming messages."""
        LOG.debug("Received message '%s'", ku.DelayedPretty(message))
        if self._collect_requeue_votes(data, message):
            self._requeue_log_error(message,
                                    errors=(kombu_exc.MessageStateError,))
        else:
            try:
                message_type = message.properties['type']
            except KeyError:
                message.reject_log_error(
                    logger=LOG, errors=(kombu_exc.MessageStateError,))
                LOG.warning("The 'type' message property is missing"
                            " in message '%s'", ku.DelayedPretty(message))
            else:
                self._process_message(data, message, message_type)
