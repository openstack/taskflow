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

import asyncore
import binascii
import collections
import errno
import functools
import hmac
import math
import os
import pickle
import socket
import struct
import time

import futurist
from oslo_utils import excutils
import six

from taskflow.engines.action_engine import executor as base
from taskflow import logging
from taskflow import task as ta
from taskflow.types import notifier as nt
from taskflow.utils import iter_utils
from taskflow.utils import misc
from taskflow.utils import schema_utils as su
from taskflow.utils import threading_utils

LOG = logging.getLogger(__name__)

# Internal parent <-> child process protocol schema, message constants...
MAGIC_HEADER = 0xDECAF
CHALLENGE = 'identify_yourself'
CHALLENGE_RESPONSE = 'worker_reporting_in'
ACK = 'ack'
EVENT = 'event'
SCHEMAS = {
    # Basic jsonschemas for verifying that the data we get back and
    # forth from parent <-> child observes at least a basic expected
    # format.
    CHALLENGE: {
        "type": "string",
        "minLength": 1,
    },
    ACK: {
        "type": "string",
        "minLength": 1,
    },
    CHALLENGE_RESPONSE: {
        "type": "string",
        "minLength": 1,
    },
    EVENT: {
        "type": "object",
        "properties": {
            'event_type': {
                "type": "string",
            },
            'sent_on': {
                "type": "number",
            },
        },
        "required": ['event_type', 'sent_on'],
        "additionalProperties": True,
    },
}

# See http://bugs.python.org/issue1457119 for why this is so complex...
_DECODE_ENCODE_ERRORS = [pickle.PickleError, TypeError]
try:
    import cPickle
    _DECODE_ENCODE_ERRORS.append(cPickle.PickleError)
    del cPickle
except (ImportError, AttributeError):
    pass
_DECODE_ENCODE_ERRORS = tuple(_DECODE_ENCODE_ERRORS)

# Use the best pickle from here on out...
from six.moves import cPickle as pickle


class UnknownSender(Exception):
    """Exception raised when message from unknown sender is recvd."""


class ChallengeIgnored(Exception):
    """Exception raised when challenge has not been responded to."""


class Reader(object):
    """Reader machine that streams & parses messages that it then dispatches.

    TODO(harlowja): Use python-suitcase in the future when the following
    are addressed/resolved and released:

    - https://github.com/digidotcom/python-suitcase/issues/28
    - https://github.com/digidotcom/python-suitcase/issues/29

    Binary format format is the following (no newlines in actual format)::

        <magic-header> (4 bytes)
        <mac-header-length> (4 bytes)
        <mac> (1 or more variable bytes)
        <identity-header-length> (4 bytes)
        <identity> (1 or more variable bytes)
        <msg-header-length> (4 bytes)
        <msg> (1 or more variable bytes)
    """

    #: Per state memory initializers.
    _INITIALIZERS = {
        'magic_header_left': 4,
        'mac_header_left': 4,
        'identity_header_left': 4,
        'msg_header_left': 4,
    }

    #: Linear steps/transitions (order matters here).
    _TRANSITIONS = tuple([
        'magic_header_left',
        'mac_header_left',
        'mac_left',
        'identity_header_left',
        'identity_left',
        'msg_header_left',
        'msg_left',
    ])

    def __init__(self, auth_key, dispatch_func, msg_limit=-1):
        if not six.callable(dispatch_func):
            raise ValueError("Expected provided dispatch function"
                             " to be callable")
        self.auth_key = auth_key
        self.dispatch_func = dispatch_func
        msg_limiter = iter_utils.iter_forever(msg_limit)
        self.msg_count = six.next(msg_limiter)
        self._msg_limiter = msg_limiter
        self._buffer = misc.BytesIO()
        self._state = None
        # Local machine variables and such are stored in here.
        self._memory = {}
        self._transitions = collections.deque(self._TRANSITIONS)
        # This is the per state callback handler set. The first entry reads
        # the data and the second entry is called after reading is completed,
        # typically to save that data into object memory, or to validate
        # it.
        self._handlers = {
            'magic_header_left': (self._read_field_data,
                                  self._save_and_validate_magic),
            'mac_header_left': (self._read_field_data,
                                functools.partial(self._save_pos_integer,
                                                  'mac_left')),
            'mac_left': (functools.partial(self._read_data, 'mac'),
                         functools.partial(self._save_data, 'mac')),
            'identity_header_left': (self._read_field_data,
                                     functools.partial(self._save_pos_integer,
                                                       'identity_left')),
            'identity_left': (functools.partial(self._read_data, 'identity'),
                              functools.partial(self._save_data, 'identity')),
            'msg_header_left': (self._read_field_data,
                                functools.partial(self._save_pos_integer,
                                                  'msg_left')),
            'msg_left': (functools.partial(self._read_data, 'msg'),
                         self._dispatch_and_reset),
        }
        # Force transition into first state...
        self._transition()

    def _save_pos_integer(self, key_name, data):
        key_val = struct.unpack("!i", data)[0]
        if key_val <= 0:
            raise IOError("Invalid %s length received for key '%s', expected"
                          " greater than zero length" % (key_val, key_name))
        self._memory[key_name] = key_val
        return True

    def _save_data(self, key_name, data):
        self._memory[key_name] = data
        return True

    def _dispatch_and_reset(self, data):
        self.dispatch_func(
            self._memory['identity'],
            # Lazy evaluate so the message can be thrown out as needed
            # (instead of the receiver discarding it after the fact)...
            functools.partial(_decode_message, self.auth_key, data,
                              self._memory['mac']))
        self.msg_count = six.next(self._msg_limiter)
        self._memory.clear()

    def _transition(self):
        try:
            self._state = self._transitions.popleft()
        except IndexError:
            self._transitions.extend(self._TRANSITIONS)
            self._state = self._transitions.popleft()
        try:
            self._memory[self._state] = self._INITIALIZERS[self._state]
        except KeyError:
            pass
        self._handle_func, self._post_handle_func = self._handlers[self._state]

    def _save_and_validate_magic(self, data):
        magic_header = struct.unpack("!i", data)[0]
        if magic_header != MAGIC_HEADER:
            raise IOError("Invalid magic header received, expected 0x%x but"
                          " got 0x%x for message %s" % (MAGIC_HEADER,
                                                        magic_header,
                                                        self.msg_count + 1))
        self._memory['magic'] = magic_header
        return True

    def _read_data(self, save_key_name, data):
        data_len_left = self._memory[self._state]
        self._buffer.write(data[0:data_len_left])
        if len(data) < data_len_left:
            data_len_left -= len(data)
            self._memory[self._state] = data_len_left
            return ''
        else:
            self._memory[self._state] = 0
            buf_data = self._buffer.getvalue()
            self._buffer.reset()
            self._post_handle_func(buf_data)
            self._transition()
            return data[data_len_left:]

    def _read_field_data(self, data):
        return self._read_data(self._state, data)

    @property
    def bytes_needed(self):
        return self._memory.get(self._state, 0)

    def feed(self, data):
        while len(data):
            data = self._handle_func(data)


class BadHmacValueError(ValueError):
    """Value error raised when an invalid hmac is discovered."""


def _create_random_string(desired_length):
    if desired_length <= 0:
        return b''
    data_length = int(math.ceil(desired_length / 2.0))
    data = os.urandom(data_length)
    hex_data = binascii.hexlify(data)
    return hex_data[0:desired_length]


def _calculate_hmac(auth_key, body):
    mac = hmac.new(auth_key, body).hexdigest()
    if isinstance(mac, six.text_type):
        mac = mac.encode("ascii")
    return mac


def _encode_message(auth_key, message, identity, reverse=False):
    message = pickle.dumps(message, 2)
    message_mac = _calculate_hmac(auth_key, message)
    pieces = [
        struct.pack("!i", MAGIC_HEADER),
        struct.pack("!i", len(message_mac)),
        message_mac,
        struct.pack("!i", len(identity)),
        identity,
        struct.pack("!i", len(message)),
        message,
    ]
    if reverse:
        pieces.reverse()
    return tuple(pieces)


def _decode_message(auth_key, message, message_mac):
    tmp_message_mac = _calculate_hmac(auth_key, message)
    if tmp_message_mac != message_mac:
        raise BadHmacValueError('Invalid message hmac')
    return pickle.loads(message)


class Channel(object):
    """Object that workers use to communicate back to their creator."""

    def __init__(self, port, identity, auth_key):
        self.identity = identity
        self.port = port
        self.auth_key = auth_key
        self.dead = False
        self._sent = self._received = 0
        self._socket = None
        self._read_pipe = None
        self._write_pipe = None

    def close(self):
        if self._socket is not None:
            self._socket.close()
            self._socket = None
            self._read_pipe = None
            self._write_pipe = None

    def _ensure_connected(self):
        if self._socket is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setblocking(1)
            try:
                s.connect(("", self.port))
            except socket.error as e:
                with excutils.save_and_reraise_exception():
                    s.close()
                    if e.errno in (errno.ECONNREFUSED, errno.ENOTCONN,
                                   errno.ECONNRESET):
                        # Don't bother with further connections...
                        self.dead = True
            read_pipe = s.makefile("rb", 0)
            write_pipe = s.makefile("wb", 0)
            try:
                msg = self._do_recv(read_pipe=read_pipe)
                su.schema_validate(msg, SCHEMAS[CHALLENGE])
                if msg != CHALLENGE:
                    raise IOError("Challenge expected not received")
                else:
                    pieces = _encode_message(self.auth_key,
                                             CHALLENGE_RESPONSE,
                                             self.identity)
                    self._do_send_and_ack(pieces, write_pipe=write_pipe,
                                          read_pipe=read_pipe)
            except Exception:
                with excutils.save_and_reraise_exception():
                    s.close()
            else:
                self._socket = s
                self._read_pipe = read_pipe
                self._write_pipe = write_pipe

    def recv(self):
        self._ensure_connected()
        return self._do_recv()

    def _do_recv(self, read_pipe=None):
        if read_pipe is None:
            read_pipe = self._read_pipe
        msg_capture = collections.deque(maxlen=1)
        msg_capture_func = (lambda _from_who, msg_decoder_func:
                            msg_capture.append(msg_decoder_func()))
        reader = Reader(self.auth_key, msg_capture_func, msg_limit=1)
        try:
            maybe_msg_num = self._received + 1
            bytes_needed = reader.bytes_needed
            while True:
                blob = read_pipe.read(bytes_needed)
                if len(blob) != bytes_needed:
                    raise EOFError("Read pipe closed while reading %s"
                                   " bytes for potential message %s"
                                   % (bytes_needed, maybe_msg_num))
                reader.feed(blob)
                bytes_needed = reader.bytes_needed
        except StopIteration:
            pass
        msg = msg_capture[0]
        self._received += 1
        return msg

    def _do_send(self, pieces, write_pipe=None):
        if write_pipe is None:
            write_pipe = self._write_pipe
        for piece in pieces:
            write_pipe.write(piece)
        write_pipe.flush()

    def _do_send_and_ack(self, pieces, write_pipe=None, read_pipe=None):
        self._do_send(pieces, write_pipe=write_pipe)
        self._sent += 1
        msg = self._do_recv(read_pipe=read_pipe)
        su.schema_validate(msg, SCHEMAS[ACK])
        if msg != ACK:
            raise IOError("Failed receiving ack for sent"
                          " message %s" % self._metrics['sent'])

    def send(self, message):
        self._ensure_connected()
        self._do_send_and_ack(_encode_message(self.auth_key, message,
                                              self.identity))


class EventSender(object):
    """Sends event information from a child worker process to its creator."""

    def __init__(self, channel):
        self._channel = channel
        self._pid = None

    def __call__(self, event_type, details):
        if not self._channel.dead:
            if self._pid is None:
                self._pid = os.getpid()
            message = {
                'event_type': event_type,
                'details': details,
                'sent_on': time.time(),
            }
            LOG.trace("Sending %s (from child %s)", message, self._pid)
            self._channel.send(message)


class DispatcherHandler(asyncore.dispatcher):
    """Dispatches from a single connection into a target."""

    #: Read/write chunk size.
    CHUNK_SIZE = 8192

    def __init__(self, sock, addr, dispatcher):
        if six.PY2:
            asyncore.dispatcher.__init__(self, map=dispatcher.map, sock=sock)
        else:
            super(DispatcherHandler, self).__init__(map=dispatcher.map,
                                                    sock=sock)
        self.blobs_to_write = list(dispatcher.challenge_pieces)
        self.reader = Reader(dispatcher.auth_key, self._dispatch)
        self.targets = dispatcher.targets
        self.tied_to = None
        self.challenge_responded = False
        self.ack_pieces = _encode_message(dispatcher.auth_key, ACK,
                                          dispatcher.identity,
                                          reverse=True)
        self.addr = addr

    def handle_close(self):
        self.close()

    def writable(self):
        return bool(self.blobs_to_write)

    def handle_write(self):
        try:
            blob = self.blobs_to_write.pop()
        except IndexError:
            pass
        else:
            sent = self.send(blob[0:self.CHUNK_SIZE])
            if sent < len(blob):
                self.blobs_to_write.append(blob[sent:])

    def _send_ack(self):
        self.blobs_to_write.extend(self.ack_pieces)

    def _dispatch(self, from_who, msg_decoder_func):
        if not self.challenge_responded:
            msg = msg_decoder_func()
            su.schema_validate(msg, SCHEMAS[CHALLENGE_RESPONSE])
            if msg != CHALLENGE_RESPONSE:
                raise ChallengeIgnored("Discarding connection from %s"
                                       " challenge was not responded to"
                                       % self.addr)
            else:
                LOG.trace("Peer %s (%s) has passed challenge sequence",
                          self.addr, from_who)
                self.challenge_responded = True
                self.tied_to = from_who
                self._send_ack()
        else:
            if self.tied_to != from_who:
                raise UnknownSender("Sender %s previously identified as %s"
                                    " changed there identity to %s after"
                                    " challenge sequence" % (self.addr,
                                                             self.tied_to,
                                                             from_who))
            try:
                task = self.targets[from_who]
            except KeyError:
                raise UnknownSender("Unknown message from %s (%s) not matched"
                                    " to any known target" % (self.addr,
                                                              from_who))
            msg = msg_decoder_func()
            su.schema_validate(msg, SCHEMAS[EVENT])
            if LOG.isEnabledFor(logging.TRACE):
                msg_delay = max(0, time.time() - msg['sent_on'])
                LOG.trace("Dispatching message from %s (%s) (it took %0.3f"
                          " seconds for it to arrive for processing after"
                          " being sent)", self.addr, from_who, msg_delay)
            task.notifier.notify(msg['event_type'], msg.get('details'))
            self._send_ack()

    def handle_read(self):
        data = self.recv(self.CHUNK_SIZE)
        if len(data) == 0:
            self.handle_close()
        else:
            try:
                self.reader.feed(data)
            except (IOError, UnknownSender):
                LOG.warning("Invalid received message", exc_info=True)
                self.handle_close()
            except _DECODE_ENCODE_ERRORS:
                LOG.warning("Badly formatted message", exc_info=True)
                self.handle_close()
            except (ValueError, su.ValidationError):
                LOG.warning("Failed validating message", exc_info=True)
                self.handle_close()
            except ChallengeIgnored:
                LOG.warning("Failed challenge sequence", exc_info=True)
                self.handle_close()


class Dispatcher(asyncore.dispatcher):
    """Accepts messages received from child worker processes."""

    #: See https://docs.python.org/2/library/socket.html#socket.socket.listen
    MAX_BACKLOG = 5

    def __init__(self, map, auth_key, identity):
        if six.PY2:
            asyncore.dispatcher.__init__(self, map=map)
        else:
            super(Dispatcher, self).__init__(map=map)
        self.identity = identity
        self.challenge_pieces = _encode_message(auth_key, CHALLENGE,
                                                identity, reverse=True)
        self.auth_key = auth_key
        self.targets = {}

    @property
    def port(self):
        if self.socket is not None:
            return self.socket.getsockname()[1]
        else:
            return None

    def setup(self):
        self.targets.clear()
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bind(("", 0))
        LOG.trace("Accepting dispatch requests on port %s", self.port)
        self.listen(self.MAX_BACKLOG)

    def writable(self):
        return False

    @property
    def map(self):
        return self._map

    def handle_close(self):
        if self.socket is not None:
            self.close()

    def handle_accept(self):
        pair = self.accept()
        if pair is not None:
            sock, addr = pair
            addr = "%s:%s" % (addr[0], addr[1])
            LOG.trace("Potentially accepted new connection from %s", addr)
            DispatcherHandler(sock, addr, self)


class ParallelProcessTaskExecutor(base.ParallelTaskExecutor):
    """Executes tasks in parallel using a process pool executor.

    NOTE(harlowja): this executor executes tasks in external processes, so that
    implies that tasks that are sent to that external process are pickleable
    since this is how the multiprocessing works (sending pickled objects back
    and forth) and that the bound handlers (for progress updating in
    particular) are proxied correctly from that external process to the one
    that is alive in the parent process to ensure that callbacks registered in
    the parent are executed on events in the child.
    """

    #: Default timeout used by asyncore io loop (and eventually select/poll).
    WAIT_TIMEOUT = 0.01

    constructor_options = [
        ('max_workers', lambda v: v if v is None else int(v)),
        ('wait_timeout', lambda v: v if v is None else float(v)),
    ]
    """
    Optional constructor keyword arguments this executor supports. These will
    typically be passed via engine options (by a engine user) and converted
    into the correct type before being sent into this
    classes ``__init__`` method.
    """

    def __init__(self, executor=None,
                 max_workers=None, wait_timeout=None):
        super(ParallelProcessTaskExecutor, self).__init__(
            executor=executor, max_workers=max_workers)
        self._auth_key = _create_random_string(32)
        self._dispatcher = Dispatcher({}, self._auth_key,
                                      _create_random_string(32))
        if wait_timeout is None:
            self._wait_timeout = self.WAIT_TIMEOUT
        else:
            if wait_timeout <= 0:
                raise ValueError("Provided wait timeout must be greater"
                                 " than zero and not '%s'" % wait_timeout)
            self._wait_timeout = wait_timeout
        # Only created after starting...
        self._worker = None

    def _create_executor(self, max_workers=None):
        return futurist.ProcessPoolExecutor(max_workers=max_workers)

    def start(self):
        if threading_utils.is_alive(self._worker):
            raise RuntimeError("Worker thread must be stopped via stop()"
                               " before starting/restarting")
        super(ParallelProcessTaskExecutor, self).start()
        self._dispatcher.setup()
        self._worker = threading_utils.daemon_thread(
            asyncore.loop, map=self._dispatcher.map,
            timeout=self._wait_timeout)
        self._worker.start()

    def stop(self):
        super(ParallelProcessTaskExecutor, self).stop()
        self._dispatcher.close()
        if threading_utils.is_alive(self._worker):
            self._worker.join()
            self._worker = None

    def _submit_task(self, func, task, *args, **kwargs):
        """Submit a function to run the given task (with given args/kwargs).

        NOTE(harlowja): Adjust all events to be proxies instead since we want
        those callbacks to be activated in this process, not in the child,
        also since typically callbacks are functors (or callables) we can
        not pickle those in the first place...

        To make sure people understand how this works, the following is a
        lengthy description of what is going on here, read at will:

        So to ensure that we are proxying task triggered events that occur
        in the executed subprocess (which will be created and used by the
        thing using the multiprocessing based executor) we need to establish
        a link between that process and this process that ensures that when a
        event is triggered in that task in that process that a corresponding
        event is triggered on the original task that was requested to be ran
        in this process.

        To accomplish this we have to create a copy of the task (without
        any listeners) and then reattach a new set of listeners that will
        now instead of calling the desired listeners just place messages
        for this process (a dispatcher thread that is created in this class)
        to dispatch to the original task (using a common accepting socket and
        per task sender socket that is used and associated to know
        which task to proxy back too, since it is possible that there many
        be *many* subprocess running at the same time).

        Once the subprocess task has finished execution, the executor will
        then trigger a callback that will remove the task + target from the
        dispatcher (which will stop any further proxying back to the original
        task).
        """
        progress_callback = kwargs.pop('progress_callback', None)
        clone = task.copy(retain_listeners=False)
        identity = _create_random_string(32)
        channel = Channel(self._dispatcher.port, identity, self._auth_key)

        def rebind_task():
            # Creates and binds proxies for all events the task could receive
            # so that when the clone runs in another process that this task
            # can receive the same notifications (thus making it look like the
            # the notifications are transparently happening in this process).
            proxy_event_types = set()
            for (event_type, listeners) in task.notifier.listeners_iter():
                if listeners:
                    proxy_event_types.add(event_type)
            if progress_callback is not None:
                proxy_event_types.add(ta.EVENT_UPDATE_PROGRESS)
            if nt.Notifier.ANY in proxy_event_types:
                # NOTE(harlowja): If ANY is present, just have it be
                # the **only** event registered, as all other events will be
                # sent if ANY is registered (due to the nature of ANY sending
                # all the things); if we also include the other event types
                # in this set if ANY is present we will receive duplicate
                # messages in this process (the one where the local
                # task callbacks are being triggered). For example the
                # emissions of the tasks notifier (that is running out
                # of process) will for specific events send messages for
                # its ANY event type **and** the specific event
                # type (2 messages, when we just want one) which will
                # cause > 1 notify() call on the local tasks notifier, which
                # causes more local callback triggering than we want
                # to actually happen.
                proxy_event_types = set([nt.Notifier.ANY])
            if proxy_event_types:
                # This sender acts as our forwarding proxy target, it
                # will be sent pickled to the process that will execute
                # the needed task and it will do the work of using the
                # channel object to send back messages to this process for
                # dispatch into the local task.
                sender = EventSender(channel)
                for event_type in proxy_event_types:
                    clone.notifier.register(event_type, sender)
            return bool(proxy_event_types)

        def register():
            if progress_callback is not None:
                task.notifier.register(ta.EVENT_UPDATE_PROGRESS,
                                       progress_callback)
            self._dispatcher.targets[identity] = task

        def deregister(fut=None):
            if progress_callback is not None:
                task.notifier.deregister(ta.EVENT_UPDATE_PROGRESS,
                                         progress_callback)
            self._dispatcher.targets.pop(identity, None)

        should_register = rebind_task()
        if should_register:
            register()
        try:
            fut = self._executor.submit(func, clone, *args, **kwargs)
        except RuntimeError:
            with excutils.save_and_reraise_exception():
                if should_register:
                    deregister()

        fut.atom = task
        if should_register:
            fut.add_done_callback(deregister)
        return fut
