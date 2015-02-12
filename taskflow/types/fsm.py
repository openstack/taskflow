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

try:
    from collections import OrderedDict  # noqa
except ImportError:
    from ordereddict import OrderedDict  # noqa

import six

from taskflow import exceptions as excp
from taskflow.types import table


class _Jump(object):
    """A FSM transition tracks this data while jumping."""
    def __init__(self, name, on_enter, on_exit):
        self.name = name
        self.on_enter = on_enter
        self.on_exit = on_exit


class FrozenMachine(Exception):
    """Exception raised when a frozen machine is modified."""
    def __init__(self):
        super(FrozenMachine, self).__init__("Frozen machine can't be modified")


class NotInitialized(excp.TaskFlowException):
    """Error raised when an action is attempted on a not inited machine."""


class FSM(object):
    """A finite state machine.

    This state machine can be used to automatically run a given set of
    transitions and states in response to events (either from callbacks or from
    generator/iterator send() values, see PEP 342). On each triggered event, a
    on_enter and on_exit callback can also be provided which will be called to
    perform some type of action on leaving a prior state and before entering a
    new state.

    NOTE(harlowja): reactions will *only* be called when the generator/iterator
    from run_iter() does *not* send back a new event (they will always be
    called if the run() method is used). This allows for two unique ways (these
    ways can also be intermixed) to use this state machine when using
    run_iter(); one where *external* events trigger the next state transition
    and one where *internal* reaction callbacks trigger the next state
    transition. The other way to use this state machine is to skip using run()
    or run_iter() completely and use the process_event() method explicitly and
    trigger the events via some *external* functionality.
    """
    def __init__(self, start_state):
        self._transitions = {}
        self._states = OrderedDict()
        self._start_state = start_state
        self._current = None
        self.frozen = False

    @property
    def start_state(self):
        return self._start_state

    @property
    def current_state(self):
        """Return the current state name.

        :returns: current state name
        :rtype: string
        """
        if self._current is not None:
            return self._current.name
        return None

    @property
    def terminated(self):
        """Returns whether the state machine is in a terminal state.

        :returns: whether the state machine is in
                  terminal state or not
        :rtype: boolean
        """
        if self._current is None:
            return False
        return self._states[self._current.name]['terminal']

    def add_state(self, state, terminal=False, on_enter=None, on_exit=None):
        """Adds a given state to the state machine.

        :param on_enter: callback, if provided will be expected to take
                         two positional parameters, these being state being
                         entered and the second parameter is the event that is
                         being processed that caused the state transition
        :param on_exit: callback, if provided will be expected to take
                         two positional parameters, these being state being
                         entered and the second parameter is the event that is
                         being processed that caused the state transition
        :param state: state being entered or exited
        :type state: string
        """
        if self.frozen:
            raise FrozenMachine()
        if state in self._states:
            raise excp.Duplicate("State '%s' already defined" % state)
        if on_enter is not None:
            if not six.callable(on_enter):
                raise ValueError("On enter callback must be callable")
        if on_exit is not None:
            if not six.callable(on_exit):
                raise ValueError("On exit callback must be callable")
        self._states[state] = {
            'terminal': bool(terminal),
            'reactions': {},
            'on_enter': on_enter,
            'on_exit': on_exit,
        }
        self._transitions[state] = OrderedDict()

    def add_reaction(self, state, event, reaction, *args, **kwargs):
        """Adds a reaction that may get triggered by the given event & state.

        :param state: the last stable state expressed
        :type state: string
        :param event: event that caused the transition
        :param args: non-keyworded arguments
        :type args: list
        :param kwargs: key-value pair arguments
        :type kwargs: dictionary

        Reaction callbacks may (depending on how the state machine is ran) be
        used after an event is processed (and a transition occurs) to cause
        the machine to react to the newly arrived at stable state. The
        expected result of a callback is expected to be a
        new event that the callback wants the state machine to react to.
        This new event may (depending on how the state machine is ran) get
        processed (and this process typically repeats) until the state
        machine reaches a terminal state.
        """
        if self.frozen:
            raise FrozenMachine()
        if state not in self._states:
            raise excp.NotFound("Can not add a reaction to event '%s' for an"
                                " undefined state '%s'" % (event, state))
        if not six.callable(reaction):
            raise ValueError("Reaction callback must be callable")
        if event not in self._states[state]['reactions']:
            self._states[state]['reactions'][event] = (reaction, args, kwargs)
        else:
            raise excp.Duplicate("State '%s' reaction to event '%s'"
                                 " already defined" % (state, event))

    def add_transition(self, start, end, event):
        """Adds an allowed transition from start -> end for the given event.

        :param start: start of the transition
        :param end: end of the transition
        :param event: event that caused the transition
        """
        if self.frozen:
            raise FrozenMachine()
        if start not in self._states:
            raise excp.NotFound("Can not add a transition on event '%s' that"
                                " starts in a undefined state '%s'" % (event,
                                                                       start))
        if end not in self._states:
            raise excp.NotFound("Can not add a transition on event '%s' that"
                                " ends in a undefined state '%s'" % (event,
                                                                     end))
        self._transitions[start][event] = _Jump(end,
                                                self._states[end]['on_enter'],
                                                self._states[start]['on_exit'])

    def process_event(self, event):
        """Trigger a state change in response to the provided event.

        :param event: event to be processed to cause a potential transition
        """
        current = self._current
        if current is None:
            raise NotInitialized("Can only process events after"
                                 " being initialized (not before)")
        if self._states[current.name]['terminal']:
            raise excp.InvalidState("Can not transition from terminal"
                                    " state '%s' on event '%s'"
                                    % (current.name, event))
        if event not in self._transitions[current.name]:
            raise excp.NotFound("Can not transition from state '%s' on"
                                " event '%s' (no defined transition)"
                                % (current.name, event))
        replacement = self._transitions[current.name][event]
        if current.on_exit is not None:
            current.on_exit(current.name, event)
        if replacement.on_enter is not None:
            replacement.on_enter(replacement.name, event)
        self._current = replacement
        return (
            self._states[replacement.name]['reactions'].get(event),
            self._states[replacement.name]['terminal'],
        )

    def initialize(self):
        """Sets up the state machine (sets current state to start state...)."""
        if self._start_state not in self._states:
            raise excp.NotFound("Can not start from a undefined"
                                " state '%s'" % (self._start_state))
        if self._states[self._start_state]['terminal']:
            raise excp.InvalidState("Can not start from a terminal"
                                    " state '%s'" % (self._start_state))
        # No on enter will be called, since we are priming the state machine
        # and have not really transitioned from anything to get here, we will
        # though allow 'on_exit' to be called on the event that causes this
        # to be moved from...
        self._current = _Jump(self._start_state, None,
                              self._states[self._start_state]['on_exit'])

    def run(self, event, initialize=True):
        """Runs the state machine, using reactions only."""
        for _transition in self.run_iter(event, initialize=initialize):
            pass

    def copy(self):
        """Copies the current state machine.

        NOTE(harlowja): the copy will be left in an *uninitialized* state.
        """
        c = FSM(self.start_state)
        c.frozen = self.frozen
        for state, data in six.iteritems(self._states):
            copied_data = data.copy()
            copied_data['reactions'] = copied_data['reactions'].copy()
            c._states[state] = copied_data
        for state, data in six.iteritems(self._transitions):
            c._transitions[state] = data.copy()
        return c

    def run_iter(self, event, initialize=True):
        """Returns a iterator/generator that will run the state machine.

        NOTE(harlowja): only one runner iterator/generator should be active for
        a machine, if this is not observed then it is possible for
        initialization and other local state to be corrupted and cause issues
        when running...
        """
        if initialize:
            self.initialize()
        while True:
            old_state = self.current_state
            reaction, terminal = self.process_event(event)
            new_state = self.current_state
            try:
                sent_event = yield (old_state, new_state)
            except GeneratorExit:
                break
            if terminal:
                break
            if reaction is None and sent_event is None:
                raise excp.NotFound("Unable to progress since no reaction (or"
                                    " sent event) has been made available in"
                                    " new state '%s' (moved to from state '%s'"
                                    " in response to event '%s')"
                                    % (new_state, old_state, event))
            elif sent_event is not None:
                event = sent_event
            else:
                cb, args, kwargs = reaction
                event = cb(old_state, new_state, event, *args, **kwargs)

    def __contains__(self, state):
        """Returns if this state exists in the machines known states.

        :param state: input state
        :type state: string
        :returns: checks whether the state exists in the machine
                  known states
        :rtype: boolean
        """
        return state in self._states

    def freeze(self):
        """Freezes & stops addition of states, transitions, reactions..."""
        self.frozen = True

    @property
    def states(self):
        """Returns the state names."""
        return list(six.iterkeys(self._states))

    @property
    def events(self):
        """Returns how many events exist.

        :returns: how many events exist
        :rtype: number
        """
        c = 0
        for state in six.iterkeys(self._states):
            c += len(self._transitions[state])
        return c

    def __iter__(self):
        """Iterates over (start, event, end) transition tuples."""
        for state in six.iterkeys(self._states):
            for event, target in six.iteritems(self._transitions[state]):
                yield (state, event, target.name)

    def pformat(self, sort=True):
        """Pretty formats the state + transition table into a string.

        NOTE(harlowja): the sort parameter can be provided to sort the states
        and transitions by sort order; with it being provided as false the rows
        will be iterated in addition order instead.

        **Example**::

            >>> from taskflow.types import fsm
            >>> f = fsm.FSM("sits")
            >>> f.add_state("sits")
            >>> f.add_state("barks")
            >>> f.add_state("wags tail")
            >>> f.add_transition("sits", "barks", "squirrel!")
            >>> f.add_transition("barks", "wags tail", "gets petted")
            >>> f.add_transition("wags tail", "sits", "gets petted")
            >>> f.add_transition("wags tail", "barks", "squirrel!")
            >>> print(f.pformat())
            +-----------+-------------+-----------+----------+---------+
                Start   |    Event    |    End    | On Enter | On Exit
            +-----------+-------------+-----------+----------+---------+
                barks   | gets petted | wags tail |          |
               sits[^]  |  squirrel!  |   barks   |          |
              wags tail | gets petted |   sits    |          |
              wags tail |  squirrel!  |   barks   |          |
            +-----------+-------------+-----------+----------+---------+
        """
        def orderedkeys(data):
            if sort:
                return sorted(six.iterkeys(data))
            return list(six.iterkeys(data))
        tbl = table.PleasantTable(["Start", "Event", "End",
                                   "On Enter", "On Exit"])
        for state in orderedkeys(self._states):
            prefix_markings = []
            if self.current_state == state:
                prefix_markings.append("@")
            postfix_markings = []
            if self.start_state == state:
                postfix_markings.append("^")
            if self._states[state]['terminal']:
                postfix_markings.append("$")
            pretty_state = "%s%s" % ("".join(prefix_markings), state)
            if postfix_markings:
                pretty_state += "[%s]" % "".join(postfix_markings)
            if self._transitions[state]:
                for event in orderedkeys(self._transitions[state]):
                    target = self._transitions[state][event]
                    row = [pretty_state, event, target.name]
                    if target.on_enter is not None:
                        try:
                            row.append(target.on_enter.__name__)
                        except AttributeError:
                            row.append(target.on_enter)
                    else:
                        row.append('')
                    if target.on_exit is not None:
                        try:
                            row.append(target.on_exit.__name__)
                        except AttributeError:
                            row.append(target.on_exit)
                    else:
                        row.append('')
                    tbl.add_row(row)
            else:
                tbl.add_row([pretty_state, "", "", "", ""])
        return tbl.pformat()
