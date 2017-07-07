----------
Resumption
----------

Overview
========

**Question**: *How can we persist the flow so that it can be resumed, restarted
or rolled-back on engine failure?*

**Answer:** Since a flow is a set of :doc:`atoms <atoms>` and relations between
atoms we need to create a model and corresponding information that allows us to
persist the *right* amount of information to preserve, resume, and rollback a
flow on software or hardware failure.

To allow for resumption TaskFlow must be able to re-create the flow and
re-connect the links between atom (and between atoms->atom details and so on)
in order to revert those atoms or resume those atoms in the correct ordering.
TaskFlow provides a pattern that can help in automating this process (it does
**not** prohibit the user from creating their own strategies for doing this).

.. _resumption factories:

Factories
=========

The default provided way is to provide a `factory`_ function which will create
(or recreate your workflow). This function can be provided when loading a flow
and corresponding engine via the provided :py:meth:`load_from_factory()
<taskflow.engines.helpers.load_from_factory>` method. This `factory`_ function
is expected to be a function (or ``staticmethod``) which is reimportable (aka
has a well defined name that can be located by the ``__import__`` function in
python, this excludes ``lambda`` style functions and ``instance`` methods). The
`factory`_ function name will be saved into the logbook and it will be imported
and called to create the workflow objects (or recreate it if resumption
happens). This allows for the flow to be recreated if and when that is needed
(even on remote machines, as long as the reimportable name can be located).

.. _factory: https://en.wikipedia.org/wiki/Factory_%28object-oriented_programming%29

Names
=====

When a flow is created it is expected that each atom has a unique name, this
name serves a special purpose in the resumption process (as well as serving a
useful purpose when running, allowing for atom identification in the
:doc:`notification <notifications>` process). The reason for having names is
that an atom in a flow needs to be somehow  matched with (a potentially)
existing :py:class:`~taskflow.persistence.models.AtomDetail` during engine
resumption & subsequent running.

The match should be:

* stable if atoms are added or removed
* should not change when service is restarted, upgraded...
* should be the same across all server instances in HA setups

Names provide this although they do have weaknesses:

* the names of atoms must be unique in flow
* it becomes hard to change the name of atom since a name change causes other
  side-effects

.. note::

    Even though these weaknesses names were selected as a *good enough*
    solution for the above matching requirements (until something better is
    invented/created that can satisfy those same requirements).

Scenarios
=========

When new flow is loaded into engine, there is no persisted data for it yet, so
a corresponding :py:class:`~taskflow.persistence.models.FlowDetail` object
will be created, as well as a
:py:class:`~taskflow.persistence.models.AtomDetail` object for each atom that
is contained in it. These will be immediately saved into the persistence
backend that is configured. If no persistence backend is configured, then as
expected nothing will be saved and the atoms and flow will be ran in a
non-persistent manner.

**Subsequent run:** When we resume the flow from a persistent backend (for
example, if the flow was interrupted and engine destroyed to save resources or
if the service was restarted), we need to re-create the flow. For that, we will
call the function that was saved on first-time loading that builds the flow for
us (aka; the flow factory function described above) and the engine will run.
The following scenarios explain some expected structural changes and how they
can be accommodated (and what the effect will be when resuming & running).

Same atoms
++++++++++

When the factory function mentioned above returns the exact same the flow and
atoms (no changes are performed).

**Runtime change:** Nothing should be done -- the engine will re-associate
atoms with :py:class:`~taskflow.persistence.models.AtomDetail` objects by name
and then the engine resumes.

Atom was added
++++++++++++++

When the factory function mentioned above alters the flow by adding a new atom
in (for example for changing the runtime structure of what was previously ran
in the first run).

**Runtime change:** By default when the engine resumes it will notice that a
corresponding :py:class:`~taskflow.persistence.models.AtomDetail` does not
exist and one will be created and associated.

Atom was removed
++++++++++++++++

When the factory function mentioned above alters the flow by removing a new
atom in (for example for changing the runtime structure of what was previously
ran in the first run).

**Runtime change:** Nothing should be done -- flow structure is reloaded from
factory function, and removed atom is not in it -- so, flow will be ran as if
it was not there, and any results it returned if it was completed before will
be ignored.

Atom code was changed
+++++++++++++++++++++

When the factory function mentioned above alters the flow by deciding that a
newer version of a previously existing atom should be ran (possibly to perform
some kind of upgrade or to fix a bug in a prior atoms code).

**Factory change:** The atom name & version will have to be altered. The
factory should replace this name where it was being used previously.

**Runtime change:** This will fall under the same runtime adjustments that
exist when a new atom is added. In the future TaskFlow could make this easier
by providing a ``upgrade()`` function that can be used to give users the
ability to upgrade atoms before running (manual introspection & modification of
a :py:class:`~taskflow.persistence.models.LogBook` can be done before engine
loading and running to accomplish this in the meantime).

Atom was split in two atoms or merged
+++++++++++++++++++++++++++++++++++++

When the factory function mentioned above alters the flow by deciding that a
previously existing atom should be split into N atoms or the factory function
decides that N atoms should be merged in <N atoms (typically occurring during
refactoring).

**Runtime change:** This will fall under the same runtime adjustments that
exist when a new atom is added or removed. In the future TaskFlow could make
this easier by providing a ``migrate()`` function that can be used to give
users the ability to migrate atoms previous data before running (manual
introspection & modification of a
:py:class:`~taskflow.persistence.models.LogBook` can be done before engine
loading and running to accomplish this in the meantime).

Flow structure was changed
++++++++++++++++++++++++++

If manual links were added or removed from graph, or task requirements were
changed, or flow was refactored (atom moved into or out of subflows, linear
flow was replaced with graph flow, tasks were reordered in linear flow, etc).

**Runtime change:** Nothing should be done.
