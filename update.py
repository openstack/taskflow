# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 Red Hat, Inc.
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

# Taken from oslo commit 09baf99fc62 and modified for taskflow usage.

r"""
A simple script to update taskflows modules which have been copied
into other projects. See:

  http://wiki.openstack.org/CommonLibrary#Incubation

The script can be called the following ways:

  $> python update.py ../myproj
  $> python update.py --config-file ../myproj/taskflow.conf

Where ../myproj is a project directory containing taskflow.conf which
might look like:

  [DEFAULT]
  primitives = flow.linear_flow,flow.graph_flow,task
  base = myproj

Or:

  $> python update.py ../myproj/myconf.conf
  $> python update.py --config-file ../myproj/myconf.conf

Where ../myproj is a project directory which contains a differently named
configuration file, or:

  $> python update.py --config-file ../myproj/myproj/taskflow.conf
                      --dest-dir ../myproj

Where ../myproject is a project directory, but the configuration file is
stored in a sub-directory, or:

  $> python update.py --primitives flow.linear_flow --base myproj ../myproj
  $> python update.py --primitives flow.linear_flow,flow.graph_flow,task
                      --base myproj --dest-dir ../myproj

Where ../myproject is a project directory, but we explicitly specify
the primitives to copy and the base destination module

Obviously, the first way is the easiest!
"""

from __future__ import print_function

import collections
import functools
import os
import os.path
import re
import shutil
import sys

from oslo.config import cfg

opts = [
    cfg.ListOpt('primitives',
                default=[],
                help='The list of primitives to copy from taskflow'),
    cfg.StrOpt('base',
               default=None,
               help='The base module to hold the copy of taskflow'),
    cfg.StrOpt('dest-dir',
               default=None,
               help='Destination project directory'),
    cfg.StrOpt('configfile_or_destdir',
               default=None,
               help='A config file or destination project directory',
               positional=True),
]
allowed_primitives = ['flow', 'task', 'decorators']


def _parse_args(argv):
    conf = cfg.ConfigOpts()
    conf.register_cli_opts(opts)
    conf(argv, usage='Usage: %(prog)s [config-file|dest-dir]')

    if conf.configfile_or_destdir:
        def def_config_file(dest_dir):
            return os.path.join(dest_dir, 'taskflow.conf')

        config_file = None
        if os.path.isfile(conf.configfile_or_destdir):
            config_file = conf.configfile_or_destdir
        elif (os.path.isdir(conf.configfile_or_destdir)
              and os.path.isfile(def_config_file(conf.configfile_or_destdir))):
            config_file = def_config_file(conf.configfile_or_destdir)

        if config_file:
            conf(argv + ['--config-file', config_file])

    return conf


def _explode_path(path):
    dirs = []
    comps = []
    dirs.append(path)
    (head, tail) = os.path.split(path)
    while tail:
        dirs.append(head)
        comps.append(tail)
        path = head
        (head, tail) = os.path.split(path)
    dirs.sort()
    comps.reverse()
    return (dirs, comps)


def _mod_to_path(mod):
    return os.path.join(*mod.split('.'))


def _dest_path(path, base, dest_dir):
    return os.path.join(dest_dir, _mod_to_path(base), path)


def _replace(path, pattern, replacement):
    with open(path, "rb+") as f:
        lines = f.readlines()
        f.seek(0)
        f.truncate()
        for line in lines:
            f.write(re.sub(pattern, replacement, line))


def _drop_init(path):
    with open(path, 'wb') as fh:
        contents = '''
# vim: tabstop=4 shiftwidth=4 softtabstop=4
'''
        fh.write(contents.strip())
        fh.write("\n")


def _make_dirs(path):
    dir_name = os.path.dirname(path)
    for d in _explode_path(dir_name)[0]:
        if not os.path.isdir(d):
            print("  '%s/' (new)" % (d))
            os.mkdir(d)
            init_path = os.path.join(d, '__init__.py')
            if not os.path.exists(init_path):
                print("  '%s' (new)" % (init_path))
                _drop_init(init_path)


def _join_prefix_postfix(prefix, postfix):
    joined = str(prefix)
    if postfix:
        if joined:
            joined += '.'
        joined += str(postfix)
    return joined


def _copy_file(path, dest, base, common_already=None):
    _make_dirs(dest)

    print("  '%s' -> '%s'" % (path, dest))
    shutil.copy2(path, dest)

    def import_replace(path):
        with open(path, "rb+") as f:
            lines = f.readlines()
            f.seek(0)
            f.truncate()
            for (i, line) in enumerate(lines):
                segments = _parse_import_line(line, i + 1, path)
                if segments:
                    old_line = line
                    (comment, prefix, postfix, alias) = segments
                    line = 'from %s import %s'
                    importing = _join_prefix_postfix(prefix, postfix)
                    if common_already and importing in common_already:
                        # Use the existing openstack common
                        mod = '%s.openstack.common' % (base)
                    else:
                        mod = '%s.taskflow' % (base)
                        if prefix:
                            mod += ".%s" % (prefix)
                    line = line % (mod, postfix)
                    if alias:
                        line += ' as %s' % (alias)
                    if comment:
                        line += ' #' + str(comment)
                    line += "\n"
                    if old_line != line:
                        print(" '%s' -> '%s'" % (old_line.strip(),
                                                 line.strip()))
                f.write(line)

    print("Fixing up %s" % (dest))
    import_replace(dest)
    _replace(dest,
             'possible_topdir, "taskflow",$',
             'possible_topdir, "' + base + '",')


def _is_mod_path(segments):
    if not segments:
        return False
    mod = ".".join(segments)
    mod_path = _mod_to_path("taskflow.%s" % (mod)) + ".py"
    if os.path.exists(mod_path):
        return True
    return False


def _split_import(text):
    pieces = []
    for piece in text.split("."):
        piece = piece.strip()
        if piece:
            pieces.append(piece)
    return pieces


def _copy_pyfile(path, base, dest_dir, common_already=None):
    _copy_file(path, _dest_path(path, base, dest_dir), base, common_already)


def _copy_mod(mod, base, dest_dir, common_already=None):
    full_mod = base + '.taskflow.%s' % mod
    base_mod = 'taskflow.%s' % mod
    print("Copying module '%s' -> '%s'" % (base_mod, full_mod))

    copy_pyfile = functools.partial(_copy_pyfile,
                                    base=base, dest_dir=dest_dir,
                                    common_already=common_already)

    mod_file = _mod_to_path(base_mod) + ".py"
    if os.path.isfile(mod_file):
        copy_pyfile(mod_file)
    else:
        raise IOError("Can not find module file: %s" % (mod_file))


def _parse_import_line(line, linenum=-1, filename=None):

    def blowup():
        msg = "Invalid import at '%s'" % (line)
        if linenum > 0:
            msg += "; line %s" % (linenum)
        if filename:
            msg += " from file (%s)" % (filename)
        raise IOError(msg)

    def split_import(text):
        pieces = []
        for piece in text.split("."):
            piece = piece.strip()
            if piece:
                pieces.append(piece)
            else:
                blowup()
        return pieces

    result = re.match(r"\s*from\s+taskflow\s*(.*)$", line)
    if not result:
        return None
    rest = result.group(1).split("#", 1)
    comment = ''
    if len(rest) > 1:
        comment = rest[1]
        rest = rest[0]
    else:
        rest = rest[0]
    if not rest:
        blowup()

    # Figure out the contents of a line like:
    #
    # from taskflow.xyz import blah as blah2

    # First looking at the '.xyz' part (if it exists)
    prefix = ''
    if rest.startswith("."):
        import_index = rest.find("import")
        if import_index == -1:
            blowup()
        before = rest[0:import_index - 1]
        before = before[1:]
        prefix += before
        rest = rest[import_index:]

    # Now examine the 'import blah' part.
    postfix = ''
    result = re.match(r"\s*import\s+(.*)$", rest)
    if not result:
        blowup()

    # Figure out if this is being aliased and keep the alias.
    importing = result.group(1).strip()
    alias_match = re.search(r"(.*?)\s+as\s+(.*)$", importing)
    alias = ''
    if not alias_match:
        postfix = importing
    else:
        alias = alias_match.group(2).strip()
        postfix = alias_match.group(1).strip()
    return (comment, prefix, postfix, alias)


def _find_import_modules(srcfile):
    with open(srcfile, 'rb') as f:
        for (i, line) in enumerate(f):
            segments = _parse_import_line(line, i + 1, srcfile)
            if segments:
                (comment, prefix, postfix, alias) = segments
                importing = _join_prefix_postfix(prefix, postfix)
                import_segments = _split_import(importing)
                while len(import_segments):
                    if _is_mod_path(import_segments):
                        break
                    else:
                        import_segments.pop()
                import_what = ".".join(import_segments)
                if import_what:
                    yield import_what


def _build_dependency_tree():
    dep_tree = {}
    base_path = 'taskflow'
    for dirpath, _, filenames in os.walk(base_path):
        for filename in [x for x in filenames if x.endswith('.py')]:
            if dirpath == base_path:
                mod_name = filename.split('.')[0]
            else:
                mod_name = dirpath.split(os.sep)[1:]
                mod_name = ".".join(mod_name)
                mod_name += '.' + filename.split('.')[0]
            if mod_name.endswith('__init__'):
                continue
            filepath = os.path.join(dirpath, filename)
            dep_list = dep_tree.setdefault(mod_name, [])
            dep_list.extend([x for x in _find_import_modules(filepath)
                             if x != mod_name and x not in dep_list])
    return dep_tree


def _dfs_dependency_tree(dep_tree, mod_name, mod_list=[]):
    mod_list.append(mod_name)
    for mod in dep_tree.get(mod_name, []):
        if mod not in mod_list:
            mod_list = _dfs_dependency_tree(dep_tree, mod, mod_list)
    return mod_list


def _complete_flow_list(flows):

    def check_fetch_mod(flow):
        mod = 'patterns.%s' % (flow)
        mod_path = _mod_to_path("taskflow.%s" % (mod)) + ".py"
        if not os.path.isfile(mod_path):
            raise IOError("Flow %s file not found at: %s" % (flow, mod_path))
        return mod

    flow_mods = []
    for f in flows:
        f = f.strip()
        if not f:
            continue
        flow_mods.append(check_fetch_mod(f))

    return _complete_module_list(flow_mods)


def _complete_module_list(base):
    dep_tree = _build_dependency_tree()
    mod_list = []
    for mod in base:
        for x in _dfs_dependency_tree(dep_tree, mod, []):
            if x not in mod_list and x not in base:
                mod_list.append(x)
    mod_list.extend(base)
    return mod_list


def _find_existing_common(mod, base, dest_dir):
    existing_mod = ".".join([base, mod])
    existing_path = _mod_to_path(existing_mod)
    existing_path = os.path.join(dest_dir, existing_path) + ".py"
    if not os.path.isfile(existing_path):
        return None
    return existing_mod


def _uniq_itr(itr):
    seen = []
    for i in itr:
        if i in seen:
            continue
        seen.append(i)
        yield i


def main(argv):
    conf = _parse_args(argv)

    dest_dir = conf.dest_dir
    if not dest_dir and conf.config_file:
        dest_dir = os.path.dirname(os.path.abspath(conf.config_file[-1]))

    if not dest_dir or not os.path.isdir(dest_dir):
        print("A valid destination dir is required", file=sys.stderr)
        sys.exit(1)

    primitives = [p for p in _uniq_itr(conf.primitives)]
    primitive_types = collections.defaultdict(list)
    for p in primitives:
        try:
            p_type, p = p.split(".", 1)
        except ValueError:
            p_type = p
            p = ''
        p_type = p_type.strip()
        p = p.strip()
        if p not in primitive_types[p_type]:
            primitive_types[p_type].append(p)

    # TODO(harlowja): for now these are the only primitives we are allowing to
    # be copied over. Later add more as needed.
    prims = 0
    for k in allowed_primitives:
        prims += len(primitive_types[k])
    if prims <= 0:
        allowed = ", ".join(sorted(allowed_primitives))
        print("A list of primitives to copy is required "
              "(%s is allowed)" % (allowed), file=sys.stderr)
        sys.exit(1)
    unknown_prims = []
    for k in primitive_types.keys():
        if k not in allowed_primitives:
            unknown_prims.append(k)
    if unknown_prims:
        allowed = ", ".join(sorted(allowed_primitives))
        unknown = ", ".join(sorted(unknown_prims))
        print("Unknown primitives (%s) are being copied "
              "(%s is allowed)" % (unknown, allowed), file=sys.stderr)
        sys.exit(1)

    if not conf.base:
        print("A destination base module is required", file=sys.stderr)
        sys.exit(1)

    def copy_mods(mod_list):
        common_already = []
        for mod in sorted(mod_list):
            if mod.startswith("openstack.common"):
                existing = _find_existing_common(mod, conf.base, dest_dir)
                if existing:
                    mod_list.remove(mod)
                    common_already.append(mod)
        for mod in _uniq_itr(sorted(mod_list)):
            _copy_mod(mod, conf.base, dest_dir, common_already)

    copy_what = []
    copy_what.extend(_complete_flow_list(primitive_types.pop('flow', [])))
    for k in primitive_types.keys():
        copy_what.extend(_complete_module_list([k]))
    copy_mods(copy_what)


if __name__ == "__main__":
    main(sys.argv[1:])
