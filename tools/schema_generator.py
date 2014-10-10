#!/usr/bin/env python

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

import contextlib
import re

import six
import tabulate

from taskflow.persistence.backends import impl_sqlalchemy

NAME_MAPPING = {
    'flowdetails': 'Flow details',
    'atomdetails': 'Atom details',
    'logbooks': 'Logbooks',
}
CONN_CONF = {
    # This uses an in-memory database (aka nothing is written)
    "connection": "sqlite://",
}
TABLE_QUERY = "SELECT name, sql FROM sqlite_master WHERE type='table'"
SCHEMA_QUERY = "pragma table_info(%s)"


def to_bool_string(val):
    if isinstance(val, (int, bool)):
        return six.text_type(bool(val))
    if not isinstance(val, six.string_types):
        val = six.text_type(val)
    if val.lower() in ('0', 'false'):
        return 'False'
    if val.lower() in ('1', 'true'):
        return 'True'
    raise ValueError("Unknown boolean input '%s'" % (val))


def main():
    backend = impl_sqlalchemy.SQLAlchemyBackend(CONN_CONF)
    with contextlib.closing(backend) as backend:
        # Make the schema exist...
        with contextlib.closing(backend.get_connection()) as conn:
            conn.upgrade()
        # Now make a prettier version of that schema...
        tables = backend.engine.execute(TABLE_QUERY)
        table_names = [r[0] for r in tables]
        for i, table_name in enumerate(table_names):
            pretty_name = NAME_MAPPING.get(table_name, table_name)
            print("*" + pretty_name + "*")
            # http://www.sqlite.org/faq.html#q24
            table_name = table_name.replace("\"", "\"\"")
            rows = []
            for r in backend.engine.execute(SCHEMA_QUERY % table_name):
                # Cut out the numbers from things like VARCHAR(12) since
                # this is not very useful to show users who just want to
                # see the basic schema...
                row_type = re.sub(r"\(.*?\)", "", r['type']).strip()
                if not row_type:
                    raise ValueError("Row %s of table '%s' was empty after"
                                     " cleaning" % (r['cid'], table_name))
                rows.append([r['name'], row_type, to_bool_string(r['pk'])])
            contents = tabulate.tabulate(
                rows, headers=['Name', 'Type', 'Primary Key'],
                tablefmt="rst")
            print("\n%s" % contents.strip())
            if i + 1 != len(table_names):
                print("")


if __name__ == '__main__':
    main()
