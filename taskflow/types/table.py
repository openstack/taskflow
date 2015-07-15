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

import itertools
import os

from debtcollector import removals
import six

# TODO(harlowja): remove me in a future version, since the futurist
# is the replacement for this whole module...
removals.removed_module(__name__,
                        replacement="the 'prettytable' library",
                        version="1.16", removal_version='2.0',
                        stacklevel=4)


class PleasantTable(object):
    """A tiny pretty printing table (like prettytable/tabulate but smaller).

    Creates simply formatted tables (with no special sauce)::

        >>> from taskflow.types import table
        >>> tbl = table.PleasantTable(['Name', 'City', 'State', 'Country'])
        >>> tbl.add_row(["Josh", "San Jose", "CA", "USA"])
        >>> print(tbl.pformat())
        +------+----------+-------+---------+
          Name |   City   | State | Country
        +------+----------+-------+---------+
          Josh | San Jose |  CA   |   USA
        +------+----------+-------+---------+
    """

    # Constants used when pretty formatting the table.
    COLUMN_STARTING_CHAR = ' '
    COLUMN_ENDING_CHAR = ''
    COLUMN_SEPARATOR_CHAR = '|'
    HEADER_FOOTER_JOINING_CHAR = '+'
    HEADER_FOOTER_CHAR = '-'
    LINE_SEP = os.linesep

    @staticmethod
    def _center_text(text, max_len, fill=' '):
        return '{0:{fill}{align}{size}}'.format(text, fill=fill,
                                                align="^", size=max_len)

    @classmethod
    def _size_selector(cls, possible_sizes):
        """Select the maximum size, utility function for adding borders.

        The number two is used so that the edges of a column have spaces
        around them (instead of being right next to a column separator).

        :param possible_sizes: possible sizes available
        :returns: maximum size
        :rtype: number
        """
        try:
            return max(x + 2 for x in possible_sizes)
        except ValueError:
            return 0

    def __init__(self, columns):
        if len(columns) == 0:
            raise ValueError("Column count must be greater than zero")
        self._columns = [column.strip() for column in columns]
        self._rows = []

    def add_row(self, row):
        if len(row) != len(self._columns):
            raise ValueError("Row must have %s columns instead of"
                             " %s columns" % (len(self._columns), len(row)))
        self._rows.append([six.text_type(column) for column in row])

    def pformat(self):
        # Figure out the maximum column sizes...
        column_count = len(self._columns)
        column_sizes = [0] * column_count
        headers = []
        for i, column in enumerate(self._columns):
            possible_sizes_iter = itertools.chain(
                [len(column)], (len(row[i]) for row in self._rows))
            column_sizes[i] = self._size_selector(possible_sizes_iter)
            headers.append(self._center_text(column, column_sizes[i]))
        # Build the header and footer prefix/postfix.
        header_footer_buf = six.StringIO()
        header_footer_buf.write(self.HEADER_FOOTER_JOINING_CHAR)
        for i, header in enumerate(headers):
            header_footer_buf.write(self.HEADER_FOOTER_CHAR * len(header))
            if i + 1 != column_count:
                header_footer_buf.write(self.HEADER_FOOTER_JOINING_CHAR)
        header_footer_buf.write(self.HEADER_FOOTER_JOINING_CHAR)
        # Build the main header.
        content_buf = six.StringIO()
        content_buf.write(header_footer_buf.getvalue())
        content_buf.write(self.LINE_SEP)
        content_buf.write(self.COLUMN_STARTING_CHAR)
        for i, header in enumerate(headers):
            if i + 1 == column_count:
                if self.COLUMN_ENDING_CHAR:
                    content_buf.write(headers[i])
                    content_buf.write(self.COLUMN_ENDING_CHAR)
                else:
                    content_buf.write(headers[i].rstrip())
            else:
                content_buf.write(headers[i])
                content_buf.write(self.COLUMN_SEPARATOR_CHAR)
        content_buf.write(self.LINE_SEP)
        content_buf.write(header_footer_buf.getvalue())
        # Build the main content.
        row_count = len(self._rows)
        if row_count:
            content_buf.write(self.LINE_SEP)
            for i, row in enumerate(self._rows):
                pieces = []
                for j, column in enumerate(row):
                    pieces.append(self._center_text(column, column_sizes[j]))
                    if j + 1 != column_count:
                        pieces.append(self.COLUMN_SEPARATOR_CHAR)
                blob = ''.join(pieces)
                if self.COLUMN_ENDING_CHAR:
                    content_buf.write(self.COLUMN_STARTING_CHAR)
                    content_buf.write(blob)
                    content_buf.write(self.COLUMN_ENDING_CHAR)
                else:
                    blob = blob.rstrip()
                    if blob:
                        content_buf.write(self.COLUMN_STARTING_CHAR)
                        content_buf.write(blob)
                if i + 1 != row_count:
                    content_buf.write(self.LINE_SEP)
            content_buf.write(self.LINE_SEP)
            content_buf.write(header_footer_buf.getvalue())
        return content_buf.getvalue()
