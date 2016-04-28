# -*- coding: utf-8 -*-

#    Copyright (C) 2016 Yahoo! Inc. All Rights Reserved.
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

import os
import string

import six

from taskflow.utils import misc
from taskflow import version


BANNER_HEADER = string.Template("""
___    __
 |    |_
 |ask |low v$version
""".strip())
BANNER_HEADER = BANNER_HEADER.substitute(version=version.version_string())


def make_banner(what, chapters):
    """Makes a taskflow banner string.

    For example::

      >>> from taskflow.utils import banner
      >>> chapters = {
          'Connection details': {
              'Topic': 'hello',
          },
          'Powered by': {
              'Executor': 'parallel',
          },
      }
      >>> print(banner.make_banner('Worker', chapters))

    This will output::

      ___    __
       |    |_
       |ask |low v1.26.1
      *Worker*
      Connection details:
        Topic => hello
      Powered by:
        Executor => parallel
    """
    buf = misc.StringIO()
    buf.write_nl(BANNER_HEADER)
    if chapters:
        buf.write_nl("*%s*" % what)
        chapter_names = sorted(six.iterkeys(chapters))
    else:
        buf.write("*%s*" % what)
        chapter_names = []
    for i, chapter_name in enumerate(chapter_names):
        chapter_contents = chapters[chapter_name]
        if chapter_contents:
            buf.write_nl("%s:" % (chapter_name))
        else:
            buf.write("%s:" % (chapter_name))
        if isinstance(chapter_contents, dict):
            section_names = sorted(six.iterkeys(chapter_contents))
            for j, section_name in enumerate(section_names):
                if j + 1 < len(section_names):
                    buf.write_nl("  %s => %s"
                                 % (section_name,
                                    chapter_contents[section_name]))
                else:
                    buf.write("  %s => %s" % (section_name,
                                              chapter_contents[section_name]))
        elif isinstance(chapter_contents, (list, tuple, set)):
            if isinstance(chapter_contents, set):
                sections = sorted(chapter_contents)
            else:
                sections = chapter_contents
            for j, section in enumerate(sections):
                if j + 1 < len(sections):
                    buf.write_nl("  %s. %s" % (j + 1, section))
                else:
                    buf.write("  %s. %s" % (j + 1, section))
        else:
            raise TypeError("Unsupported chapter contents"
                            " type: one of dict, list, tuple, set expected"
                            " and not %s" % type(chapter_contents).__name__)
        if i + 1 < len(chapter_names):
            buf.write_nl("")
    # NOTE(harlowja): this is needed since the template in this file
    # will always have newlines that end with '\n' (even on different
    # platforms due to the way this source file is encoded) so we have
    # to do this little dance to make it platform neutral...
    if os.linesep != "\n":
        return misc.fix_newlines(buf.getvalue())
    return buf.getvalue()
