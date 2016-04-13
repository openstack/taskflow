# -*- coding: utf-8 -*-

import datetime
import os
import subprocess
import sys
import warnings

sys.path.insert(0, os.path.abspath('../..'))
# -- General configuration ----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.inheritance_diagram',
    'sphinx.ext.viewcode',
    'oslosphinx'
]

# autodoc generation is a bit aggressive and a nuisance when doing heavy
# text edit cycles.
# execute "export SPHINX_DEBUG=1" in your terminal to disable

# Add any paths that contain templates here, relative to this directory.
templates_path = ['templates']

# The suffix of source filenames.
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ['_build']

# General information about the project.
project = u'TaskFlow'
copyright = u'%s, OpenStack Foundation' % datetime.date.today().year
source_tree = 'http://git.openstack.org/cgit/openstack/taskflow/tree'

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = True

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
add_module_names = True

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# Prefixes that are ignored for sorting the Python module index
modindex_common_prefix = ['taskflow.']

# Shortened external links.
extlinks = {
    'example': (source_tree + '/taskflow/examples/%s.py', ''),
    'pybug': ('http://bugs.python.org/issue%s', ''),
}

# -- Options for HTML output --------------------------------------------------

# The theme to use for HTML and HTML Help pages.  Major themes that come with
# Sphinx are currently 'default' and 'sphinxdoc'.
# html_theme_path = ["."]
# html_theme = '_theme'
html_static_path = ['static']

# Output file base name for HTML help builder.
htmlhelp_basename = '%sdoc' % project

git_cmd = ["git", "log", "--pretty=format:'%ad, commit %h'", "--date=local",
           "-n1"]
try:
    html_last_updated_fmt = subprocess.Popen(
        git_cmd, stdout=subprocess.PIPE).communicate()[0]
except Exception:
    warnings.warn('Cannot get last updated time from git repository. '
                  'Not setting "html_last_updated_fmt".')

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass
# [howto/manual]).
latex_documents = [
    ('index',
     '%s.tex' % project,
     '%s Documentation' % project,
     'OpenStack Foundation', 'manual'),
]

# -- Options for autoddoc ----------------------------------------------------

# Keep source order
autodoc_member_order = 'bysource'

# Always include members
autodoc_default_flags = ['members', 'show-inheritance']

