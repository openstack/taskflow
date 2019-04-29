# -*- coding: utf-8 -*-

import datetime

# -- General configuration ----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.inheritance_diagram',
    'sphinx.ext.viewcode',
    'openstackdocstheme'
]

# openstackdocstheme options
repository_name = 'openstack/taskflow'
bug_project = 'taskflow'
bug_tag = ''

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
source_tree = 'https://opendev.org/openstack/taskflow/src/branch/master/'
extlinks = {
    'example': (source_tree + '/taskflow/examples/%s.py', ''),
    'pybug': ('http://bugs.python.org/issue%s', ''),
}


# -- Options for HTML output --------------------------------------------------

# The theme to use for HTML and HTML Help pages.  Major themes that come with
# Sphinx are currently 'default' and 'sphinxdoc'.
# html_theme_path = ["."]
html_theme = 'openstackdocs'


# -- Options for autoddoc ----------------------------------------------------

# Keep source order
autodoc_member_order = 'bysource'

# Always include members
autodoc_default_options = {'members': None, 'show-inheritance': None}
