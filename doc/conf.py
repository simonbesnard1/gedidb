# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
import os
import sys
import importlib
import gedidb

from docutils import nodes
from docutils.parsers.rst import Directive
from datetime import datetime
import inspect
from importlib.metadata import version as version_

# Minimum version, enforced by sphinx
needs_sphinx = "4.3"

# -----------------------------------------------------------------------------
# General configuration
# -----------------------------------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be extensions
# coming with Sphinx (named 'sphinx.ext.*') or your custom ones.

sys.path.insert(0, os.path.abspath("../sphinxext"))

extensions = [
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
    "sphinx.ext.extlinks",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
    "sphinx_autosummary_accessors",
    "sphinx.ext.linkcode",
    "sphinxext.opengraph",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_inline_tabs",
    "sphinx_remove_toctrees",
    "sphinx_gallery.gen_gallery"

]

# Gallery configuration
sphinx_gallery_conf = {
     'filename_pattern': '/plot_',
     'examples_dirs': 'gallery',   # path to your example scripts
     'gallery_dirs': 'auto_examples',  # path to where to save gallery generated output
}

skippable_extensions = [
    ("breathe", "skip generating C/C++ API from comment blocks."),
]
for ext, warn in skippable_extensions:
    ext_exist = importlib.util.find_spec(ext) is not None
    if ext_exist:
        extensions.append(ext)
    else:
        print(f"Unable to find Sphinx extension '{ext}', {warn}.")

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = ".rst"

# General substitutions.
project = "gediDB"
year = datetime.now().year
copyright = f"2024-{year}, gediDB Developers"

# The full version, including alpha/beta/rc tags.
version = version_("gedidb")

# There are two options for replacing |today|: either, you set today to some
# non-false value, then it is used:
# today = ''
# Else, today_fmt is used as the format for a strftime call.
today_fmt = "%B %d, %Y"
html_last_updated_fmt = today_fmt

# List of documents that shouldn't be included in the build.
# unused_docs = []

# The reST default role (used for this markup: `text`) to use for all documents.
default_role = "autolink"

# List of directories, relative to source directories, that shouldn't be searched
# for source files.
exclude_dirs = []

exclude_patterns = ['gallery/README.rst']

# If true, '()' will be appended to :func: etc. cross-reference text.
add_function_parentheses = False

# If true, the current module name will be prepended to all description
# unit titles (such as .. function::).
# add_module_names = True

# If true, sectionauthor and moduleauthor directives will be shown in the
# output. They are ignored by default.
# show_authors = False


class LegacyDirective(Directive):
    """
    Adapted from docutils/parsers/rst/directives/admonitions.py

    Uses a default text if the directive does not have contents. If it does,
    the default text is concatenated to the contents.

    See also the same implementation in SciPy's conf.py.
    """
    has_content = True
    node_class = nodes.admonition
    optional_arguments = 1

    def run(self):
        try:
            obj = self.arguments[0]
        except IndexError:
            # Argument is empty; use default text
            obj = "submodule"
        text = (f"This {obj} is considered legacy and will no longer receive "
                "updates. This could also mean it will be removed in future "
                "gediDB versions.")

        try:
            self.content[0] = text + " " + self.content[0]
        except IndexError:
            # Content is empty; use the default text
            source, lineno = self.state_machine.get_source_and_line(
                self.lineno
            )
            self.content.append(
                text,
                source=source,
                offset=lineno
            )
        text = '\n'.join(self.content)
        # Create the admonition node, to be populated by `nested_parse`
        admonition_node = self.node_class(rawsource=text)
        # Set custom title
        title_text = "Legacy"
        textnodes, _ = self.state.inline_text(title_text, self.lineno)
        title = nodes.title(title_text, '', *textnodes)
        # Set up admonition node
        admonition_node += title
        # Select custom class for CSS styling
        admonition_node['classes'] = ['admonition-legacy']
        # Parse the directive contents
        self.state.nested_parse(self.content, self.content_offset,
                                admonition_node)
        return [admonition_node]


def setup(app):
    # add a config value for `ifconfig` directives
    app.add_config_value('python_version_major', str(sys.version_info.major), 'env')
    app.add_directive("legacy", LegacyDirective)

# While these objects do have type `module`, the names are aliases for modules
# elsewhere. Sphinx does not support referring to modules by an aliases name,
# so we make the alias look like a "real" module for it.
# If we deemed it desirable, we could in future make these real modules, which
# would make `from numpy.char import split` work.
# sys.modules['gedidb.char'] = gedidb.char

# -----------------------------------------------------------------------------
# HTML output
# -----------------------------------------------------------------------------

# Set up the version switcher.  The versions.json is stored in the doc repo.
# Determine the version to display in the switcher
    
    
html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "logo": {
        "image_light": "_static/logos/gediDB_logo.svg",
        "image_dark": "_static/logos/gediDB_logo.svg",
    },
    "gitlab_url": "https://git.gfz-potsdam.de/global-land-monitoring/gedidb/",
    "collapse_navigation": True,
    
    "header_links_before_dropdown": 6,
    "navbar_end": [
        "search-button",
        "theme-switcher",
        "navbar-icon-links"
    ],
    "navbar_persistent": [],
    "show_version_warning_banner": True,
}


html_title = "%s v%s Manual" % (project, version)
html_static_path = ["_static"]
html_last_updated_fmt = "%b %d, %Y"
html_css_files = ["gedidb.css"]
html_context = {"default_mode": "dark"}
html_use_modindex = True
html_copy_source = False
html_domain_indices = False
html_file_suffix = ".html"

htmlhelp_basename = "gedidbdoc"

if "sphinx.ext.pngmath" in extensions:
    pngmath_use_preview = True
    pngmath_dvipng_args = ["-gamma", "1.5", "-D", "96", "-bg", "Transparent"]

mathjax_path = "scipy-mathjax/MathJax.js?config=scipy-mathjax"

plot_html_show_formats = False
plot_html_show_source_link = False

# sphinx-copybutton configurations
copybutton_prompt_text = r">>> |\.\.\. |\$ |In \[\d*\]: | {2,5}\.\.\.: | {5,8}: "
copybutton_prompt_is_regexp = True
# -----------------------------------------------------------------------------
# LaTeX output
# -----------------------------------------------------------------------------

# The paper size ('letter' or 'a4').
# latex_paper_size = 'letter'

# The font size ('10pt', '11pt' or '12pt').
# latex_font_size = '10pt'

# XeLaTeX for better support of unicode characters
latex_engine = "xelatex"

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, document class [howto/manual]).
_stdauthor = "Written by the gediDB members"
latex_documents = [
    ("reference/index", "gedidb-ref.tex", "gediDB Reference", _stdauthor, "manual"),
    ("user/index", "gedidb-user.tex", "gediDB User Guide", _stdauthor, "manual"),
]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
# latex_logo = None

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
# latex_use_parts = False

latex_elements = {}

# Additional stuff for the LaTeX preamble.
latex_elements[
    "preamble"
] = r"""
\newfontfamily\FontForChinese{FandolSong-Regular}[Extension=.otf]
\catcode`琴\active\protected\def琴{{\FontForChinese\string琴}}
\catcode`春\active\protected\def春{{\FontForChinese\string春}}
\catcode`鈴\active\protected\def鈴{{\FontForChinese\string鈴}}
\catcode`猫\active\protected\def猫{{\FontForChinese\string猫}}
\catcode`傅\active\protected\def傅{{\FontForChinese\string傅}}
\catcode`立\active\protected\def立{{\FontForChinese\string立}}
\catcode`业\active\protected\def业{{\FontForChinese\string业}}
\catcode`（\active\protected\def（{{\FontForChinese\string（}}
\catcode`）\active\protected\def）{{\FontForChinese\string）}}

% In the parameters section, place a newline after the Parameters
% header.  This is default with Sphinx 5.0.0+, so no need for
% the old hack then.
% Unfortunately sphinx.sty 5.0.0 did not bump its version date
% so we check rather sphinxpackagefootnote.sty (which exists
% since Sphinx 4.0.0).
\makeatletter
\@ifpackagelater{sphinxpackagefootnote}{2022/02/12}
    {}% Sphinx >= 5.0.0, nothing to do
    {%
\usepackage{expdlist}
\let\latexdescription=\description
\def\description{\latexdescription{}{} \breaklabel}
% but expdlist old LaTeX package requires fixes:
% 1) remove extra space
\usepackage{etoolbox}
\patchcmd\@item{{\@breaklabel} }{{\@breaklabel}}{}{}
% 2) fix bug in expdlist's way of breaking the line after long item label
\def\breaklabel{%
    \def\@breaklabel{%
        \leavevmode\par
        % now a hack because Sphinx inserts \leavevmode after term node
        \def\leavevmode{\def\leavevmode{\unhbox\voidb@x}}%
    }%
}
    }% Sphinx < 5.0.0 (and assumed >= 4.0.0)
\makeatother

% Make Examples/etc section headers smaller and more compact
\makeatletter
\titleformat{\paragraph}{\normalsize\py@HeaderFamily}%
            {\py@TitleColor}{0em}{\py@TitleColor}{\py@NormalColor}
\titlespacing*{\paragraph}{0pt}{1ex}{0pt}
\makeatother

% Fix footer/header
\renewcommand{\chaptermark}[1]{\markboth{\MakeUppercase{\thechapter.\ #1}}{}}
\renewcommand{\sectionmark}[1]{\markright{\MakeUppercase{\thesection.\ #1}}}
"""

# Documents to append as an appendix to all manuals.
# latex_appendices = []

# If false, no module index is generated.
latex_use_modindex = False


# -----------------------------------------------------------------------------
# Texinfo output
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Texinfo output
# -----------------------------------------------------------------------------

texinfo_documents = [
  ("index", 'gedidb', 'gediDB Documentation', _stdauthor, 'gediDB',
   "gediDB: A toolbox for processing and providing Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data",
   'Programming',
   1),
]


# -----------------------------------------------------------------------------
# Intersphinx configuration
# -----------------------------------------------------------------------------
intersphinx_mapping = {
    "neps": ("https://numpy.org/neps", None),
    "python": ("https://docs.python.org/3", None),
    "scipy": ("https://docs.scipy.org/doc/scipy", None),
    "matplotlib": ("https://matplotlib.org/stable", None),
    "imageio": ("https://imageio.readthedocs.io/en/stable", None),
    "skimage": ("https://scikit-image.org/docs/stable", None),
    "pandas": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "scipy-lecture-notes": ("https://scipy-lectures.org", None),
    "pytest": ("https://docs.pytest.org/en/stable", None),
    "numpy-tutorials": ("https://numpy.org/numpy-tutorials", None),
    "numpydoc": ("https://numpydoc.readthedocs.io/en/latest", None),
    "dlpack": ("https://dmlc.github.io/dlpack/latest", None),
}


# -----------------------------------------------------------------------------
# gediDB extensions
# -----------------------------------------------------------------------------

# If we want to do a phantom import from an XML file for all autodocs
phantom_import_file = "dump.xml"

# Make numpydoc to generate plots for example sections
numpydoc_use_plots = True

# -----------------------------------------------------------------------------
# Autosummary
# -----------------------------------------------------------------------------

autosummary_generate = True

# -----------------------------------------------------------------------------
# Coverage checker
# -----------------------------------------------------------------------------
coverage_ignore_modules = r"""
    """.split()
coverage_ignore_functions = r"""
    test($|_) (some|all)true bitwise_not cumproduct pkgload
    generic\.
    """.split()
coverage_ignore_classes = r"""
    """.split()

coverage_c_path = []
coverage_c_regexes = {}
coverage_ignore_c_items = {}

# -----------------------------------------------------------------------------
# Source code links
# -----------------------------------------------------------------------------


for name in ["sphinx.ext.linkcode", "gedidbdoc.linkcode"]:
    try:
        __import__(name)
        extensions.append(name)
        break
    except ImportError:
        pass
else:
    print("NOTE: linkcode extension not found -- no links to source generated")


def _get_c_source_file(obj):
    if issubclass(obj, gedidb.generic):
        return r"_core/src/multiarray/scalartypes.c.src"
    elif obj is gedidb.ndarray:
        return r"_core/src/multiarray/arrayobject.c"
    else:
        # todo: come up with a better way to generate these
        return None


# based on numpy doc/source/conf.py
def linkcode_resolve(domain, info):
    """
    Determine the URL corresponding to Python object
    """
    if domain != "py":
        return None

    modname = info["module"]
    fullname = info["fullname"]

    submod = sys.modules.get(modname)
    if submod is None:
        return None

    obj = submod
    for part in fullname.split("."):
        try:
            obj = getattr(obj, part)
        except AttributeError:
            return None

    try:
        fn = inspect.getsourcefile(inspect.unwrap(obj))
    except TypeError:
        fn = None
    if not fn:
        return None

    try:
        source, lineno = inspect.getsourcelines(obj)
    except OSError:
        lineno = None

    if lineno:
        linespec = f"#L{lineno}-L{lineno + len(source) - 1}"
    else:
        linespec = ""

    fn = os.path.relpath(fn, start=os.path.dirname(gedidb.__file__))

    if "+" in gedidb.__version__:
        return f"https://git.gfz-potsdam.de/global-land-monitoring/gedidb/{fn}{linespec}"
    else:
        return (
            f"https://git.gfz-potsdam.de/global-land-monitoring/gedidb/blob/"
            f"v{gedidb.__version__}/gedidb/{fn}{linespec}"
        )


def html_page_context(app, pagename, templatename, context, doctree):
    # Disable edit button for docstring generated pages
    if "generated" in pagename:
        context["theme_use_edit_page_button"] = False

# -----------------------------------------------------------------------------
# Breathe & Doxygen
# -----------------------------------------------------------------------------


breathe_projects = dict(gedidb=os.path.join("..", "build", "doxygen", "xml"))
breathe_default_project = "gedidb"
breathe_default_members = ("members", "undoc-members", "protected-members")

# See https://github.com/breathe-doc/breathe/issues/696
nitpick_ignore = [
    ("c:identifier", "FILE"),
    ("c:identifier", "size_t"),
    ("c:identifier", "PyHeapTypeObject"),
]
