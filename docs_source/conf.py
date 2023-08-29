# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys

from sphinx_pyproject import SphinxConfig

sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath(".."))

config = SphinxConfig(globalns=globals())

author = config.author
project = config.name
release = config.version
version = release
documentation_summary = config.description

github_url = "https://github.com/{github_username}/{github_repository}".format_map(
    config
)


# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",  # Supports Google / Numpy docstring
    "sphinx.ext.autodoc",  # Documentation from docstrings
    "sphinx.ext.doctest",  # Test snippets in documentation
    "sphinx.ext.todo",  # to-do syntax highlighting
    "sphinx.ext.ifconfig",  # Content based configuration
    "sphinx.ext.viewcode",  # add link to source code
    "sphinx_toolbox.documentation_summary",  # Add a summary to each page
    "sphinx_copybutton",  # Add copy button to code blocks
]

source_suffix = [".rst", ".md"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]

html_short_title = "infineac"

html_theme_options = {
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/falseazure/infineac",
            "icon": "fa-brands fa-github",
        },
    ],
    "logo": {
        "text": "InFineac",
        "alt_text": "InFineac",
    },
    "show_nav_level": 3,
    "navigation_depth": 3,
}
