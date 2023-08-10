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
documentation_summary = config.description

github_url = "https://github.com/{github_username}/{github_repository}".format_map(
    config
)
# project = "infineac"
# copyright = "2023, falseAzure"
# author = "falseAzure"

# version = "0.1.1"
# release = "0.1.1"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.napoleon",  # Supports Google / Numpy docstring
    "sphinx.ext.autodoc",  # Documentation from docstrings
    "sphinx.ext.doctest",  # Test snippets in documentation
    "sphinx.ext.todo",  # to-do syntax highlighting
    "sphinx.ext.ifconfig",  # Content based configuration
    "sphinx_toolbox.documentation_summary",  # Add a summary to each page
    "m2r2",  # Markdown support
]

source_suffix = [".rst", ".md"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_material"
html_static_path = ["_static"]
