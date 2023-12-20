.. costum-module-template.rst is necessary to get the wanted view of the
   module documentation in the API Reference: autosummary with toctree
   See https://github.com/sphinx-doc/sphinx/issues/7912

API Reference
-------------

.. autosummary::
    :toctree: _autosummary
    :template: custom-module-template.rst
    :recursive:
    :nosignatures:

    infineac
