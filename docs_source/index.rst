.. infineac documentation master file, created by
   sphinx-quickstart on Sat Aug  5 13:17:24 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

InFineac
========

.. include:: ../README.rst
    :start-after: start short_desc
    :end-before: end short_desc

.. include:: ../README.rst
    :start-after: start overview_wo
    :end-before: end overview


Documentation
-------------

.. toctree::
   :maxdepth: 3

   user_guide
   detailed_description

Package
-------
.. toctree::
   :maxdepth: 1
   
   modules

.. autosummary:: 

   infineac.constants
   infineac.file_loader
   infineac.helper
   infineac.pipeline
   infineac.process_event
   infineac.process_text
   infineac.topic_extractor



   

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. include:: ../README.rst
    :start-after: start references
    :end-before: .. end references