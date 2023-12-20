.. start-after and end-before are used to include only the relevant parts of the README.rst file in the documentation.

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

   infineac.compare_results
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