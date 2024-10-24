########
InFineac
########

.. start short_desc

**Extracting Financial Insights from Earnings Calls using NLP.**

.. end short_desc


.. contents::


User Guide
**********

.. start overview

Overview
========

.. start overview_wo

InFineac is a Python package that extracts financial insights from earnings
calls by categorizing them into a range of topics using NLP. Earnings calls are
a rich source of information for investors, that are held quarterly by publicly
traded companies. InFineac heavily uses the spaCy_ and BERTopic_ libraries for
the NLP tasks.

.. end overview

.. start install

Install
=======

First create a new conda environment with python 3.10 and activate it:

.. code-block:: bash

    conda create -n infineac python=3.10
    conda activate infineac


Then install this repository as a package, the ``-e`` flag installs the package
in editable mode, so you can make changes to the code and they will be
reflected in the package.

.. code-block:: bash

    pip install -e .

All the requirements are specified in the ``pyproject.toml`` file with the needed
versions.

.. end install


.. start quickstart

Quickstart
==========

The ``scripts`` folder contains the scripts to load the data and extract the
topics.

Load the data
-------------

The ``load_data.py`` script loads the data from the xml-files and saves it as a
(compressed) pickle.

.. code-block:: bash

    python scripts/load_save_data.py

-p, --path      Path to directory of earnings calls transcripts
-c, --compress      Whether to compress the pickle file


Create Corpus
-------------

The ``create_corpus.py`` script creates a corpus from the data by extracting
relevant passages from the earnings calls. The corpus is saved as a compressed pickle file.

.. code-block:: bash

    python scripts/create_corpus.py

-p, --path       Path to pickle/lz4 file containing the earnings calls transcripts
-y, --year       Year to filter events by - all events before this year will be removed
-k, --keywords       Keywords to filter events by - all events not containing these keywords will be removed
-s, --sections       Section/s to extract passages from
-w, --window         Context window size in sentences
-par, --paragraphs   Whether to include subsequent paragraphs
-j, --join           Whether to join adjacent sentences
-a, --answers        Whether to extract answers from the Q&A section if keywords are present in the preceding question.
-rk, --remove_keywords   Whether to remove keywords from the extracted passages
-rn, --remove_names      Whether to remove names from the extracted passages
-rs, --remove_strategies  Whether to remove stopwords from the extracted passages
-ra, --remove_additional_words    Whether to remove additional words from the extracted passages


Extract Topics
--------------

The ``extract_topics.py`` script extracts the topics from a corpus of earnings
calls and saves them as a pickle/lz4 file. Additionally it saves the the
results, the aggregated results (per company and year) and the topics as
.xlsx-files.

.. code-block:: bash

    python scripts/extract_topics.py

-p, --path       Path to pickle/lz4 file containing the earnings calls transcripts
-pe, --preload_events   Path to pickle/lz4 file containing the events
-pc, --preloaded_corpus  Path to pickle/lz4 file containing the corpus
-y, --year       Year to filter events by - all events before this year will be removed
-k, --keywords       Keywords to filter events by - all events not containing these keywords will be removed
-s, --sections       Section/s to extract passages from
-w, --window         Context window size in sentences
-par, --paragraphs   Whether to include subsequent paragraphs
-j, --join           Whether to join adjacent sentences
-a, --answers        Whether to extract answers from the Q&A section if keywords are present in the preceding question.
-rk, --remove_keywords   Whether to remove keywords from the extracted passages
-rn, --remove_names      Whether to remove names from the extracted passages
-rs, --remove_strategies  Whether to remove stopwords from the extracted passages
-ra, --remove_additional_words    Whether to remove additional words from the extracted passages
-t, --threshold     All documents with equal or less words than the threshold are removed from the corpus

.. end quickstart

.. start file_structure

File structure
==============


.. code-block:: bash

    📦infineac
    ┣ 📂docs_source
    ┣ 📂notebooks
    ┃ ┗ 📜infineac.ipynb
    ┣ 📂infineac
    ┃ ┣ 📜__init__.py
    ┃ ┣ 📜constants.py
    ┃ ┣ 📜file_loader.py
    ┃ ┣ 📜helper.py
    ┃ ┣ 📜pipeline.py
    ┃ ┣ 📜process_event.py
    ┃ ┣ 📜process_text.py
    ┃ ┗ 📜topic_extractor.py
    ┣ 📂scripts
    ┃ ┣ 📜create_corpus.py
    ┃ ┣ 📜extract_topics.py
    ┃ ┗ 📜load_save_transcripts.py
    ┣ 📂tests
    ┃ ┗ 📜test.py
    ┣ 📜.gitignore
    ┣ 📜LICENSE
    ┣ 📜pyproject.toml
    ┣ 📜README.rst
    ┗ 📜tox.ini


* ``docs:source``: Contains the source for creating the documentation of the project.
  
* ``notebooks/infineac.ipynb``: This notebook contains the execution process and
  insights gained throughout the project.

* ``infineac``: Contains the code of the project. This is a python
  package that is installed in the conda environment. This package is used to import
  the code in our scripts and notebooks. The ``project.toml`` file contains
  the necessary information for the installation of this repository. The structure
  of this folder is the following:

  * ``__init__.py``: Initializes the ``infineac`` package. 
  * ``constants.py``: Contains the constants used throughout the project.
  * ``file_loader``: Contains the functions for loading and initially
    preprocessing the earnings calls from the xml-files.
  * ``helper.py``: Contains the helper functions used throughout the project.
  * ``pipeline.py``: Contains the functions for the entire pipeline of the project.
  * ``process_event.py``: Contains all the necessary functions for processing the
    earnings calls events.
  * ``process_text.py``: Contains all the necessary functions for the processing
    of text, which are used by ``process_event.py``.
  * ``topic_extractor.py``: Contains the functions for extracting the topics from
    the earnings calls.

* ``scripts``: This folder contains the scripts that are used to load the
  transcripts, preprocess the corpus and extract the topics of the earnings calls.
* ``tests``: Contains the unit tests for the code.
* ``pyproject.toml``: Contains all the information about the installation of this
  repository. You can use this file to install this repository as a package in
  your conda environment.

.. end file_structure


.. start detailed_description

Detailed Description
********************





.. start detailed_description_wo

Research question
=================

InFineac is a Python package that extracts financial insights from earnings
calls by categorizing them into a range of topics using NLP. These topics give
an indication about the focus of the earnings call and the respective company.
Although these insights might be used for a plentitude of tasks, this project
centers around the following question: **How are companies effected by the
Russian invasion into the Ukraine?**




Earnings Calls
==============

Earnings calls are conference calls conducted by publicly traded companies with
their shareholders, investors, analysts, and the general public to discuss
their financial performance for a specific period - typically held quarterly.
These calls usually take place shortly after the release of the company's
quarterly financial reports.

During an earnings call, key members of the company's leadership, such as the
CEO, CFO or other executives, provide insights and analysis about the company's
financial results, operations, strategies, and any other relevant developments.
They often cover topics like revenue, net income, expenses, margins and
forward-looking guidance and plans for the upcoming quarter or year.

The earnings call is a **key source of information** for investors, as it
provides insights into the company's financial performance and future
prospects.


Data
====

The data used in this project stems from transcripts of earnings calls provided
by Refinitiv_ The data is not publicly available and has to be purchased from
`Refinitiv Events`_. 


Structure
---------

The transcripts are provided as xml-files, with each xml-file containing the
entire transcript of a single earnings call as well as some metadata, covering
an unique ID, the title of the earnings all, the company name and ticker, the
name of the city, where the earnings call was held, the date and time of the
call as well as some other technical information. The transcript itself is
structured into two three parts: 

* List of participants, the company they are working for and their respective
  position within that company. The list is divided into corporate and
  conference call participants.
* Presentation
* Q&A

Both the presentation and the Q&A session are structured into parts, which are
comprised of the speaker (name, company and position) and the corresponding
text. The presentation is held by the corporate participants. In the Q&A
session, the corporate participants answer questions from the conference call
participants. Most of the times an operator moderates the presentation as well
as the Q&A session.


.. end detailed_description


.. start references

.. _Refinitiv: https://www.refinitiv.com/en
.. _Refinitiv Events: https://www.refinitiv.com/en/financial-data/company-data/events/earnings-transcripts-briefs
.. _spaCy: https://spacy.io/
.. _BERTopic: https://maartengr.github.io/BERTopic/index.html

.. end references