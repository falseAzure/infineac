########
InFineac
########

.. start short_desc

**Extracting Financial Insights from Earnings Calls using NLP.**

.. end short_desc

.. contents::

.. toctree::
    :maxdepth: 2

    docs/index


Overview
********

InFineac is a Python package that extracts financial insights from earnings
calls using NLP. Earnings calls are a rich source of information for investors,
that are held quarterly by publicly traded companies. 
It uses the spaCy_ library for NLP


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

The data used in this project stems from transcript of earnings calls provided
by Refinitiv_ The data is not publicly available and has to be purchased from
`Refinitiv Events`_.


Structure
---------

The data is provided in .xml format, with each xml-file containing the
transcript of a single earnings call as well as some metadata, like the title,
city, company name and date of the call. The transcript itself is structured
into two three parts: 

* Participants (corporate and conference call participants)
* Presentation
* Q&A

Both the presentation and the Q&A session are structured into
sections, which are comprised of the speaker and the corresponding text. The
presentation is held by the corporate participants. In the Q&A session, the
corporate participants answer questions from the conference call participants.
An operator moderates the presentation as well as the Q&A session.




.. _Refinitiv: https://www.refinitiv.com/en
.. _Refinitiv Events: https://www.refinitiv.com/en/financial-data/company-data/events/earnings-transcripts-briefs
.. _spaCy: https://spacy.io/

