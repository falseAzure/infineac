"""
This module contains functions to manipulate events and strings and extract the
corresponding information for the infineac package. For text processing it uses
the :mod:`infineac.process_text` module.

An event is a dictionary with the following key-value pairs:
    - 'file': str - the file name
    - 'year_upload': integer - the year of the upload
    - 'corp_participants': list[list[str]] - the corporate participants
    - 'corp_participants_collapsed': list[str] - collapsed list
    - 'conf_participants': list[list[str]] - the conference call participants
    - 'conf_participants_collapsed': list[str] - collapsed list
    - 'presentation': list[dict] - the presentation part
    - 'presentation_collapsed': list[str] - collapsed list
    - 'qa': list[dict] - the Q&A part
    - 'qa_collapsed': list[str] - collapsed list
    - 'action': str - the action (e.g. publish)
    - 'story_type': str - the story type (e.g. transcript)
    - 'version': str - the version of the publication (e.g. final)
    - 'title': str - the title of the earnings call
    - 'city': str - the city of the earnings call
    - 'company_name': str - the company of the earnings call
    - 'company_ticker': str - the company ticker of the earnings call
    - 'date': date - the date of the earnings call
    - 'id': int - the id of the publication
    - 'last_update': date - the last update of the publication
    - 'event_type_id': int - the event type id
    - 'event_type_name': str - the event type name
"""

import re

import polars as pl
from tqdm import tqdm

import infineac.process_text as process_text
from infineac.process_text import MODIFIER_WORDS, extract_passages_from_paragraphs


def extract_passages_from_presentation(
    presentation: list[dict],
    keywords: list[str] | dict,
    modifier_words: list[str] = MODIFIER_WORDS,
    context_window_sentence: int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    return_type: str = "list",
    nlp=None,
) -> str | list[list[list[str]]]:
    """
    Extracts important passages from the presentation section of an event.

    Importance of a passage is determined by the presence of one of the
    `keywords`. If a keyword occurs in a paragraph, the sentence containing it
    and the context surrounding it (`context_window_sentence`) are extracted.
    Additionally, `window_subsequent` paragraphs are extracted. (Like
    :func:`extract_passages_from_qa` but for the presentation section of an
    event.)

    Parameters
    ----------
    presentation : list[dict]
        Presentation part of an event.
    keywords : list[str] | dict
        List of `keywords` to search for in the presentation and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[list[list[str]]]
        The extracted passages as a concatenated string or list of lists
        (passages) of lists (paragraphs) of sentences.
    """
    if return_type == "str":
        passages = ""
    elif return_type == "list":
        passages = []
    else:
        return False

    if presentation is None:
        return passages

    keyword_n_paragraphs_above = -1
    for part in presentation:
        if part["position"] != "cooperation":
            continue
        else:
            paragraphs = re.split("\n", part["text"])
            new_passages = extract_passages_from_paragraphs(
                paragraphs,
                keywords,
                modifier_words,
                context_window_sentence,
                join_adjacent_sentences,
                subsequent_paragraphs,
                return_type,
                keyword_n_paragraphs_above,
                nlp,
            )
            if new_passages:
                if return_type == "list":
                    passages.append(new_passages)
                if return_type == "str":
                    passages += new_passages

    return passages


def extract_passages_from_qa(
    qa: list[dict],
    keywords: list[str] | dict,
    modifier_words: list[str] = MODIFIER_WORDS,
    context_window_sentence: int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
    nlp=None,
) -> str | list[list[list[str]]]:
    """
    Extracts important passages, like :func:`extract_passages_from_presentation`, but
    for the Q&A section of an event.

    Importance of a passage is determined by the presence of one of the
    `keywords`. If a keyword occurs in a paragraph of an answer, the sentence
    containing it and the context surrounding it (`context_window_sentence`)
    are extracted. Additionally, `window_subsequent` paragraphs are extracted.
    If `extract_answers` is set to True, the entire answer is extracted, if the
    prior question contains a keyword.

    Parameters
    ----------
    qa : list[dict]
        Q&A part of an event.
    keywords : list[str] | dict
        List of `keywords` to search for in the Q&A and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[list[list[str]]]
        The extracted passages as a concatenated string or list of lists (passages)
        of lists (paragraphs) of sentences.
    """
    if return_type == "str":
        passages = ""
    elif return_type == "list":
        passages = []
    else:
        return False

    if qa is None:
        return passages

    previous_question_has_keyword = False
    keyword_n_paragraphs_above = -1
    for part in qa:
        if part["position"] in ["operator", "editor"]:
            continue

        # conference participants and others (unidentified, unknown
        # etc.)
        if part["position"] != "cooperation":
            previous_question_has_keyword = False
            if any(keyword in part["text"].lower() for keyword in keywords):
                previous_question_has_keyword = True
            keyword_n_paragraphs_above = -1
            continue

        # cooperation
        if previous_question_has_keyword and extract_answers:
            if return_type == "str":
                passages += part["text"] + "\n"
            if return_type == "list":
                passages.append([[part["text"]]])
            continue

        paragraphs = re.split("\n", part["text"])
        new_passages = extract_passages_from_paragraphs(
            paragraphs,
            keywords,
            modifier_words,
            context_window_sentence,
            join_adjacent_sentences,
            subsequent_paragraphs,
            return_type,
            keyword_n_paragraphs_above,
            nlp,
        )
        if new_passages:
            if return_type == "list":
                passages.append(new_passages)
            if return_type == "str":
                passages += new_passages

    return passages


def check_if_keyword_align_qa(qa: list[dict], keywords: list[str]) -> int:
    """
    Function to check if a keyword occurs in a question and the answer to that.

    Parameters
    ----------
    qa : list[dict]
        Q&A section of an event.
    keywords : list[str]
        Keywords to check for.

    Returns
    -------
    int
        Number of times a keyword occurs in a question and NOT in the answer to
        that.
    """
    n_only_qa_uses_keyword = 0
    if qa is None:
        return n_only_qa_uses_keyword
    previous_question_keyword = False
    question_and_answer_use_keyword = True
    for part in qa:
        # conference participants and others (operator, unidentified, unknown etc.)
        if part["position"] != "cooperation":
            # previous_question = part["text"]
            if not question_and_answer_use_keyword:
                # print(previous_question)
                n_only_qa_uses_keyword = +1
            if any(keyword in part["text"].lower() for keyword in keywords):
                previous_question_keyword = True
                question_and_answer_use_keyword = False
            continue

        # cooperation
        if previous_question_keyword:
            if any(keyword in part["text"].lower() for keyword in keywords):
                question_and_answer_use_keyword = True

    return n_only_qa_uses_keyword


def extract_passages_from_event(
    event: dict,
    keywords: list[str] | dict,
    modifier_words: list[str] = MODIFIER_WORDS,
    context_window_sentence: int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
    nlp=None,
) -> str | list[list[list[list[str]]]]:
    """
    Wrapper function to extract important passages from an event: comprises of
    :func:`extract_passages_from_presentation` and :func:`extract_passages_from_qa`.

    Parameters
    ----------
    keywords : list[str] | dict
        List of `keywords` to search for in the event and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[list[list[list[str]]]]
        The extracted passages as a concatenated string or a nested list with the
        following hierarchy: presentation and qa - parts - paragraphs -
        passages.
    """
    presentation_extracted = extract_passages_from_presentation(
        event["presentation"],
        keywords,
        modifier_words,
        context_window_sentence,
        join_adjacent_sentences,
        subsequent_paragraphs,
        return_type,
        nlp,
    )
    qa_extracted = extract_passages_from_qa(
        event["qa"],
        keywords,
        modifier_words,
        context_window_sentence,
        join_adjacent_sentences,
        subsequent_paragraphs,
        extract_answers,
        return_type,
        nlp,
    )
    if return_type == "str":
        doc = presentation_extracted + "/n" + qa_extracted
    elif return_type == "list":
        doc = [presentation_extracted, qa_extracted]
    return doc


def extract_passages_from_events(
    events: list[dict],
    keywords: list[str] | dict,
    modifier_words: list[str] = MODIFIER_WORDS,
    context_window_sentence: int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
    nlp=None,
) -> list[str] | list[list[list[list[list[str]]]]]:
    """
    Wrapper function to extract important paragraphs from a list of events.
    Loops over all events and calls :func:`extract_passages_from_event`.

    Parameters
    ----------
    keywords : list[str] | dict
        List of `keywords` to search for in the events and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : list[int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a list of length 2. The first element of the list indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element the number of sentences after it. If
        only an integer is provided, the same number of sentences are extracted
        before and after the keyword. If one of the elements is -1, all
        sentences before or after the keyword are extracted. So -1 can be used
        to extract all sentences before and after the keyword, e.g. the entire
        text.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    nlp : spacy.lang, default: None
        NLP model.

    Returns
    -------
    str | list[list[list[list[list[str]]]]]
        The extracted passages as a list of strings or a nested list with the
        following hierarchy: event - presentation and qa - parts - paragraphs -
        passages.
    """
    print("Extracting paragraphs from events")
    docs = []
    for event in tqdm(events, desc="Events", total=len(events)):
        docs.append(
            extract_passages_from_event(
                event,
                keywords,
                modifier_words,
                context_window_sentence,
                join_adjacent_sentences,
                subsequent_paragraphs,
                extract_answers,
                return_type,
                nlp,
            )
        )
    return docs


def check_keywords_in_event(
    event: dict,
    keywords: dict[str, int] | list[str] = {},
    modifier_words: list[str] = MODIFIER_WORDS,
) -> bool:
    """
    Function to check if keywords are present in the presentation or Q&A part
    of an event. Calls :func:`process_text.keyword_search_exclude_threshold`.
    """
    return process_text.keyword_search_exclude_threshold(
        str(event["qa_collapsed"] + event["presentation_collapsed"]),
        keywords,
        modifier_words,
    )


def filter_events(
    events: list[dict],
    year: int = 2022,
    keywords: dict[str, int] | list[str] = {},
    modifier_words: list[str] = MODIFIER_WORDS,
) -> list[dict]:
    """
    Filters events based on a given `year` and `keywords`.

    Parameters
    ----------
    events : list[dict]
       Lists of dicts containing the events.
    year : int, default: 2022
        All events before the given year are filtered out.
    keywords : dict[str, int] | list[str], default: {}
        Dictionary or list of `keywords`. If `keywords` is a dictionary, the key is
        the keyword and the value is the minimum number of occurrences of the
        keyword in the text.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword

    Returns
    -------
    list[dict]
        Filtered events.
    """
    print("Filtering events")
    events_filtered = []
    for event in tqdm(events, desc="Events", total=len(events)):
        if not (
            "date" in event.keys()
            and event["date"].year >= year
            and event["action"] == "publish"
            and event["version"] == "Final"
        ):
            continue

        if not check_keywords_in_event(event, keywords, modifier_words):
            continue

        events_filtered.append(event)

    return events_filtered


def test_positions(events: list[dict]):
    """Checks if all positions of the speakers of an event are valid."""
    positions = []
    for i, event in enumerate(events):
        if event["qa"] is not None:
            for speaker in event["qa"]:
                if speaker["position"] not in [
                    "conference",
                    "cooperation",
                    "operator",
                    "unknown participant",
                ]:
                    positions.append("" + str(i) + ": " + speaker["position"])


def corpus_list_to_dataframe(corpus: list[list[list[list[list[str]]]]]) -> pl.DataFrame:
    indices = []
    for event_idx, event in enumerate(corpus):
        for presentation_and_qa_idx, presentation_and_qa in enumerate(event):
            for part_idx, part in enumerate(presentation_and_qa):
                for paragraph_idx, paragraph in enumerate(part):
                    for sentence_idx, sentence in enumerate(paragraph):
                        indices.append(
                            {
                                "event": event_idx,
                                "presentation_and_qa": presentation_and_qa_idx,
                                "part": part_idx,
                                "paragraph": paragraph_idx,
                                "sentence": sentence_idx,
                                "text": sentence,
                            }
                        )
    return pl.DataFrame(indices)


def events_to_corpus(
    events: list[dict],
    keywords: list[str] | dict,
    modifier_words: list[str] = MODIFIER_WORDS,
    context_window_sentence: int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    nlp_model=None,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_additional_words: list[str] | bool = [],
) -> list[str]:
    """Converts a list of events to a corpus (list of texts)"""
    corpus_raw = extract_passages_from_events(
        events,
        keywords,
        modifier_words,
        context_window_sentence,
        join_adjacent_sentences,
        subsequent_paragraphs,
        extract_answers,
        "list",
        nlp_model,
    )
    corpus_df = corpus_list_to_dataframe(corpus_raw)
    corpus_raw_list = corpus_df["text"].to_list()

    if remove_additional_words:
        if type(keywords) == list:
            remove_additional_words = keywords
        if type(keywords) == dict:
            remove_additional_words = list(keywords.keys())

    docs = process_text.process_corpus(
        corpus_raw_list,
        nlp_model,
        lemmatize,
        lowercase,
        remove_stopwords,
        remove_punctuation,
        remove_numeric,
        remove_currency,
        remove_space,
        remove_additional_words,
    )
    docs_joined = [process_text.list_to_string(doc) for doc in docs]

    corpus_df = corpus_df.with_columns(pl.Series("processed_text", docs_joined))

    return corpus_df
