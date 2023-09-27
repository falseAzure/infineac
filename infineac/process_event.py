"""
Contains functions to manipulate events and strings and extract the
corresponding information for the infineac package. For text processing it uses
the :mod:`infineac.process_text` module.

Examples
--------
>>> import infineac.process_event as process_event
>>> import infineac.file_loader as file_loader
>>> import spacy_stanza
>>> nlp = spacy_stanza.load_pipeline("en", processors="tokenize, lemma")
>>> nlp.add_pipe('sentencizer')
>>> PATH_DIR = "data/transcripts/"
>>> files = list(Path(PATH_DIR).rglob("*.xml"))
>>> events = file_loader.load_files_from_xml(files)
>>> keywords = {"russia": 1, "ukraine": 1}
>>> process_event.events_to_corpus(events=events, keywords=keywords, nlp_model=nlp)


Notes
-----
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

import os
import re
import shutil

import polars as pl
from tqdm import tqdm

import infineac.constants as constants
import infineac.process_text as process_text


def extract_passages_from_presentation(
    presentation: list[dict[str, int | str]] | None,
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    return_type: str = "list",
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
    presentation : list[dict[str, int | str]]
        Presentation part of an event.
    keywords : list[str] | dict[str, int]
        List of `keywords` to search for in the presentation and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"

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
            new_passages = process_text.extract_passages_from_paragraphs(
                paragraphs=paragraphs,
                keywords=keywords,
                nlp_model=nlp_model,
                modifier_words=modifier_words,
                context_window_sentence=context_window_sentence,
                join_adjacent_sentences=join_adjacent_sentences,
                subsequent_paragraphs=subsequent_paragraphs,
                return_type=return_type,
                keyword_n_paragraphs_above=keyword_n_paragraphs_above,
            )
            if new_passages:
                if return_type == "list":
                    passages.append(new_passages)
                if return_type == "str":
                    passages += new_passages

    return passages


def extract_passages_from_qa(  # noqa: C901
    qa: list[dict[str, int | str]],
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
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
    qa : list[dict[str, int | str]]
        Q&A part of an event.
    keywords : list[str] | dict[str, int]
        List of `keywords` to search for in the Q&A and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"

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
        new_passages = process_text.extract_passages_from_paragraphs(
            paragraphs=paragraphs,
            keywords=keywords,
            nlp_model=nlp_model,
            modifier_words=modifier_words,
            context_window_sentence=context_window_sentence,
            join_adjacent_sentences=join_adjacent_sentences,
            subsequent_paragraphs=subsequent_paragraphs,
            return_type=return_type,
            keyword_n_paragraphs_above=keyword_n_paragraphs_above,
        )

        if new_passages:
            if return_type == "list":
                passages.append(new_passages)
            if return_type == "str":
                passages += new_passages

    return passages


def check_if_keyword_align_qa(
    qa: list[dict[str, int | str]], keywords: list[str]
) -> int:
    """
    Function to check if a keyword occurs in a question and the answer to that.

    Parameters
    ----------
    qa : list[dict[str, int | str]]
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
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    sections: str = "all",
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
) -> str | list[list[list[list[str]]]]:
    """
    Wrapper function to extract important passages from an event: comprises of
    :func:`extract_passages_from_presentation` and :func:`extract_passages_from_qa`.

    Parameters
    ----------
    event : dict
        Event to extract the passages from.
    keywords : list[str] | dict[str, int]
        List of `keywords` to search for in the event and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    sections : str, default: "all"
        Section of the event to extract the passages from. Either "all",
        "presentation" or "qa"
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"

    Returns
    -------
    str | list[list[list[list[str]]]]
        The extracted passages as a concatenated string or a nested list with the
        following hierarchy: presentation and qa - parts - paragraphs -
        passages.
    """
    if sections in ["all", "presentation"]:
        presentation_extracted = extract_passages_from_presentation(
            presentation=event["presentation"],
            keywords=keywords,
            nlp_model=nlp_model,
            modifier_words=modifier_words,
            context_window_sentence=context_window_sentence,
            join_adjacent_sentences=join_adjacent_sentences,
            subsequent_paragraphs=subsequent_paragraphs,
            return_type=return_type,
        )

        # for participant in event["corp_participants"] + event["conf_participants"]:
        #     if any(keyword in participant.lower() for keyword in keywords):
        #         presentation_extracted += participant + "\n"
    else:
        presentation_extracted = ""
    if sections in ["all", "qa"]:
        qa_extracted = extract_passages_from_qa(
            qa=event["qa"],
            keywords=keywords,
            nlp_model=nlp_model,
            modifier_words=modifier_words,
            context_window_sentence=context_window_sentence,
            join_adjacent_sentences=join_adjacent_sentences,
            subsequent_paragraphs=subsequent_paragraphs,
            extract_answers=extract_answers,
            return_type=return_type,
        )
    else:
        qa_extracted = ""

    if return_type == "str":
        doc = presentation_extracted + "/n" + qa_extracted
    elif return_type == "list":
        doc = [presentation_extracted, qa_extracted]
    return doc


def extract_passages_from_events(
    events: list[dict],
    keywords: list[str] | dict[str, int],
    nlp_model,
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    sections: str = "all",
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
) -> list[str] | list[list[list[list[list[str]]]]]:
    """
    Wrapper function to extract important paragraphs from a list of events.
    Loops over all events and calls :func:`extract_passages_from_event`.

    Parameters
    ----------
    events : list[dict]
         Lists of dicts containing the events.
    keywords : list[str] | dict[str, int]
        List of `keywords` to search for in the events and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    nlp_model : spacy.lang
        NLP model.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    sections : str, default: "all"
        Section of the event to extract the passages from. Either "all",
        "presentation" or "qa".
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
            Whether to join adjacent sentences or leave them as individual. If
            `context_window_sentence` > 0, this parameter is automatically set
            to `True`.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"

    Returns
    -------
    str | list[list[list[list[list[str]]]]]
        The extracted passages as a list of strings or a nested list with the
        following hierarchy: event - presentation and qa - parts - paragraphs -
        passages.
    """
    print("Extracting passages from events")
    docs = []
    for event in tqdm(events, desc="Events", total=len(events)):
        docs.append(
            extract_passages_from_event(
                event=event,
                keywords=keywords,
                nlp_model=nlp_model,
                modifier_words=modifier_words,
                sections=sections,
                context_window_sentence=context_window_sentence,
                join_adjacent_sentences=join_adjacent_sentences,
                subsequent_paragraphs=subsequent_paragraphs,
                extract_answers=extract_answers,
                return_type=return_type,
            )
        )
    return docs


def check_keywords_in_event(
    event: dict,
    keywords: list[str] | dict[str, int] = [],
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> bool:
    """
    Function to check if keywords are present in the presentation or Q&A part
    of an event. Calls :func:`infineac.process_text.keyword_search_exclude_threshold`.
    """
    return process_text.keyword_threshold_search_exclude_mod(
        str(event["qa_collapsed"] + event["presentation_collapsed"]),
        keywords,
        modifier_words,
    )


def filter_events(
    events: list[dict],
    year: int = constants.BASE_YEAR,
    keywords: list[str] | dict[str, int] = [],
    modifier_words: list[str] = constants.MODIFIER_WORDS,
) -> list[dict]:
    """
    Filters events based on a given `year` and `keywords`.

    Parameters
    ----------
    events : list[dict]
       Lists of dicts containing the events.
    year : int, default: constants.BASE_YEAR
        All events before the given year are filtered out.
    keywords : list[str] | dict[str, int], default: []
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


def excluded_sentences_by_mod_words(events, keywords, nlp_model):
    """Extracts the sentences that are excluded by the modifier words. Calls
    on :func:`infineac.process_text.extract_keyword_sentences_preceding_mod`."""
    excluded_sentences = []
    for event in tqdm(events, desc="Events", total=len(events)):
        excluded_qa = process_text.extract_keyword_sentences_preceding_mod(
            text=event["qa_collapsed"], keywords=keywords, nlp_model=nlp_model
        )
        excluded_presentation = process_text.extract_keyword_sentences_preceding_mod(
            text=event["presentation_collapsed"],
            keywords=keywords,
            nlp_model=nlp_model,
        )
        excluded_sentences.append(excluded_qa + excluded_presentation)
    return [sent for lst in excluded_sentences for sent in lst if lst != []]


def test_positions(events: list[dict]):
    """Checks if all positions of the speakers of the given `events` are valid."""
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


def create_participants_to_remove(event: dict) -> list[str]:
    """Creates a list containing the names of the participants of an `event` to
    be later removed during the text processing."""
    names = []
    for participant in event["corp_participants"] + event["conf_participants"]:
        names += participant["name"].split(" ")
    return names


def corpus_list_to_dataframe(corpus: list[list[list[list[list[str]]]]]) -> pl.DataFrame:
    """Converts a `corpus` (nested list of texts) to a polars DataFrame with
    indices, indicating the position of the texts in the corpus: event -
    presentation or qa - part - paragraph - sentence."""

    indices = []
    for event_idx, event in enumerate(corpus):
        for presentation_and_qa_idx, presentation_and_qa in enumerate(event):
            for part_idx, part in enumerate(presentation_and_qa):
                for paragraph_idx, paragraph in enumerate(part):
                    for sentence_idx, sentence in enumerate(paragraph):
                        indices.append(
                            {
                                "event_idx": event_idx,
                                "presentation_and_qa_idx": presentation_and_qa_idx,
                                "part_idx": part_idx,
                                "paragraph_idx": paragraph_idx,
                                "sentence_idx": sentence_idx,
                                "text": sentence,
                            }
                        )
    return pl.DataFrame(indices)


def extract_infos_from_events(events: list[dict]) -> pl.DataFrame:
    """Extracts the id, year, date and company name from a list of events."""
    ids = []
    years_upload = []
    dates = []
    company_names = []
    event_idx = []
    for idx, event in enumerate(events):
        event_idx.append(idx)
        ids.append(event["id"])
        years_upload.append(event["year_upload"])
        dates.append(event["date"])
        company_names.append(event["company_name"])
    return pl.DataFrame(
        {
            "event_idx": event_idx,
            "id": ids,
            "years_upload": years_upload,
            "date": dates,
            "company_name": company_names,
        }
    )


def events_to_corpus(
    events: list[dict],
    nlp_model,
    keywords: list[str] | dict[str, int] = [],
    modifier_words: list[str] = constants.MODIFIER_WORDS,
    sections: str = "all",
    context_window_sentence: tuple[int, int] | int = 0,
    join_adjacent_sentences: bool = True,
    subsequent_paragraphs: int = 0,
    extract_answers: bool = False,
    return_type: str = "list",
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_keywords: bool = True,
    remove_names: bool = True,
    remove_strategies: bool | dict[str, list[str]] = True,
    remove_additional_stopwords: bool | list[str] = True,
) -> pl.DataFrame:
    """
    Converts a list of events to a corpus (list of texts).

    This is a wrapper function that calls :func:`extract_passages_from_events`,
    :func:`corpus_list_to_dataframe` and
    :func:`infineac.process_text.process_corpus`. This function is used to
    extract the corpus from the events and process it with the
    :mod:`infineac.process_text` module according to the given parameters.

    Parameters
    ----------
    events : list[dict]
         Lists of dicts containing the events.
    nlp_model : spacy.lang, default: None
        NLP model. lemmatize : bool, default: True If document should be
        lemmatized.
    keywords : list[str] | dict[str, int], default: []
        List of `keywords` to search for in the events and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    sections : str, default: "all"
        Section of the event to extract the passages from. Either "all",
        "presentation" or "qa"
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
        Whether to join adjacent sentences.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    lowercase : bool, default: True
        If document should be lowercased.
    remove_stopwords : bool, default: True
        If stopwords should be removed from document.
    remove_punctuation : bool, default: True
        If punctuation should be removed from document.
    remove_numeric : bool, default: False
        If numerics should be removed from document.
    remove_currency : bool, default: True
        If currency symbols should be removed from document.
    remove_space : bool, default: True
        If spaces should be removed from document.
    remove_keywords: bool, default: True
        If keywords should be removed from document.
    remove_names : bool, default: True
        If participant names should be removed from document.
    remove_strategies : bool | dict[str, list[str]], default: True
        If the strategy keywords should be removed from document.
    remove_additional_stopwords : bool | list[str], default: True
        If additional stopwords should be removed from document.

    Returns
    -------
    pl.DataFrame
        The corpus as a polars DataFrame with indices, indicating the position
        of the texts in the corpus: event - presentation or qa - part -
        paragraph - sentence, the original text and the processed text.
    """
    corpus_raw = extract_passages_from_events(
        events=events,
        keywords=keywords,
        nlp_model=nlp_model,
        modifier_words=modifier_words,
        sections=sections,
        context_window_sentence=context_window_sentence,
        join_adjacent_sentences=join_adjacent_sentences,
        subsequent_paragraphs=subsequent_paragraphs,
        extract_answers=extract_answers,
        return_type=return_type,
    )
    corpus_df = corpus_list_to_dataframe(corpus_raw)
    corpus_raw_list = corpus_df["text"].to_list()

    remove_additional_words_part = []
    if remove_keywords is True:
        if type(keywords) == list:
            remove_additional_words_part += keywords
        if type(keywords) == dict:
            remove_additional_words_part += list(keywords.keys())
    if remove_additional_stopwords:
        if remove_additional_stopwords is True:
            remove_additional_words_part += constants.ADDITIONAL_STOPWORDS
        else:
            remove_additional_words_part += remove_additional_stopwords
    if remove_strategies:
        if remove_strategies is True:
            remove_additional_words_part += process_text.strategy_keywords_tolist(
                constants.STRATEGY_KEYWORDS
            )
        else:
            remove_additional_words_part += process_text.strategy_keywords_tolist(
                remove_strategies
            )

    remove_names_list = []
    if remove_names is True:
        for idx in corpus_df["event_idx"].to_list():
            print(idx, end="\r")
            remove_names_list.append(create_participants_to_remove(events[idx]))
    assert len(remove_names_list) == len(corpus_df)

    docs = process_text.process_corpus(
        corpus=corpus_raw_list,
        nlp_model=nlp_model,
        lemmatize=lemmatize,
        lowercase=lowercase,
        remove_stopwords=remove_stopwords,
        remove_punctuation=remove_punctuation,
        remove_numeric=remove_numeric,
        remove_currency=remove_currency,
        remove_space=remove_space,
        remove_additional_words_part=remove_additional_words_part,
        remove_specific_stopwords=remove_names_list,
    )
    docs_joined = [process_text.list_to_string(doc) for doc in docs]

    corpus_df = corpus_df.with_columns(pl.Series("processed_text", docs_joined))
    info_df = extract_infos_from_events(events)

    corpus_df = corpus_df.join(info_df, on="event_idx")

    return corpus_df


def create_samples(df):
    """Creates 15 samples for keyword 'russia'."""
    if len(df[df["russia"] == "russia & sanctions"]) > 0:
        sample_files_russia = (
            df[df["russia"] == "russia"].sample(8)["file"].tolist()
            + df[df["russia"] == "russia & sanctions"].sample(7)["file"].tolist()
        )

    folder = "../output/sample transcripts/russia/"
    files = os.listdir(folder)
    for f in files:
        os.remove(folder + f)
    # copy sample files to folder
    for file in sample_files_russia:
        shutil.copy(file, folder)
