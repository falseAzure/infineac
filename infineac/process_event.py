"""
This file contains functions to manipulate events and strings and extract the
corresponding information for the infineac package.

An event is a dictionary with the following keys:
    - file (string): the file name
    - year_upload (integer): the year of the upload
    - corp_participants (list of lists): the corporate participants
    - corp_participants_collapsed (list): collapsed list
    - conf_participants (list of lists): the conference call participants
    - conf_participants_collapsed (list): collapsed list
    - presentation (list of dicts): the presentation part
    - presentation_collapsed (list): collapsed list
    - qa (list of dicts): the Q&A part
    - qa_collapsed (list): collapsed list
    - action (string): the action (e.g. publish)
    - story_type (string): the story type (e.g. transcript)
    - version (string): the version of the publication (e.g. final)
    - title (string): the title of the earnings call
    - city (string): the city of the earnings call
    - company_name (string): the company of the earnings call
    - company_ticker (string): the company ticker of the earnings call
    - date (date): the date of the earnings call
    - id (int): the id of the publication
    - last_update (date): the last update of the publication
    - event_type_id (int): the event type id
    - event_type_name (string): the event type name
"""

import re

from tqdm import tqdm

import infineac.process_text as process_text


def loop_through_paragraphs(
    paragraphs: list,
    keywords: dict,
    subsequent_paragraphs: int,
    output_type: str = "str",
    part_type: str = "paragraph",
    keyword_n_paragraphs_above: int = -1,
    nlp=None,
) -> str | list:
    """
    Function to loop through paragraphs and extract the ones that contain a
    keyword. If a keyword occurs in a paragraph, either the entire paragraph or
    the part beginning with the sentence in which the keyword was found is
    extracted. Additionally, n subsequent paragraphs are extracted.


    Args:
        paragraphs (list): List of paragraphs to loop through.
        keywords (dict): List of keywords to determine importance.
        subsequent_paragraphs (int, optional): Number of subsequent paragraphs to
        extract. Defaults to 0.
        output_type (str): Type of output. Either "str" or "list". Defaults to
        "list".
        part_type (str, optional): Type of part to extract. Either "paragraph"
        or "part". Defaults to "paragraph".
        key_n_paragraphs_above (int): Number of paragraphs above the current
        paragraph. Defaults to -1.
        nlp(spacy.lang, optional): Spacy language model. Defaults to None.

    Raises:
        ValueError: If output_type is not "str" or "list".
        ValueError: If part_type is not "paragraph" or "part".

    Returns:
        str | list: The extracted parts as a concatenated string or list of
        strings.
    """
    if output_type not in ["str", "list"]:
        raise ValueError("output_type must be either str or list")
    if part_type not in ["paragraph", "part"]:
        raise ValueError("part_type must be either paragraph or part")

    if output_type == "str":
        parts_out = ""
    elif output_type == "list":
        parts_out = []

    for paragraph in paragraphs:
        if any(keyword in paragraph.lower() for keyword in keywords):
            keyword_n_paragraphs_above = 0
            if part_type == "paragraph":
                part = paragraph
            elif part_type == "part":
                part = get_sentences_after_keywords(paragraph, keywords, nlp)

            if output_type == "list":
                parts_out.append(part)
            elif output_type == "str":
                parts_out += part + "\n"
        elif (
            keyword_n_paragraphs_above >= 0
            and keyword_n_paragraphs_above <= subsequent_paragraphs
        ):
            if output_type == "list":
                parts_out.append(paragraph)
            elif output_type == "str":
                parts_out += paragraph + "\n"
        if keyword_n_paragraphs_above != -1:
            keyword_n_paragraphs_above += 1
    return parts_out


def extract_parts_from_presentation(
    presentation: list,
    keywords: dict,
    subsequent_paragraphs: int = 0,
    output_type: str = "list",
    part_type: str = "paragraph",
    nlp=None,
) -> str | list:
    """
    Method to extract important paragraphs or parts from the presentation section
    of an event. Importance of a paragraph/part is determined by the presence
    of a keyword. If a keyword occurs in a paragraph, either the entire
    paragraph or the part beginning with the sentence in which the keyword was
    found is extracted.  Additionally, n subsequent paragraphs are extracted.

    Args:
        presentation (list): List of dicts containing the presentation part.
        keywords (dict): List of keywords to determine importance.
        subsequent_paragraphs (int, optional): Number of subsequent paragraphs to
        extract. Defaults to 0.
        output_type (str): Type of output. Either "str" or "list". Defaults to
        "list".
        part_type (str, optional): Type of part to extract. Either "paragraph"
        or "part". Defaults to "paragraph".
        nlp(spacy.lang, optional): Spacy language model. Defaults to None.

    Returns:
        str | list: The extracted parts as a concatenated string or list of
        strings.
    """
    if output_type == "str":
        parts = ""
    elif output_type == "list":
        parts = []
    else:
        return False

    if presentation is None:
        return parts

    keyword_n_paragraphs_above = -1
    for part in presentation:
        if part["position"] != "cooperation":
            continue
        else:
            paragraphs = re.split("\n", part["text"])
            new_parts = loop_through_paragraphs(
                paragraphs,
                keywords,
                subsequent_paragraphs,
                output_type,
                part_type,
                keyword_n_paragraphs_above,
                nlp,
            )
            if new_parts:
                parts += new_parts

    return parts


def extract_parts_from_qa(
    qa: list,
    keywords: dict,
    subsequent_paragraphs: int = 0,
    output_type: str = "list",
    part_type: str = "paragraph",
    nlp=None,
) -> str | list:
    """
    Method to extract important parts from the Q&A section of an event.
    Importance of a paragraph/part is determined by the presence
    of a keyword. If a keyword is present in a question the entire answer referring to
    that question is extracted. If a keyword occurs in an answer (without being
    posed in the question), either the entire paragraph or the part beginning
    with the sentence in which the keyword was found is extracted.
    Additionally, n subsequent paragraphs are extracted.


    Args:
        presentation (list): List of dicts containing the presentation part.
        keywords (dict): List of keywords to determine importance.
        subsequent_paragraphs (int, optional): Number of subsequent paragraphs to
        extract. Defaults to 0.
        output_type (str): Type of output. Either "str" or "list". Defaults to
        "list".
        part_type (str, optional): Type of part to extract. Either "paragraph"
        or "part". Defaults to "paragraph".
        nlp(spacy.lang, optional): Spacy language model. Defaults to None.

    Returns:
        str | list: The extracted parts as a concatenated string or list of
        strings.
    """
    if output_type == "str":
        parts = ""
    elif output_type == "list":
        parts = []
    else:
        return False

    if qa is None:
        return parts

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
        if previous_question_has_keyword:
            if output_type == "str":
                parts += part["text"] + "\n"
            if output_type == "list":
                parts.append(part["text"])
            continue

        paragraphs = re.split("\n", part["text"])
        new_parts = loop_through_paragraphs(
            paragraphs,
            keywords,
            subsequent_paragraphs,
            output_type,
            part_type,
            keyword_n_paragraphs_above,
            nlp,
        )
        if new_parts:
            parts += new_parts

    return parts


def get_sentences_after_keywords(text: str, keywords: dict = {}, nlp=None) -> str:
    """
    Method to extract the sentences after a keyword in a text.

    Args:
        text (str): The text to extract the sentences from.
        keywords (dict, optional): The keywords starting from which the
        sentences are extracted. Defaults to {}.
        nlp (Spacy.lang, optional): NLP model. Defaults to None.

    Returns:
        str: The extracted sentences.
    """
    if nlp is None:
        return None
    if str == "":
        return ""
    if not any(keyword in text.lower() for keyword in keywords):
        return ""

    doc = nlp(text)
    sentences = list(doc.sents)
    for idx, sent in enumerate(sentences):
        if any(keyword in sent.text.lower() for keyword in keywords):
            start_idx = idx
            break
    matching_sentences = sentences[start_idx:]
    part = " ".join([sentence.text for sentence in matching_sentences])
    return part


def check_if_keyword_align_qa(qa: list, keywords: dict) -> int:
    """
    Function to check if a keyword occurs in a question and the answer to that.

    Args:
        qa (list): Q&A section of an event.
        keywords (dict): Keywords to check for.

    Returns:
        int: Number of times a keyword occurs in a question and NOT the answer
        to that.
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


def extract_parts_from_event(
    event: dict,
    keywords: dict,
    subsequent_paragraphs: int = 0,
    output_type: str = "list",
    part_type: str = "paragraph",
    nlp=None,
) -> str | list:
    """
    Wrapper function to extract important paragraphs from an event.

    Args:
        presentation (list): List of dicts containing the presentation part.
        keywords (dict): List of keywords to determine importance.
        subsequent_paragraphs (int, optional): Number of subsequent paragraphs to
        extract. Defaults to 0.
        output_type (str): Type of output. Either "str" or "list". Defaults to
        "list".
        part_type (str, optional): Type of part to extract. Either "paragraph"
        or "part". Defaults to "paragraph".
        nlp(spacy.lang, optional): Spacy language model. Defaults to None.

    Returns:
        str | list: The extracted parts as a concatenated string or list of
        strings.
    """
    presentation_extracted = extract_parts_from_presentation(
        event["presentation"],
        keywords,
        subsequent_paragraphs,
        output_type,
        part_type,
        nlp,
    )
    qa_extracted = extract_parts_from_qa(
        event["qa"], keywords, subsequent_paragraphs, output_type, part_type, nlp
    )
    if output_type == "str":
        doc = presentation_extracted + "/n" + qa_extracted
    elif output_type == "list":
        doc = [presentation_extracted, qa_extracted]
    return doc


def extract_parts_from_events(
    events: list,
    keywords: dict,
    subsequent_paragraphs: int = 0,
    output_type: str = "list",
    part_type: str = "paragraph",
    nlp=None,
) -> list:
    """
    Wrapper function to extract important paragraphs from a list of events.

    Args:
        events (list): List of dicts containing the events.
        keywords (dict): List of keywords to determine importance.
        subsequent_paragraphs (int, optional): Number of subsequent paragraphs to
        extract. Defaults to 0.
        output_type (str): Type of output. Either "str" or "list". Defaults to
        "list".
        part_type (str, optional): Type of part to extract. Either "paragraph"
        or "part". Defaults to "paragraph".
        nlp(spacy.lang, optional): Spacy language model. Defaults to None.

    Returns:
        list: The extracted parts as a list of strings or list.
    """
    print("Extracting paragraphs from events")
    docs = []
    for event in tqdm(events, desc="Events", total=len(events)):
        docs.append(
            extract_parts_from_event(
                event,
                keywords,
                subsequent_paragraphs,
                output_type,
                part_type,
                nlp,
            )
        )
    return docs


def check_keywords_in_event(event: dict, keywords: dict = {}) -> bool:
    """
    Function to check if keywords are present in the presentation or
    Q&A part of an event.

    Args:
        event (dict): Dict containing the event.
        keywords (dict, optional): Dict of keywords, where the key is the
        keyword and the value is the number of appearances of that keyword.
        Defaults to {}.

    Returns:
        bool: True if keywords are present, False otherwise.
    """
    return process_text.check_keywords_in_string(
        string=str(event["qa_collapsed"] + event["presentation_collapsed"]),
        keywords=keywords,
    )


def filter_events(events: list, year: int = 2022, keywords: dict = {}) -> list:
    """
    Method to filter events based on the year and keywords.
    All events before the given year are filtered out.
    All events that do not contain the keywords in
    the presentation and Q&Q part are filtered out.

    Args:
        events (list): List of dicts containing the events.
        year (int, optional): Year. Defaults to 2022.
        keywords (dict, optional): Dict of keywords, where the key is the
        keyword and the value is the number of appearances of that keyword.
        Defaults to {}.

    Returns:
        list: List of filtered events.
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

        if not check_keywords_in_event(event, keywords):
            continue

        events_filtered.append(event)

    return events_filtered
