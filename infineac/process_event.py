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


def extract_paragraphs_from_presentation(presentation: list, keywords: list) -> str:
    """
    Method to extract important paragraphs from
    the presentation part of an event.
    Importance of a paragraph is determined by the presence of a keyword.
    If a keyword is present in a paragraph the whole paragraph
    as well as the subsequent one is extracted.

    Args:
        presentation (list): List of dicts containing the presentation part.
        keywords (list): List of keywords to determine importance.

    Returns:
        str: The extracted parts as a concatenated string.
    """
    whole_text = ""
    if presentation is None:
        return whole_text
    previous_paragraph_keyword = False
    for part in presentation:
        if part["name"] == "Operator" or part["position"] == "operator":
            continue
        else:
            paragraphs = re.split("\n", part["text"])
            for paragraph in paragraphs:
                if any(keyword in paragraph.lower() for keyword in keywords):
                    whole_text += paragraph + "\n"
                    previous_paragraph_keyword = True
                elif previous_paragraph_keyword:
                    whole_text += paragraph + "\n"
                    previous_paragraph_keyword = False

    return whole_text


def extract_paragraphs_from_qa(qa: list, keywords: list) -> str:
    """
    Method to extract important paragraphs from the Q&A part of an event.
    Importance of a paragraph is determined by the presence of a keyword.
    If a keyword is present in a paragraph the whole paragraph
    as well as the subsequent one is extracted.

    Args:
        qa (list): List of dicts containing the Q&A part.
        keywords (list): List of keywords to determine importance.

    Returns:
        str: The extracted parts as a concatenated string.
    """
    whole_text = ""
    if qa is None:
        return whole_text
    previous_question_keyword = False
    previous_paragraph_keyword = False
    for part in qa:
        # conference participants and others (operator, unidentified, unknown etc.)
        if part["position"] != "cooperation":
            previous_question_keyword = False
            if any(keyword in part["text"].lower() for keyword in keywords):
                previous_question_keyword = True
            continue

        # cooperation
        if previous_question_keyword:
            whole_text += part["text"] + "\n"
            continue

        paragraphs = re.split("\n", part["text"])
        for paragraph in paragraphs:
            if any(keyword in paragraph.lower() for keyword in keywords):
                whole_text += paragraph + "\n"
                previous_paragraph_keyword = True
            elif previous_paragraph_keyword:
                whole_text += paragraph + "\n"
                previous_paragraph_keyword = False

    return whole_text


def extract_paragraphs_from_event(event: dict, keywords: list) -> str:
    """
    Method to extract important paragraphs from an event.

    Args:
        event (dict): Dict containing the event.
        keywords (list): List of keywords to determine importance.

    Returns:
        str: The extracted parts as a concatenated string.
    """
    doc = (
        extract_paragraphs_from_presentation(event["presentation"], keywords)
        + "\n"
        + extract_paragraphs_from_qa(event["qa"], keywords)
    )
    return doc


def extract_paragraphs_from_events(events: list, keywords: list) -> list:
    """
    Method to extract important paragraphs from a list of events.

    Args:
        events (list): List of dicts containing the events.
        keywords (list): List of keywords to determine importance.

    Returns:
        list: The extracted paragraphs as a list of concatenated strings.
    """
    print("Extracting paragraphs from events")
    docs = [
        extract_paragraphs_from_event(event, keywords)
        for event in tqdm(events, desc="Events", total=len(events))
    ]
    return docs


def check_keywords_in_event(event: dict, keywords: dict = {}) -> bool:
    """
    Method to check if keywords are present in the presentation or
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
    filtered_events = [e for e in events if e["type"] == "PushEvent"]
    print("Filtered to {} events".format(len(filtered_events)))
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
