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


def get_russia_and_sanction(string: str) -> str:
    """
    Evaluates a string if it contains the words "russia" and "sanction" and
    returns a string accordingly.

    Args:
        string (str): String to be evaluated.

    Returns:
        str: Returns "russia & sanction" if the string contains both words,
        "russia" if it contains only "russia", "sanction" if it contains only
        "sanction" and "none" if it contains neither.
    """
    string_lower = string.lower()
    if re.search(r"russia", string_lower):
        if re.search(r"sanctions", string_lower):
            return "russia & sanction"
        else:
            return "russia"
    if re.search(r"sanction", string_lower):
        return "sanction"
    else:
        return "none"


def get_elections(string: str) -> str:
    """
    Evaluates a string if it contains the words "election" and "presidential
    election" and returns a string accordingly.

    Args:
        string (str): String to be evaluated.

    Returns:
        str: Returns "presidential election" if the string contains both words,
        "election" if it contains only "election", "none" if it contains
        neither.
    """
    string_lower = string.lower()
    if re.search(r"presidential election", string_lower):
        return "presidential election"
    if re.search(r" election", string_lower):
        return "election"
    else:
        return "none"


def extract_parts_from_presentation(presentation: list, keywords: list) -> str:
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


def extract_parts_from_qa(qa: list, keywords: list) -> str:
    whole_text = ""
    if qa is None:
        return whole_text
    previous_question_keyword = False
    previous_paragraph_keyword = False
    for part in qa:
        # operator, editor, moderator
        # if part["position"] in ["operator", "editor", "moderator"]:
        #     continue

        # conference and others (unidentified, unknown etc.)
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


def extract_doc_from_event(event: dict, keywords: list) -> str:
    doc = (
        extract_parts_from_presentation(event["presentation"], keywords)
        + "\n"
        + extract_parts_from_qa(event["qa"], keywords)
    )
    return doc


def extract_docs_from_events(events: list, keywords: list) -> list:
    docs = [extract_doc_from_event(event, keywords) for event in events]
    return docs


def check_keywords_in_event(event: dict, keywords: dict = {}) -> bool:
    if keywords == {}:
        return True

    for key, value in keywords.items():
        if (
            str(event["qa_collapsed"] + event["presentation_collapsed"])
            .lower()
            .count(key)
            >= value
        ):
            return True

    return False


def filter_events(events: list, year: int = 2022, keywords: dict = {}) -> list:
    events_filtered = []
    for event in events:
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
    # if not (
    #     str(event["qa_collapsed"] + event["presentation_collapsed"])
    #     .lower()
    #     .count("russia")
    #     >= 1
    # ):
    #     continue

    return events_filtered
