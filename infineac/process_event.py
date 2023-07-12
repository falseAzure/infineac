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


def extract_text_from_event(event: dict, keywords: list) -> str:
    doc = (
        extract_parts_from_presentation(event["presentation"], keywords)
        + "\n"
        + extract_parts_from_qa(event["qa"], keywords)
    )
    return doc


def extract_texts_from_events(events: list, keywords: list) -> list:
    print("Extracting texts from events")
    docs = [
        extract_text_from_event(event, keywords)
        for event in tqdm(events, desc="Events", total=len(events))
    ]
    return docs


def check_keywords_in_event(event: dict, keywords: dict = {}) -> bool:
    return process_text.check_keywords_in_string(
        string=str(event["qa_collapsed"] + event["presentation_collapsed"]),
        keywords=keywords,
    )


def filter_events(events: list, year: int = 2022, keywords: dict = {}) -> list:
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
