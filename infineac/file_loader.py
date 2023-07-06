"""
Module for importing and preprocessing the earnings calls data.
"""

import logging
import re
import warnings
from datetime import datetime
from pathlib import Path

from lxml import etree

# logging.basicConfig(
#     filename="load_files_from_xml.log",
#     level=logging.DEBUG,
#     format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
# )

load_logger = logging.getLogger("load_files_from_xml")
load_warnings_logger = logging.getLogger("load_files_from_xml_warnings")

# Set the logging level (optional)
load_logger.setLevel(logging.DEBUG)
load_warnings_logger.setLevel(logging.WARNING)

# Create a file handler and set the logging level for the handler (optional)
load_handler = logging.FileHandler("../logging/load_files.log")
load_handler.setLevel(logging.DEBUG)

load_warnings_handler = logging.FileHandler("../logging/load_files_warnings.log")
load_warnings_handler.setLevel(logging.WARNING)

# Create a formatter and add it to the handler (optional)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
load_handler.setFormatter(formatter)
load_warnings_handler.setFormatter(formatter)

# Add the handler to the logger
load_logger.addHandler(load_handler)
load_warnings_logger.addHandler(load_warnings_handler)


def separate_earnings_call(string: str) -> dict:
    """Separates the earnings call into its parts and returns them as a dictionary.:
        - Corporate Participants
        - Conference Call Participants
        - Presentation/Transcript
        - Questions and Answers

    If a part is not present, the corresponding string is empty.
    Each part starts and ends with a specific string, that surrounds the part.

    Args:
        string (str): The earnings call (body) as a string.

    Returns:
        dict: A dictionary with the four parts as keys and the corresponding
        text as values.
    """

    start_word_corp_participants = (
        "Corporate Participants\r\n========================"
        "========================================================\r\n"
    )
    end_word_corp_participants = (
        "========================================"
        "========================================"
    )

    start_word_conf_participants = (
        "Conference Call Participants\r\n==================="
        "=============================================================\r\n"
    )
    end_word_conf_participants = (
        "========================================"
        "========================================"
    )

    # es kann auch der Fall sein, dass anstatt von Presentation "Transcript" steht
    start_word_presentation = (
        "Presentation\r\n------------------------"
        "--------------------------------------------------------"
    )
    end_word_presentation = (
        "========================================"
        "========================================"
    )
    start_word_transcript = (
        "Transcript\r\n--------------------------"
        "------------------------------------------------------"
    )

    start_word_qa = (
        "Questions and Answers\r\n---------------"
        "-----------------------------------------------------------------"
    )
    end_word_qa = (
        "========================================"
        "========================================"
    )

    if start_word_corp_participants in string:
        start_index_corp_participants = string.find(start_word_corp_participants) + len(
            start_word_corp_participants
        )
        end_index_corp_participants = string.find(
            end_word_corp_participants, start_index_corp_participants
        )
        corp_participants = string[
            start_index_corp_participants:end_index_corp_participants
        ]
    else:
        corp_participants = ""

    if start_word_conf_participants in string:
        start_index_conf_participants = string.find(start_word_conf_participants) + len(
            start_word_conf_participants
        )
        end_index_conf_participants = string.find(
            end_word_conf_participants, start_index_conf_participants
        )
        conf_participants = string[
            start_index_conf_participants:end_index_conf_participants
        ]
    else:
        conf_participants = ""

    if start_word_presentation in string:
        start_index_presentation = string.find(start_word_presentation) + len(
            start_word_presentation
        )
        end_index_presentation = string.find(
            end_word_presentation, start_index_presentation
        )
        presentation = string[start_index_presentation:end_index_presentation]
    elif start_word_transcript in string:
        start_index_presentation = string.find(start_word_transcript) + len(
            start_word_transcript
        )
        end_index_presentation = string.find(
            end_word_presentation, start_index_presentation
        )
        presentation = string[start_index_presentation:end_index_presentation]
    else:
        presentation = ""

    if start_word_qa in string:
        start_index_qa = string.find(start_word_qa) + len(start_word_qa)
        end_index_qa = string.find(end_word_qa, start_index_qa)
        qa = string[start_index_qa:end_index_qa]
    else:
        qa = ""

    output = {
        "corp_participants": corp_participants,
        "conf_participants": conf_participants,
        "presentation": presentation,
        "qa": qa,
    }

    return output


def extract_info_from_earnings_call_part(
    part: str,
    corp_participants: list,
    conf_participants: list,
    type: str = "presentation",
) -> list:
    """Extracts information from earnings call part.
    This part is either the presentation/transcript or Q&A. The part is split
    into speakers and texts and then combined into a list, that holds for each
    part:
        - the speaker's appearance number (from 1 to n)
        - the speaker's name (+ position in company)
        - if the speaker is the operator, a corporate or conference call
          participant (or unknown)
        - the speaker's text

    Args:
        part (str): Either the presentation/transcript or Q&A.
        corp_participants (list): List of corporate participants
        conf_participants (list): List of conference call participants type
        (str, optional): Either presentation/transcript or Q&A. Defaults to
        "presentation".

    Returns:
        list: List of all the individual speakers (their appearance number,
        name and position) and their corresponding texts within a part of the
        earning call.
    """
    # Split presentation into slides
    split_symbol = (
        "------------------------------------"
        "--------------------------------------------\r\n"
    )
    part_split = part.split(split_symbol)
    part_split = [el.strip() for el in part_split]
    # removes the double spaces between speaker and his position
    # presentation=[re.sub(' +', ' ', el.strip()) for el in presentation if el.strip()]

    # Split presentation into speakers and texts
    speakers = [
        el
        for el in part_split
        if re.match(".+  \[\d+\]", el) or (re.match("\[\d+\]", el) and len(el) <= 5)
    ]

    texts = [el for el in part_split if el not in speakers]

    n_speakers = len(speakers)
    n_texts = len(texts)

    # Note: if no speaker or no text is found, the presentation is not included
    if n_speakers == 0:
        warning_message = f"No speakers present at {type}"
        load_logger.warning(warning_message)
        warnings.warn(warning_message)
        return None
    if n_texts == 0:
        warning_message = f"No texts present at {type}"
        load_logger.warning(warning_message)
        warnings.warn(warning_message)
        return None

    regex_pattern = r"(.*)\s{2,}\[(\d+)\]$"
    speakers_ordered = [
        [
            int(re.search(regex_pattern, el).group(2)),
            re.search(regex_pattern, el).group(1).strip(),
        ]
        if not re.match("\[\d+\]", el)
        else [
            int(re.search(r"\[(\d+)\]", el).group(1)),
            "unknown",
        ]  # if the speaker is not mentioned
        for el in speakers
    ]

    speakers_ordered = [
        {
            "n": el[0],
            "name": el[1],
            "position": "operator"
            if el[1] == "Operator"
            else "editor"
            if el[1] == "Editor"
            else "cooperation"
            if el[1] in corp_participants
            else "conference"
            if el[1] in conf_participants
            else el[1]
            if corp_participants != [] and conf_participants != []
            else "unknown",
        }
        for el in speakers_ordered
    ]

    if len(speakers) != len(texts):
        warning_message = (
            f"presentation_speakers ({n_speakers})"
            "and presentation_texts ({n_texts}) have different lengths"
        )
        load_logger.warning(warning_message)
        warnings.warn(warning_message)

        if n_speakers > n_texts:
            missing = n_speakers - n_texts
            texts = texts + [""] * missing
            warning_message = "presentation_texts was extended with empty strings"
            load_logger.warning(warning_message)
            warnings.warn(warning_message)
        if n_speakers < n_texts:
            missing = n_texts - n_speakers
            last_speaker = speakers_ordered[-1][0]
            for i in range(missing):
                speakers.append([last_speaker + i, "unknown", "unknown"])
            warning_message = "presentation_speakers was extended with unknown speakers"
            load_logger.warning(warning_message)
            warnings.warn(warning_message)
    part_ordered = [
        {
            "n": speakers_ordered[i]["n"],
            "name": speakers_ordered[i]["name"],
            "position": speakers_ordered[i]["position"],
            "text": texts[i],
        }
        for i in range(len(speakers))
    ]
    return part_ordered


def extract_info_from_earnings_call_sep(conference_call_sep_dict: dict) -> dict:
    """Extracts information from an earnings call.
    This information includes:
        - the corporate participants (name and position)
        - the conference call participants (name)
        - the presentation/transcript
        - the Q&A

    Args:
        conference_call_sep_raw (dict): Dictionary containing the separated raw
        earnings call.

    Returns:
        dict: Dictionary containing the extracted information from the earnings.
    """

    # Participants
    # Corporation Participants
    corp_participants = conference_call_sep_dict["corp_participants"].split("*")
    corp_participants = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in corp_participants
        if pair.strip()
    ]
    corp_participants_collapsed = [",  ".join(pair) for pair in corp_participants]

    # Conference Call Participants
    conf_participants = conference_call_sep_dict["conf_participants"].split("*")
    conf_participants = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in conf_participants
        if pair.strip()
    ]
    conf_participants_collapsed = [",  ".join(pair) for pair in conf_participants]

    # Presentation
    presentation = extract_info_from_earnings_call_part(
        conference_call_sep_dict["presentation"],
        corp_participants_collapsed,
        conf_participants_collapsed,
        type="presentation",
    )
    # Q&A
    qa = extract_info_from_earnings_call_part(
        conference_call_sep_dict["qa"],
        corp_participants_collapsed,
        conf_participants_collapsed,
        type="qa",
    )

    output = {
        "corp_participants": corp_participants,
        "corp_participants_collapsed": corp_participants_collapsed,
        "conf_participants": conf_participants,
        "conf_participants_collapsed": conf_participants_collapsed,
        "presentation": presentation,
        "qa": qa,
    }
    return output


def extract_info_from_earnings_call_body(body: str) -> dict:
    """Extracts information from the body of a conference call.
    This information includes:
        - the corporate participants (name and position)
        - the conference call participants (name)
        - the presentation/transcript
        - the Q&A

    This function is a wrapper for the function separate_earnings_call and
    extract_info_from_earnings_call_sep.

    Args:
        body (str): The earnings call (body) as a string.

    Returns:
        dict: Dictionary containing the extracted information from the
        earnings.
    """
    conference_call_sep_raw = separate_earnings_call(body)
    output = extract_info_from_earnings_call_sep(conference_call_sep_raw)
    return output


def create_blank_event() -> dict:
    """
    Creates a blank event with the keys that are expected in the final output.
    A list of these keys can be found in the documentation of the function:
    load_files_from_xml.

    Returns:
        dict: Dictionary containing the blank event.
    """
    event = {}
    event["file"] = ""
    event["year_upload"] = ""
    event["body_orig"] = ""
    event["corp_participants"] = []
    event["corp_participants_collapsed"] = []
    event["conf_participants"] = []
    event["conf_participants_collapsed"] = []
    event["presentation"] = []
    event["presentation_collapsed"] = ""
    event["qa"] = []
    event["qa_collapsed"] = ""
    event["action"] = "unknown"
    event["story_type"] = "unknown"
    event["version"] = "unknown"
    event["title"] = "unknown"
    event["city"] = "unknown"
    event["company_name"] = "unknown"
    event["company_ticker"] = "unknown"
    event["date"] = datetime(1900, 1, 1)
    event["id"] = int(-1)
    event["last_update"] = datetime(1900, 1, 1)
    event["event_type_id"] = int(-1)
    event["event_type_name"] = "unknown"

    return event


def add_info_to_event(event: dict, elem) -> dict:
    """Adds information to an event based on the element of an xml file.

    Args:
        event (dict): Event to which the information should be added.
        elem (lxml.etree.Element): Element of an xml file.

    Returns:
        dict: Dictionary containing the event with the added information.
    """
    tag = elem.tag
    if tag is not None:
        text = elem.text
        if tag == "Body":
            event["body_orig"] = text

            body = extract_info_from_earnings_call_body(text)

            event["corp_participants"] = body["corp_participants"]
            event["corp_participants_collapsed"] = body["corp_participants_collapsed"]
            event["conf_participants"] = body["conf_participants"]
            event["conf_participants_collapsed"] = body["conf_participants_collapsed"]

            presentation = body["presentation"]
            event["presentation"] = presentation
            if presentation:
                event["presentation_collapsed"] = " ".join(
                    [
                        el["text"]
                        for el in presentation
                        if el["position"] == "cooperation"
                    ]
                )
            else:
                event["presentation_collapsed"] = ""

            qa = body["qa"]
            event["qa"] = qa
            if qa:
                event["qa_collapsed"] = " ".join(
                    [el["text"] for el in qa if el["position"] == "cooperation"]
                )
            else:
                event["qa_collapsed"] = ""

        if tag == "EventStory":
            event["action"] = elem.attrib["action"]
            event["story_type"] = elem.attrib["storyType"]
            event["version"] = elem.attrib["version"]
        if tag == "eventTitle":
            event["title"] = text
        if tag == "city":
            event["city"] = text
        if tag == "companyName":
            event["company_name"] = text
        if tag == "companyTicker":
            event["company_ticker"] = text
        if tag == "startDate":
            event["date"] = datetime.strptime(text, "%d-%b-%y %I:%M%p %Z")
        if tag == "Event":
            event["id"] = int(elem.attrib["Id"])
            event["last_update"] = datetime.strptime(
                elem.attrib["lastUpdate"], "%A, %B %d, %Y at %I:%M:%S%p %Z"
            )
            event["event_type_id"] = int(elem.attrib["eventTypeId"])
            event["event_type_name"] = elem.attrib["eventTypeName"]

    return event


def load_files_from_xml(files: list) -> list:
    """Parses the xml files and extracts the information from the earnings calls.

    Args:
        files (list): List of xml files, that will be parsed.

    Returns:
        list: List of dictionaries containing the extracted information from
        the earnings calls. For each file there is one dictionary, that
        contains the following information:
            - the file name (file: string)
            - the year of the upload (year_upload: integer)
            - the original body of the earnings call (body_orig string)
            - the corporate participants (corp_participants: list of lists and
              corp_participants_collapsed: collapsed list)
            - the conference call participants (conf_participants: list of lists and
              conf_participants_collapsed: collapsed list)
            - the presentation (presentation list of lists)
            - the Q&A (Q&A list of lists)
            - the action (e.g. publish) (action: string)
            - the story type (e.g. transcript) (story_type string)
            - the version of the publication (e.g. final) (version: string)
            - the title of the earnings call (title: string)
            - the city of the earnings call (city: string)
            - the company of the earnings call (company_name: string)
            - the company ticker of the earnings call (company_ticker: string)
            - the date of the earnings call (date: date)
            - the id of the publication (id: int)
            - the last update of the publication (last_update: date)
            - the event type id (event_type_id: int)
            - the event type name (event_type_name: string)
    """
    load_logger.info("Start loading files from xml")
    load_logger.info("Number of files: " + str(len(files)))
    load_logger.info("Start processing files")

    events = []
    i = 1
    for file in files:
        load_logger.info(
            "Processing file: " + str(i) + "/" + str(len(files)) + ": " + str(file)
        )
        print(i, "/", len(files), " - Processing file: ", file, end="\r")
        event = create_blank_event()
        # event["file"] = Path(file).stem
        event["file"] = file
        event["year_upload"] = int(Path(file).parts[3])

        for _, elem in etree.iterparse(file):
            with warnings.catch_warnings(record=True) as caught_warnings:
                event = add_info_to_event(event, elem)

            if caught_warnings:
                for warning in caught_warnings:
                    warning_message = f"{file}: {warning.message}"
                    load_warnings_logger.warning(warning_message)
                    # print(f"Warning occurred in {file}: {warning.message}", end="\n")

        events.append(event)
        i += 1

    return events
