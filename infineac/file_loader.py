"""
Module for importing and structuring the earnings calls data. The earnings
calls are stored in xml files. :func:`load_files_from_xml` is the main function
of the module, that loads the xml files, extracts the relevant information and
stores it in a list of dictionaries.
"""

import logging
import os
import re
import warnings
from datetime import datetime
from pathlib import Path

from lxml import etree
from rapidfuzz import fuzz
from tqdm import tqdm

# main directory
main_dir = Path(__file__).resolve().parents[1]
logging_dir = main_dir / "logging"

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
if not logging_dir.exists():
    logging_dir.mkdir(parents=True, exist_ok=True)

load_handler = logging.FileHandler(logging_dir / "load_files.log")
load_handler.setLevel(logging.DEBUG)

load_warnings_handler = logging.FileHandler(logging_dir / "load_files_warnings.log")
load_warnings_handler.setLevel(logging.WARNING)

# Create a formatter and add it to the handler (optional)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
load_handler.setFormatter(formatter)
load_warnings_handler.setFormatter(formatter)

# Add the handler to the logger
load_logger.addHandler(load_handler)
load_warnings_logger.addHandler(load_warnings_handler)


def structure_earnings_call(string: str) -> dict:
    """
    Separates and structures the earnings call into its parts and returns them
    as a dictionary:

        - Corporate Participants
        - Conference Call Participants
        - Presentation/Transcript
        - Questions and Answers

    If a part is not present, the corresponding string is empty. Each part
    starts and ends with a specific string, that surrounds the part.

    Parameters
    ----------
    string : str
        The earnings call (body) as a string.

    Returns
    -------
    dict
        A dictionary with the following key - value pairs:

            - 'corp_participants': str - Corporate Participants
            - 'conf_participants': str - Conference Call Participants
            - 'presentation': str - Presentation/Transcript
            - 'qa': str - Questions and Answers

        The values are empty strings if the corresponding part is not present.
        If a value represents multiple values (e.g. holds information about
        multiple participants), these are separated by a specific string.
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

    string = string.replace("&amp;", "&")  # replace html ampersand

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


def transform_unlisted_participants(
    participant: dict, corp_participants: list[str], conf_participants: list[str]
) -> dict:
    """
    Transforms the unlisted participants name so that it can be either
    identified among the listed participants (corp_participants or
    conf_participants), the generic participants(operator, editor, moderator)
    or be added as an unknown participant.

    Parameters
    ----------
    participant : dict
        Participant to be transformed. Only uses the key 'name'.
    corp_participants : list[str]
        List of corporate participants.
    conf_participants : list[str]
        List of conference call participants.

    Returns
    -------
    dict
        The transformed participant.
    """
    # some participants are listed with a comma at the end
    if participant["name"].endswith(","):
        participant["name"] = participant["name"][:-1]
    # some participants are listed as "unknown ..."
    # if participant["name"].lower().startswith("unknown") or participant[
    #     "name"
    # ].lower().startswith("unidentified"):
    #     participant["name"] = "unknown participant"
    #     return participant
    # some operators / moderators are listed as "operator ..."
    if participant["name"].lower().startswith("operator"):
        participant["name"] = "Operator"
        return participant
    if participant["name"].lower().startswith("moderator"):
        participant["name"] = "Moderator"
        return participant
    participant["name"] = re.sub(r"\s{2,}", "  ", participant["name"])
    # (ph) is added to some of the participants' names
    # check if a similar name is in the list of participants
    for participant_ in corp_participants + conf_participants:
        if fuzz.ratio(participant["name"], participant_.replace("(ph)", "")) >= 80:
            participant["name"] = participant_
            return participant
    participant["name"] = re.sub(r"^[^A-Za-z]*", "", participant["name"])

    return participant


def get_participants_position(
    participant: dict, corp_participants: list[str], conf_participants: list[str]
) -> str:
    """
    Returns the position of the participant based on the lists of corporate and
    conference call participants.

    Parameters
    ----------
    participant : dict
        Participant to be transformed. Only uses the key 'name'.
    corp_participants : list[str]
        List of corporate participants.
    conf_participants : list[str]
        List of conference call participants.

    Returns
    -------
    str
        The position of the participant.
    """
    if participant["name"].lower() == "operator":
        return "operator"
    if participant["name"].lower() == "editor":
        return "operator"
    if participant["name"].lower() == "moderator":
        return "operator"
    if participant["name"] in corp_participants:
        return "cooperation"
    if participant["name"] in conf_participants:
        return "conference"
    if participant["name"].lower().startswith("unidentified") or participant[
        "name"
    ].lower().startswith("unknown"):
        return "unknown participant"
    if corp_participants != [] and conf_participants != []:
        return participant["name"]
    return "unknown participant"


def extract_info_from_earnings_call_part(
    part: str,
    corp_participants: list,
    conf_participants: list,
    type: str = "presentation",
) -> list[dict]:
    """
    Extracts information from an earnings call `part`.

    This `part` is either the presentation/transcript or Q&A. The extracted
    information contains the participant's number of appearance, the the name
    of the participant, the participant's position and the participant's text.
    The position of the participant can be either 'corporate', 'conference',
    'operator', 'editor', 'moderator' or 'unknown'. The information is returned
    as a list of dictionaries, where each dictionary holds the information of
    the individual participants and their corresponding texts.


    Parameters
    ----------
    part : str
        Either the presentation/transcript or Q&A as a string.
    corp_participants : list
        List of corporate participants.
    conf_participants : list
        List of conference call participants.
    type : str, default: "presentation"
        Either 'presentation' or 'qa'.

    Returns
    -------
    list[dict]
        List of dictionaries, where each dictionary holds the information of
        the individual participants and their corresponding texts within the given
        given part of the earning call.
        The dictionary key-value pairs are:

            - 'n': int - the participant's appearance number
            - 'name': str - the participant's name
            - 'position': str - The participant's position
            - 'text': str - the participant's text
    """
    # Split presentation into slides
    split_symbol = (
        "------------------------------------"
        "--------------------------------------------\r\n"
    )
    parts_split = part.split(split_symbol)
    parts_split = [part.strip() for part in parts_split]
    # removes the double spaces between participant and his position
    # presentation=[re.sub(' +', ' ', el.strip()) for el in presentation if el.strip()]

    # Split part into participants and texts
    participants = [
        part
        for part in parts_split
        if re.match(".+  \[\d+\]", part)
        or (re.match("\[\d+\]", part) and len(part) <= 5)
    ]

    texts = [part for part in parts_split if part not in participants]

    n_participants = len(participants)
    n_texts = len(texts)

    # Note: if no participant or no text is found, the presentation is not included
    if n_participants == 0:
        warning_message = f"No participants present at {type}"
        load_logger.warning(warning_message)
        warnings.warn(warning_message)
        return None
    if n_texts == 0:
        warning_message = f"No texts present at {type}"
        load_logger.warning(warning_message)
        warnings.warn(warning_message)
        return None

    regex_pattern = r"(.*)\s{2,}\[(\d+)\]$"
    participants_ordered = [
        {
            "n": int(re.search(regex_pattern, participant).group(2)),
            "name": re.search(regex_pattern, participant).group(1).strip(),
        }
        if not re.match("\[\d+\]", participant)
        else {
            "n": int(re.search(r"\[(\d+)\]", participant).group(1)),
            "name": "unknown participant",
        }  # if the participant is not mentioned
        for participant in participants
    ]

    participants_not_listed = [
        participant
        for participant in participants_ordered
        if participant["name"]
        not in corp_participants
        + conf_participants
        + ["editor", "operator", "moderator"]
        and not participant["name"].lower().startswith("unidentified")
    ]

    for participant in participants_not_listed:
        participant = transform_unlisted_participants(
            participant, corp_participants, conf_participants
        )

    participants_ordered = [
        {
            "n": participant["n"],
            "name": participant["name"],
            "position": get_participants_position(
                participant, corp_participants, conf_participants
            ),
        }
        for participant in participants_ordered
    ]

    if len(participants) != len(texts):
        warning_message = (
            f"presentation_participants ({n_participants})"
            "and presentation_texts ({n_texts}) have different lengths"
        )
        load_logger.warning(warning_message)
        warnings.warn(warning_message)

        # Extend the shorter list with empty strings
        if n_participants > n_texts:
            missing = n_participants - n_texts
            texts = texts + [""] * missing
            warning_message = "presentation_texts was extended with empty strings"
            load_logger.warning(warning_message)
            warnings.warn(warning_message)
        if n_participants < n_texts:
            missing = n_texts - n_participants
            last_participant = participants_ordered[-1][0]
            for i in range(missing):
                participants.append(
                    [last_participant + i, "unknown participant", "unknown participant"]
                )
            warning_message = (
                "presentation_participants was extended with unknown participants"
            )
            load_logger.warning(warning_message)
            warnings.warn(warning_message)

    part_ordered = [
        {
            "n": participants_ordered[i]["n"],
            "name": participants_ordered[i]["name"],
            "position": participants_ordered[i]["position"],
            "text": texts[i],
        }
        for i in range(len(participants))
    ]
    return part_ordered


def participants_string_to_list(participants: str) -> list[str]:
    """Split the participants string into a list of participants."""
    participants_list = re.split("\s{1,}\\*", participants)

    participants_list = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in participants_list
        if pair.strip()
    ]

    return participants_list


def participants_list_collapsed(participants_list: list[str]) -> list[str]:
    """Collapse the participants list into a list of strings."""
    return [",  ".join(pair) for pair in participants_list]


def extract_info_from_earnings_call_structured(
    conference_call_structured_dict: dict[str, str]
) -> dict:
    """
    Extracts information from an already structured earnings call.
    This information includes:

        - corporate participants (name and position)
        - conference call participants (name)
        - Presentation part of the earnings call
        - Q&A part of the earnings call

    Parameters
    ----------
    conference_call_structured_dict : dict[str, str]
        Dictionary containing the structured earnings call returned by
        :func:`structure_earnings_call`.

    Returns
    -------
    dict
        Dictionary containing the extracted information from the earnings call.
        The key-value pairs are:

            - 'corp_participants': list[list[str]] - List of corporate
              participants. Each participant is itself a list of strings with
              length 2. The first string is the name of the participant, the
              second string is the position of the participant.
            - 'corp_participants_collapsed': list[str] - As a above, but name
              and position of each participant are collapsed into a single
              string.
            - 'conf_participants': list[list[str]] - Conference call
              participants. Same format as above.
            - 'conf_participants_collapsed': list[str] - Conference call
              participants with collapsed name and position.
            - 'presentation': list[dict] - Presentation part of the earnings
              call as returned by :func:`extract_info_from_earnings_call_part`.
            - 'qa': list[dict] - Q&A part of the earnings call as returned by
              :func:`extract_info_from_earnings_call_part`.

    """
    # Participants
    # is listed with a '  *' at the beginning of each participant
    # Corporation Participants
    corp_participants = re.split(
        "\s{1,}\\*", conference_call_structured_dict["corp_participants"]
    )
    # corp_participants = conference_call_structured_dict["corp_participants"].split(
    #     "\s*"
    # )
    corp_participants = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in corp_participants
        if pair.strip()
    ]
    corp_participants_collapsed = [",  ".join(pair) for pair in corp_participants]

    # Conference Call Participants
    conf_participants = re.split(
        "\s{1,}\\*", conference_call_structured_dict["conf_participants"]
    )
    # conf_participants = conference_call_structured_dict["conf_participants"].split(
    #     "*"
    # )
    conf_participants = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in conf_participants
        if pair.strip()
    ]
    conf_participants_collapsed = [",  ".join(pair) for pair in conf_participants]

    # Presentation
    presentation = extract_info_from_earnings_call_part(
        conference_call_structured_dict["presentation"],
        corp_participants_collapsed,
        conf_participants_collapsed,
        type="presentation",
    )
    # Q&A
    qa = extract_info_from_earnings_call_part(
        conference_call_structured_dict["qa"],
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
    """
    Extracts information from the `body` of a conference call.
    This information includes:

        - corporate participants (name and position)
        - conference call participants (name)
        - Presentation part of the earnings call
        - Q&A part of the earnings call

    This function is a wrapper for the function :func:`structure_earnings_call`
    and :func:`extract_info_from_earnings_call_structured`.

    Parameters
    ----------
    body: str
        The earnings call (body) as a string.

    Returns
    -------
    dict
        Dictionary containing the extracted information from the earnings call.
        The key-value pairs are:

            - 'corp_participants': list[list[str]] - List of corporate
              participants. Each participant is itself a list of strings with
              length 2. The first string is the name of the participant, the
              second string is the position of the participant.
            - 'corp_participants_collapsed': list[str] - As a above, but name
              and position of each participant are collapsed into a single
              string.
            - 'conf_participants': list[list[str]] - Conference call
              participants. Same format as above.
            - 'conf_participants_collapsed': list[str] - Conference call
              participants with collapsed name and position.
            - 'presentation': list[dict] - Presentation part of the earnings
              call as returned by :func:`extract_info_from_earnings_call_part`.
            - 'qa': list[dict] - Q&A part of the earnings call as returned by
              :func:`extract_info_from_earnings_call_part`.
    """
    conference_call_structured_raw = structure_earnings_call(body)
    output = extract_info_from_earnings_call_structured(conference_call_structured_raw)
    return output


def create_blank_event() -> dict:
    """
    Creates a blank event with the keys that are expected in the final output.
    A list of these keys can be found in the documentation of
    :func:`load_files_from_xml`.

    Returns
    -------
    dict
        Dictionary containing the blank event.
    """
    event = {}
    event["file"] = ""
    event["year_upload"] = ""
    # event["body_orig"] = ""
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


def add_info_to_event(event: dict, element) -> dict:
    """
    Adds information to given `event` based on the `element` of an xml file.
    Used by :func:`load_files_from_xml`.

    Parameters
    ----------
    event : dict
        Event to which the information should be added.
    element : lxml.etree.Element
        Element of an xml file.

    Returns
    -------
    dict
        Dictionary containing the event with the added information.
    """
    tag = element.tag
    if tag is not None:
        text = element.text
        if tag == "Body":
            # event["body_orig"] = text

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
            event["action"] = element.attrib["action"]
            event["story_type"] = element.attrib["storyType"]
            event["version"] = element.attrib["version"]
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
            event["id"] = int(element.attrib["Id"])
            event["last_update"] = datetime.strptime(
                element.attrib["lastUpdate"], "%A, %B %d, %Y at %I:%M:%S%p %Z"
            )
            event["event_type_id"] = int(element.attrib["eventTypeId"])
            event["event_type_name"] = element.attrib["eventTypeName"]

    return event


def load_files_from_xml(files: list) -> list[dict]:
    """
    Parses the xml files and extracts the information from the earnings calls.

    Parameters
    ----------
    files : list
        List of xml `files`, to be parsed.

    Returns
    -------
    list[dict]
        List of dictionaries containing the extracted information from
        the earnings calls. For each file the information is extracted into a
        dictionary, that contains the following key-value pairs:

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
    load_logger.info("Start loading files from xml")
    load_logger.info("Number of files: " + str(len(files)))
    load_logger.info("Start processing files")

    events = []
    i = 1
    for file in tqdm(files, desc="Files", total=len(files)):
        load_logger.info(
            "Processing file: " + str(i) + "/" + str(len(files)) + ": " + str(file)
        )
        # print(i, "/", len(files), " - Processing file: ", file, end="\r")
        event = create_blank_event()
        # event["file"] = Path(file).stem
        event["file"] = file
        event["year_upload"] = int(os.path.basename(os.path.dirname(file)))

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
