"""
Module for importing and preprocessing the earnings calls data.
"""

import re
from datetime import datetime
from pathlib import Path

from lxml import etree


def seperate_earnings_call(string: str) -> dict:
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
        - the speaker's name (+ position)
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
    part_split = [el.strip() for el in part_split]  # if el.strip()
    # removes the double spaces between speaker and his position
    # presentation=[re.sub(' +', ' ', el.strip()) for el in presentation if el.strip()]

    # Split presentation into speakers and texts
    speakers = [
        el
        for el in part_split
        if re.match(".+\s{2,}\[\d+\]", el) or (re.match("\[\d+\]", el) and len(el) <= 5)
    ]

    texts = [el for el in part_split if el not in speakers]

    n_speakers = len(speakers)
    n_texts = len(texts)

    # Note: if no speaker or no text is found, the presentation is not included
    if n_speakers == 0:
        print("Warning: No speakers present at " + type)
        return None
    if n_texts == 0:
        print("Warning: No texts present " + type)
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
        [
            el[0],
            el[1],
            "operator"
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
        ]
        for el in speakers_ordered
    ]

    if len(speakers) != len(texts):
        print(
            "Warning: presentation_speakers (",
            n_speakers,
            ") and presentation_texts (",
            n_texts,
            ") have different lengths",
            sep="",
        )
        if n_speakers > n_texts:
            missing = n_speakers - n_texts
            texts = texts + [""] * missing
            print("Warning: presentation_texts was extended with empty strings")
        if n_speakers < n_texts:
            missing = n_texts - n_speakers
            last_speaker = speakers_ordered[-1][0]
            for i in range(missing):
                speakers.append([last_speaker + i, "unknown", "unknown"])
            print("Warning: presentation_speakers was extended with unknown speakers")
    part_ordered = [
        [
            speakers_ordered[i][0],
            speakers_ordered[i][1],
            speakers_ordered[i][2],
            texts[i],
        ]
        for i in range(len(speakers))
    ]
    return part_ordered


def extract_info_from_earnings_call_sep(conference_call_sep_dict: dict) -> dict:
    """Extracts information from an earnings call.
    This information includes:
        - the corporate participants
        - the conference call participants
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
    corp_participants_colapsed = [",  ".join(pair) for pair in corp_participants]

    # Conference Call Participants
    conf_participants = conference_call_sep_dict["conf_participants"].split("*")
    conf_participants = [
        [el.strip() for el in pair.split("\r\n") if el.strip()]
        for pair in conf_participants
        if pair.strip()
    ]
    conf_participants_colapsed = [",  ".join(pair) for pair in conf_participants]

    # Presentation
    presentation = extract_info_from_earnings_call_part(
        conference_call_sep_dict["presentation"],
        corp_participants_colapsed,
        conf_participants_colapsed,
        type="presentation",
    )
    # Q&A
    qa = extract_info_from_earnings_call_part(
        conference_call_sep_dict["qa"],
        corp_participants_colapsed,
        conf_participants_colapsed,
        type="qa",
    )

    output = {
        "corp_participants": corp_participants,
        "corp_participants_colapsed": corp_participants_colapsed,
        "conf_participants": conf_participants,
        "conf_participants_colapsed": conf_participants_colapsed,
        "presentation": presentation,
        "qa": qa,
    }
    return output


def extract_info_from_earnings_call_body(body: str) -> dict:
    """Extracts information from the body of a conference call.
    This information includes:
        - the corporate participants
        - the conference call participants
        - the presentation/transcript
        - the Q&A

    This function is a wrapper for the function seperate_earnings_call and
    extract_info_from_earnings_call_sep.

    Args:
        body (str): The earnings call (body) as a string.

    Returns:
        dict: Dictionary containing the extracted information from the
        earnings.
    """
    conference_call_sep_raw = seperate_earnings_call(body)
    output = extract_info_from_earnings_call_sep(conference_call_sep_raw)
    return output


def load_files_xml(files: list) -> list:
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
              corp_participants_colapsed: colapsed list)
            - the conference call participants (conf_participants: list of lists and
              conf_participants_colapsed: colapsed list)
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
    events = []
    for file in files:
        event = {}
        event["file"] = Path(file).stem
        event["year_upload"] = int(Path(file).parts[2])
        # print(file)
        for _, elem in etree.iterparse(file):
            tag = elem.tag
            if tag is not None:
                text = elem.text
                if tag == "Body":
                    event["body_orig"] = text

                    body = extract_info_from_earnings_call_body(text)
                    event["corp_participants"] = body["corp_participants"]
                    event["corp_participants_colapsed"] = body[
                        "corp_participants_colapsed"
                    ]
                    event["conf_participants"] = body["conf_participants"]
                    event["conf_participants_colapsed"] = body[
                        "conf_participants_colapsed"
                    ]
                    event["presentation"] = body["presentation"]
                    event["qa"] = body["qa"]

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
        events.append(event)

    return events
