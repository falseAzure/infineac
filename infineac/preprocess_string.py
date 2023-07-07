"""
This file contains functions to manipulate strings and extract the
corresponding information for the infineac package.
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
    previous_paragraph_keyword = False
    for part in presentation:
        if part["name"] == "Operator" or part["position"] == "operator:":
            pass
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
    previous_question_keyword = False
    previous_paragraph_keyword = False
    for part in qa:
        if part["name"] == "Operator" or part["position"] == "operator:":
            pass

        if part["position"] == "conference:":
            if any(keyword in part["text"].lower() for keyword in keywords):
                previous_question_keyword = True
        else:  # part['position'] == 'cooperation':
            if previous_question_keyword:
                whole_text += part["text"]
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
