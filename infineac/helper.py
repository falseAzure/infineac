"""
This file contains helper functions for the infineac package.
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
