"""
Module for importing and preprocessing the earnings calls data.
"""

import lxml
from datetime import datetime
import os
import glob
from pathlib import Path
import polars as pl
import re
from lxml import etree, objectify


def extract_info_from_conference_call_part(part, corp_participants, conf_participants, type='presentation'):
    # Part is either presentation or qa
    # Split presentation into slides
    split_symbol = '--------------------------------------------------------------------------------\r\n'
    part_split = part.split(split_symbol)
    part_split = [el.strip() for el in part_split] #  if el.strip()
    # removes the double spaces between speaker and his position
    # presentation = [re.sub(' +', ' ', el.strip()) for el in presentation if el.strip()]

    # Split presentation into speakers and texts
    speakers = [el for el in part_split if re.match('.+\s{2,}\[\d+\]', el) or (re.match('\[\d+\]', el) and len(el)<=5)]
    texts = [el for el in part_split if el not in speakers]

    n_speakers = len(speakers)
    n_texts = len(texts)
    
    # Note: if no speaker or no text is found, the presentation is not included
    if n_speakers==0:
        print ("Warning: No speakers present at " + type)
        return None
    if n_texts==0:
        print("Warning: No texts present " + type)
        return None
    
    regex_pattern = r"(.*)\s{2,}\[(\d+)\]$"
    speakers_ordered = [[int(re.search(regex_pattern, el).group(2)), re.search(regex_pattern, el).group(1).strip()] if not re.match('\[\d+\]', el) 
                                 else [int(re.search(r'\[(\d+)\]', el).group(1)), "unknown"] # if the speaker is not mentioned
                                 for el in speakers]

    speakers_ordered = [[el[0],
    el[1],
    "operator" if el[1] == "Operator"
    else "editor" if el[1] == "Editor"
    else "cooperation" if el[1] in corp_participants
    else "conference" if el[1] in conf_participants
    else el[1] if corp_participants != [] and conf_participants != []
    else "unknown"]
    for el in speakers_ordered]


    if len(speakers) != len(texts):
        print("Warning: presentation_speakers (", n_speakers,") and presentation_texts (", n_texts ,") have different lengths", sep="")
        if n_speakers > n_texts:
            missing = n_speakers - n_texts
            texts = texts + [''] * missing
            print("Warning: presentation_texts was extended with empty strings")
        if n_speakers < n_texts:
            missing = n_texts - n_speakers
            last_speaker = speakers_ordered[-1][0]
            for i in range(missing):
                speakers.append([last_speaker+i, "unknown", "unknown"])
            print("Warning: presentation_speakers was extended with unknown speakers")
    part_ordered = [[speakers_ordered[i][0],
                     speakers_ordered[i][1],
                     speakers_ordered[i][2],
                     texts[i]]
                    for i in range(len(speakers))]
    return part_ordered