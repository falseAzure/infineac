"""
This file contains helper functions for the infineac package.
"""

from pathlib import Path

import file_loader as fl
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired


def bert_basic(docs):
    topic_model = BERTopic(verbose=False)
    topics, probs = topic_model.fit_transform(docs)
    return topics, probs


def bert_inspired(docs):
    # Fine-tune your topic representations
    representation_model = KeyBERTInspired()
    topic_model = BERTopic(representation_model=representation_model, verbose=False)
    topics, probs = topic_model.fit_transform(docs)
    return topics, probs


def run(path):
    files = list(Path(path).rglob("*.xml"))
    events = fl.load_files_from_xml(files)
    events_filt = [
        event
        for event in events
        if "date" in event.keys()
        and event["action"] == "publish"
        and event["date"].year >= 2022
        and event["version"] == "Final"
    ]
    events_russia = [
        event
        for event in events_filt
        if event["body_orig"].lower().count("russia") >= 1
    ]
    docs = [
        event["presentation_collapsed"] + "\n" + event["qa_collapsed"]
        for event in events_russia
    ]
    topics, probs = bert_basic(docs[0:500])
    return topics, probs
