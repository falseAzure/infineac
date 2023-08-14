from pathlib import Path

import infineac.file_loader as file_loader
import infineac.process_event as process_event
import infineac.topic_extractor as topic_extractor
from infineac.process_text import FILTER_WORDS


def pipeline(
    path,
    year=2022,
    keywords={"russia": 1, "ukraine": 1},
    filter_words=FILTER_WORDS,
    context_window_sentence=0,
    subsequent_paragraphs=0,
    extract_answers=True,
):
    files = list(Path(path).rglob("*.xml"))
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")

    events = file_loader.load_files_from_xml(files[0:500])

    print("\nFilter events")
    events_filtered = process_event.filter_events(events, year=year, keywords=keywords)

    print(f"Parsing {len(events_filtered)} events")
    docs = process_event.extract_parts_from_events(
        events_filtered,
        keywords=keywords,
        filter_words=filter_words,
        context_window_sentence=context_window_sentence,
        subsequent_paragraphs=subsequent_paragraphs,
        extract_answers=extract_answers,
    )

    topics, probs = topic_extractor.bert_inspired(docs)

    return topics, probs


def parameter_tuning():
    pass
