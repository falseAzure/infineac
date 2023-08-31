from pathlib import Path

import infineac.file_loader as file_loader
import infineac.process_event as process_event
import infineac.topic_extractor as topic_extractor
from infineac.process_text import MODIFIER_WORDS


def pipeline(
    path,
    year=2022,
    keywords={"russia": 1, "ukraine": 1},
    modifier_words=MODIFIER_WORDS,
    context_window_sentence=0,
    subsequent_paragraphs=0,
    join_adjacent_sentences=True,
    extract_answers=True,
    nlp_model=None,
    remove_additional_words=True,
):
    files = list(Path(path).rglob("*.xml"))
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")
    events = file_loader.load_files_from_xml(files[0:500])

    print(f"Filtering events by year {year} and keywords {keywords}")
    events_filtered = process_event.filter_events(events, year=year, keywords=keywords)

    print(f"Creating corpus from {len(events_filtered)} events")

    corpus_df = process_event.events_to_corpus(
        events=events_filtered,
        keywords=keywords,
        modifier_words=modifier_words,
        context_window_sentence=context_window_sentence,
        subsequent_paragraphs=subsequent_paragraphs,
        join_adjacent_sentences=join_adjacent_sentences,
        extract_answers=extract_answers,
        nlp_model=nlp_model,
        remove_additional_words=remove_additional_words,
    )

    topic_model, topics, probs = topic_extractor.bert_inspired(
        corpus_df["processed_text"].tolist()
    )

    return topics, probs


def parameter_tuning():
    pass
