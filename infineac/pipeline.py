"""Entire pipeline to extract topics from a given list of files containing
earnings calls transcripts or a list of events."""

from pathlib import Path

import polars as pl

import infineac.constants as constants
import infineac.file_loader as file_loader
import infineac.helper as helper
import infineac.process_event as process_event
import infineac.process_text as process_text
import infineac.topic_extractor as topic_extractor


def pipeline(
    path,
    preload_events: bool | str = False,
    year=constants.BASE_YEAR,
    keywords={"russia": 1, "ukraine": 1},
    modifier_words=constants.MODIFIER_WORDS,
    context_window_sentence=0,
    subsequent_paragraphs=0,
    join_adjacent_sentences=True,
    extract_answers=True,
    nlp_model=None,
    lemmatize: bool = True,
    lowercase: bool = True,
    remove_stopwords: bool = True,
    remove_punctuation: bool = True,
    remove_numeric: bool = True,
    remove_currency: bool = True,
    remove_space: bool = True,
    remove_keywords: bool = True,
    remove_names: bool = True,
    remove_strategies: bool | dict[str, list[str]] = True,
    remove_additional_stopwords: bool | list[str] = True,
    representation_model=None,
    embedding_model=None,
    umap_model=None,
    vectorizer_model=None,
    nr_topics=None,
    predefined_topics: bool | list[list[str]] = None,
    threshold: int = 1,
):
    """Pipeline to extract topics from a given list of files containing
    earnings calls transcripts or a list of events."""

    files = list(Path(path).rglob("*.xml"))
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")
    events = file_loader.load_files_from_xml(files[0:500])

    if preload_events is not False:
        events = helper.load_data(preload_events)

    print(f"Filtering events by year {year} and keywords {keywords}")
    events_filtered = process_event.filter_events(events, year=year, keywords=keywords)

    print(f"Creating corpus from {len(events_filtered)} events")

    corpus_df = process_event.events_to_corpus(
        events_filtered,
        keywords,
        modifier_words,
        context_window_sentence,
        subsequent_paragraphs,
        join_adjacent_sentences,
        extract_answers,
        nlp_model,
        lemmatize,
        lowercase,
        remove_stopwords,
        remove_punctuation,
        remove_numeric,
        remove_currency,
        remove_space,
        remove_keywords,
        remove_names,
        remove_strategies,
        remove_additional_stopwords,
    )

    corpus_df = process_text.get_strategies(dataframe=corpus_df)

    corpus, bridge = process_text.remove_sentences_under_threshold(
        corpus_df["processed_text"].to_list(), threshold
    )

    topic_model, topics, probs = topic_extractor.bert_advanced(
        corpus,
        representation_model,
        embedding_model,
        umap_model,
        vectorizer_model,
        nr_topics,
        predefined_topics,
    )

    topics_filled = helper.fill_list_from_mapping(topics, bridge, -2)
    categories = topic_extractor.categorize_topics(
        topic_model.get_topic_info()["Chain: Inspired - MMR"].to_list()
    )
    categories = categories.hstack(
        pl.DataFrame(topic_model.get_topic_info())[
            ["Count", "Name", "Representative_Docs"]
        ]
    )
    topics_df = topic_extractor.map_topics_to_categories(topics_filled, categories)
    categories.filter(pl.col("n") != -1).groupby(("category")).agg(
        pl.col("Count").sum()
    ).sort("Count", descending=True)

    corpus_df = corpus_df.with_columns(pl.Series("year", corpus_df["date"].dt.year()))
    corpus_df = corpus_df.hstack(topics_df)

    corpus_df_agg = topic_extractor.get_topics_per_company(corpus_df)

    return corpus_df, corpus_df_agg
