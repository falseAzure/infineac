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
    path: str = None,
    preload_events: bool | str = False,
    preload_corpus: bool | str = False,
    keywords=[],
    nlp_model=None,
    year=constants.BASE_YEAR,
    modifier_words=constants.MODIFIER_WORDS,
    sections="all",
    context_window_sentence=0,
    join_adjacent_sentences=True,
    subsequent_paragraphs=0,
    extract_answers=True,
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
    earnings calls transcripts or a list of events.

    Parameters
    ----------
    path : str
        Path to directory of earnings calls transcripts.
    preload_events : bool | str, default: False
        Path to file containing events.
    preload_corpus : bool | str, default: False
        Path to file containing corpus.
    keywords : list[str] | dict[str, int], default: None
        List of `keywords` to search for in the events and extract the
        corresponding passages. If `keywords` is a dictionary, the keys are the
        keywords.
    nlp_model : spacy.lang, default: None
        NLP model. lemmatize : bool, default: True If document should be
        lemmatized.
    year : int, default: contants.BASE_YEAR
        Year to filter the events by.
    modifier_words : list[str], default: MODIFIER_WORDS
        List of `modifier_words`, which must not precede the keyword.
    sections : str, default: "all"
        Section of the event to extract the passages from. Either "all",
        "presentation" or "qa"
    context_window_sentence : tuple[int, int] | int, default: 0
        The context window of of the sentences to be extracted. Either an
        integer or a tuple of length 2. The first element of the tuple indicates
        the number of sentences to be extracted before the sentence the keyword
        was found in, the second element indicates the number of sentences
        after it. If only an integer is provided, the same number of sentences
        are extracted before and after the keyword. If one of the elements is
        -1, all sentences before or after the keyword are extracted. So -1 can
        be used to extract all sentences before and after the keyword, e.g. the
        entire paragraph.
    join_adjacent_sentences : bool, default: True
        Whether to join adjacent sentences.
    subsequent_paragraphs : int, default: 0
        Number of subsequent paragraphs to extract after the one containing a
        keyword.
    extract_answers : bool, default: False
        If True, entire answers to questions that include a keyword are also
        extracted.
    return_type : str, default: "list"
        The return type of the method. Either "str" or "list"
    lowercase : bool, default: True
        If document should be lowercased.
    remove_stopwords : bool, default: True
        If stopwords should be removed from document.
    remove_punctuation : bool, default: True
        If punctuation should be removed from document.
    remove_numeric : bool, default: False
        If numerics should be removed from document.
    remove_currency : bool, default: True
        If currency symbols should be removed from document.
    remove_space : bool, default: True
        If spaces should be removed from document.
    remove_keywords: bool, default: True
        If keywords should be removed from document.
    remove_names : bool, default: True
        If participant names should be removed from document.
    remove_strategies : bool | dict[str, list[str]], default: True
        If the strategy keywords should be removed from document.
    remove_additional_stopwords : bool | list[str], default: True
        If additional stopwords should be removed from document.
    representation_model : any
        Representation model to use.
    embedding_model : any, default: None
        Embedding model to use. If None, the default embedding model is used.
    umap_model : any, default: None
        UMAP model to use. If None, the default UMAP model is used.
    vectorizer_model : any, default: None
        Vectorizer model to use. If None, the default vectorizer model is used.
    nr_topics : any, default: None
        Number of topics to extract. If None, the number of topics is
        determined automatically.
    predefined_topics : bool | list[list[str]], default: None
        Whether to use `predefined_topics`. If True,
        :func:`infineac.constants.TOPICS` is used.
    threshold : int, default: 1
        Threshold to remove documents from the corpus. If a document contains
        less words than the `threshold`, it is removed.

    Returns
    -------
    Tuple[polars.DataFrame, polars.DataFrame]
        Tuple of two polars DataFrames. The first DataFrame contains the
        results for each event. The second DataFrame contains the results
        aggregated for each company and year.
    """
    if nlp_model is None:
        raise ValueError("nlp_model must be provided.")

    if preload_corpus is not False:
        preload_events = None

    if preload_events is False:
        files = list(Path(path).rglob("*.xml"))
        print(f"Found {len(files)} files\n")
        print(f"Loading files from {files[0]} to {files[len(files) - 1]}")
        events = file_loader.load_files_from_xml(files[0:500])
    elif type(preload_events) == str:
        events = helper.load_data(preload_events)
    elif type(preload_events) == list:
        events = preload_events

    if preload_corpus is False:
        print(f"Filtering events by year {year} and keywords {keywords}")
        events_filtered = process_event.filter_events(
            events, year=year, keywords=keywords
        )

        print(f"Creating corpus from {len(events_filtered)} events")

        corpus_df = process_event.events_to_corpus(
            events=events_filtered,
            keywords=keywords,
            modifier_words=modifier_words,
            sections=sections,
            context_window_sentence=context_window_sentence,
            join_adjacent_sentences=join_adjacent_sentences,
            subsequent_paragraphs=subsequent_paragraphs,
            extract_answers=extract_answers,
            return_type="list",
            nlp_model=nlp_model,
            lemmatize=lemmatize,
            lowercase=lowercase,
            remove_stopwords=remove_stopwords,
            remove_punctuation=remove_punctuation,
            remove_numeric=remove_numeric,
            remove_currency=remove_currency,
            remove_space=remove_space,
            remove_keywords=remove_keywords,
            remove_names=remove_names,
            remove_strategies=remove_strategies,
            remove_additional_stopwords=remove_additional_stopwords,
        )
    elif type(preload_corpus) == str:
        corpus_df = helper.load_data(preload_corpus)
    elif type(preload_corpus) == pl.dataframe.frame.DataFrame:
        corpus_df = preload_corpus

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
    topics_categories = topic_extractor.categorize_topics(
        topic_model.get_topic_info()["Chain: Inspired - MMR"].to_list()
    )
    topics_categories = topics_categories.hstack(
        pl.DataFrame(topic_model.get_topic_info())[
            ["Count", "Name", "Representative_Docs"]
        ]
    )
    topics_categories_per_doc = topic_extractor.map_topics_to_categories(
        topics_filled, topics_categories
    )
    categories_count = (
        topics_categories.filter(pl.col("n") != -1)
        .groupby(("category"))
        .agg(pl.col("Count").sum())
        .sort("Count", descending=True)
    )

    results_df = corpus_df.with_columns(pl.Series("year", corpus_df["date"].dt.year()))
    results_df = results_df.hstack(topics_categories_per_doc)
    results_df = results_df.select(
        pl.exclude(["keywords", "Count", "Name", "Representative_Docs"])
    )

    results_comp = topic_extractor.get_topics_per_company(results_df)
    results_comp = results_comp.select(pl.exclude(["text", "processed_text", "id"]))

    return results_df, results_comp, topics_categories, categories_count
