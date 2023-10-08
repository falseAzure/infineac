"""
Extracts topics from a list of documents using BERTopic.

Examples
--------
>>> import infineac.process_event as process_event
>>> import infineac.file_loader as file_loader
>>> import spacy_stanza
>>> nlp = spacy_stanza.load_pipeline("en", processors="tokenize, lemma")
>>> nlp.add_pipe('sentencizer')
>>> PATH_DIR = "data/transcripts/"
>>> files = list(Path(PATH_DIR).rglob("*.xml"))
>>> events = file_loader.load_files_from_xml(files)
>>> keywords = {"russia": 1, "ukraine": 1}
>>> corpus = process_event.events_to_corpus(
                events=events,
                keywords=keywords,
                nlp_model=nlp)
>>> docs = process_text.remove_sentences_under_threshold(
                corpus["processed_text"].tolist())
>>> topics, probs = topic_extractor.bert_advanced(docs)


"""

import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import seaborn as sns
from bertopic import BERTopic
from numpy import ndarray
from umap import UMAP

import infineac.constants as constants


def bert_advanced(
    docs: list[str],
    representation_model: any,
    embedding_model: any = None,
    umap_model: any = None,
    vectorizer_model: any = None,
    nr_topics: any = None,
    predefined_topics: bool | list[list[str]] = None,
) -> tuple[BERTopic, list[int], ndarray]:
    """Extracts topics from a list of documents using BERTopic.

    Parameters
    ----------
    docs : list[str]
        List of documents.
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
        Whether to use `predefined_topics`. If True, :func:constants.TOPICS is used.

    Returns
    -------
    tuple[BERTopic, list[int], ndarray]
        Tuple containing the BERTopic model, the topics and the probabilities.
    """
    seed_topic_list = None
    if predefined_topics is True:
        seed_topic_list = list(constants.TOPICS.values())
    elif type(predefined_topics) is list:
        seed_topic_list = predefined_topics

    if umap_model is None:
        umap_model = UMAP(
            n_neighbors=15,
            n_components=5,
            min_dist=0.0,
            metric="cosine",
            random_state=111,
        )

    # Fine-tune your topic representations
    topic_model = BERTopic(
        representation_model=representation_model,
        calculate_probabilities=True,
        embedding_model=embedding_model,
        umap_model=umap_model,
        vectorizer_model=vectorizer_model,
        nr_topics=nr_topics,
        seed_topic_list=seed_topic_list,
        low_memory=True,
        verbose=True,
    )

    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs


def get_groups_from_hierarchy(
    hierarchical_topics: pl.DataFrame, n: int = 10
) -> pl.DataFrame:
    """Returns the top `n` children/groups from the hierarchical topics."""
    parent = hierarchical_topics[
        hierarchical_topics["Distance"] == hierarchical_topics["Distance"].max()
    ]
    parent_id = str(parent["Parent_ID"].values[0])
    children = {parent_id}
    for i in range(n - 1):
        children.remove(parent_id)
        new_children = (
            parent[["Child_Left_ID", "Child_Right_ID"]].values.flatten().tolist()
        )
        children.update(new_children)

        new_df = hierarchical_topics[hierarchical_topics["Parent_ID"].isin(children)]
        parent = new_df[new_df["Distance"] == new_df["Distance"].max()]
        parent_id = str(parent["Parent_ID"].values[0])
    topics_grouped = hierarchical_topics[
        hierarchical_topics["Parent_ID"].isin(children)
    ]
    return topics_grouped


def get_topics_per_company(df: pl.DataFrame) -> pl.DataFrame:
    """
    Returns the topics and categories per company.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing the topics and the company names as well as the
        year and the three strategies.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the topics, categories and strategies per company.
    """
    df_comp = (
        df.groupby("company_name", "year")
        .agg(
            pl.col("exit_strategy", "stay_strategy", "adaptation_strategy").sum(),
            pl.col("text", "processed_text", "topic", "category"),
        )
        .sort("company_name")
    )

    topics_comp = df_comp["topic"].to_list()
    for i, topics in enumerate(topics_comp):
        topics_ = sorted(list(set(topics)))
        topics_ = [topic for topic in topics_ if topic != -1 | topic != -2]
        topics_comp[i] = topics_

    category_comp = df_comp["category"].to_list()
    for i, categories in enumerate(category_comp):
        category_ = list(set(categories))
        category_ = [group for group in category_]
        category_comp[i] = category_

    df_comp = df_comp.with_columns(pl.Series("category", category_comp))
    df_comp = df_comp.with_columns(pl.Series("topic", topics_comp))
    return df_comp


def categorize_topics(keywords_topics: list[list[str]]) -> pl.DataFrame:
    """
    Categorizes a lists of keywords (`keywords_topics`) according to the
    :func:`infineac.constants.TOPICS` dictionary, that maps keywords to
    categories.

    Notes
    -----
    Categories are grouped topics. Each list in `keywords_topics` corresponds
    to a topic, that is a list of keywords. Each list in `keywords_topics` is
    mapped to a category, that is a list of as well keywords.

    Parameters
    ----------
    keywords_topics : list[list[str]]
        List of lists of keywords per topic.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the categories and keywords per topic.
    """
    max_category_list = []
    for keywords in keywords_topics:
        count_max_group = 0
        max_topic = "misc"
        for category, keywords_group in constants.TOPICS.items():
            common_keywords = set(keywords_group) & set(keywords)
            count = len(common_keywords)
            if count > 0 and count > count_max_group:
                max_topic = category
                count_max_group = count
        max_category_list.append(max_topic)

    return pl.DataFrame(
        {
            "n": range(-1, len(max_category_list) - 1),
            "category": max_category_list,
            "keywords": keywords_topics,
        }
    )


def map_topics_to_categories(topics: list[int], mapping: pl.DataFrame) -> list[str]:
    """
    Maps a list of `topics` to the corresponding categories.

    The -1 and -2 topics are mapped to the "standard" and "empty" categories.

    Parameters
    ----------
    topics : list[int]
        List of topics.
    mapping : pl.DataFrame
        DataFrame containing the mapping from topics to categories.

    Returns
    -------
    list[str]
        List of categories.
    """
    topics_pl = pl.DataFrame({"topic": topics})
    topics_pl = topics_pl.join(mapping, left_on="topic", right_on="n", how="left")
    topics_pl = topics_pl.with_columns(
        category=pl.when(pl.col("topic") == -1)
        .then("standard")
        .otherwise(pl.col("category"))
    )
    topics_pl = topics_pl.with_columns(
        category=pl.when(pl.col("topic") == -2)
        .then("empty")
        .otherwise(pl.col("category"))
    )
    return topics_pl


def plot_category_distribution(
    df: pl.DataFrame, aggregate: list[str] = []
) -> pl.DataFrame:
    """
    Plots the category distribution for the given DataFrame `df` and the given
    `aggregate`.

    Parameters
    ----------
    df : pl.DataFrame
        DataFrame containing the categories and the strategies.
    aggregate : list[str], default: []
        List of columns to aggregate by.

    Returns
    -------
    pl.DataFrame
        DataFrame containing the category distribution.
    """
    count_categories = (
        df.groupby(["category"] + aggregate)
        .agg(
            count_category=pl.col("category").count(),
            exit=pl.col("exit_strategy").filter(pl.col("exit_strategy") > 0).count(),
            adaptation=pl.col("adaptation_strategy")
            .filter(pl.col("adaptation_strategy") > 0)
            .count(),
            stay=pl.col("stay_strategy").filter(pl.col("stay_strategy") > 0).count(),
        )
        .sort("count_category", descending=True)
    )
    if len(aggregate) > 0:
        count_categories = (
            count_categories.groupby(["category"])
            .agg(
                count_category=pl.col("category").count(),
                exit=pl.col("exit").filter(pl.col("exit") > 0).count(),
                adaptation=pl.col("adaptation")
                .filter(pl.col("adaptation") > 0)
                .count(),
                stay=pl.col("stay").filter(pl.col("stay") > 0).count(),
            )
            .sort("count_category", descending=True)
        )

    count_categories = count_categories.with_columns(
        (
            pl.col("count_category")
            - pl.col("exit")
            - pl.col("adaptation")
            - pl.col("stay")
        ).alias("no strategy")
    )
    melted_result = pd.melt(
        count_categories.to_pandas(),
        id_vars="category",
        value_vars=["no strategy", "exit", "adaptation", "stay"],
        var_name="strategy",
    )
    plot_data = melted_result[
        ~melted_result["category"].isin(["standard", "empty", "misc"])
    ]

    if len(aggregate) == 0:
        title_add = "all mentions."
    else:
        title_add = " and ".join(aggregate)

    ax = sns.histplot(
        plot_data,
        x="category",
        hue="strategy",
        weights="value",
        common_norm=True,
        multiple="stack",
        palette="muted",
    )
    plt.xlabel("Category")
    plt.ylabel("Count")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, horizontalalignment="right")
    plt.title("Category and strategy distribution for " + title_add)
    plt.show()

    return count_categories
