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

import polars as pl
from bertopic import BERTopic
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
):
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
    _type_
        _description_
    """
    seed_topic_list = None
    if predefined_topics is True:
        seed_topic_list = list(constants.TOPICS.values())
    elif type(predefined_topics) == list:
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


def get_topics_per_company(df: pl.DataFrame):
    """Returns the topics per company."""
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
    """Categorizes the topics according to the corresponding `keywords_topics` and a
    dictionary, that maps keywords to topics. Categories are grouped topics
    defined by :func:`infineac.constants.TOPICS`."""
    max_category_list = []
    for keywords in keywords_topics:
        count_max_group = 0
        max_topic = "other"
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
    """Maps the `topics` to the corresponding groups."""
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
