"""
This module extracts topics from a list of documents using BERTopic.

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

from bertopic import BERTopic

import infineac.constants as constants


def bert_advanced(
    docs: list[str],
    representation_model,
    embedding_model=None,
    vectorizer_model=None,
    nr_topics=None,
    predefined_topics: bool | list[list[str]] = None,
):
    seed_topic_list = None
    if predefined_topics is True:
        seed_topic_list = constants.TOPICS
    elif type(predefined_topics) == list:
        seed_topic_list = predefined_topics

    # Fine-tune your topic representations
    topic_model = BERTopic(
        representation_model=representation_model,
        calculate_probabilities=True,
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        nr_topics=nr_topics,
        seed_topic_list=seed_topic_list,
        low_memory=True,
        verbose=True,
    )

    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs


def get_groups_from_hierarchy(hierarchical_topics, n=10):
    """Returns the top n groups from the hierarchical topics."""
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
