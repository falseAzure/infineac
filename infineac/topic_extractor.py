"""
This module contains helper functions for the infineac package.
"""

from bertopic import BERTopic

# seed_topic_list = [
#     ["drug", "cancer", "drugs", "doctor"],
#     ["windows", "drive", "dos", "file"],
#     ["space", "launch", "orbit", "lunar"],
# ]

# topic_model = BERTopic(seed_topic_list=seed_topic_list)


def bert_basic(docs):
    topic_model = BERTopic(
        verbose=True, low_memory=True, nr_topics=10, calculate_probabilities=True
    )
    topics, probs = topic_model.fit_transform(docs)
    return topics, probs


def bert_inspired(docs, representation_model, nr_topics=None, seed_topic_list=None):
    # Fine-tune your topic representations
    topic_model = BERTopic(
        representation_model=representation_model,
        nr_topics=nr_topics,
        calculate_probabilities=True,
        seed_topic_list=seed_topic_list,
        low_memory=True,
        verbose=True,
    )
    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs


def bert_advanced(
    docs, representation_model, embedding_model=None, vectorizer_model=None
):
    # Fine-tune your topic representations
    topic_model = BERTopic(
        representation_model=representation_model,
        nr_topics=10,
        calculate_probabilities=True,
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        # seed_topic_list=seed_topic_list,
        low_memory=True,
        verbose=True,
    )
    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs
