"""
This file contains helper functions for the infineac package.
"""

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired


def bert_basic(docs):
    topic_model = BERTopic(
        verbose=True, low_memory=True, nr_topics=10, calculate_probabilities=False
    )
    topics, probs = topic_model.fit_transform(docs)
    return topics, probs


def bert_inspired(docs):
    # Fine-tune your topic representations
    representation_model = KeyBERTInspired()
    topic_model = BERTopic(
        representation_model=representation_model,
        verbose=True,
        low_memory=True,
        nr_topics=10,
        calculate_probabilities=False,
    )
    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs
