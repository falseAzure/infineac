"""
This file contains helper functions for the infineac package.
"""

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired

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


def bert_inspired(docs, seed_topic_list=None):
    # Fine-tune your topic representations
    representation_model = KeyBERTInspired()
    topic_model = BERTopic(
        representation_model=representation_model,
        verbose=True,
        low_memory=True,
        calculate_probabilities=True,
        seed_topic_list=seed_topic_list,
    )
    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs
