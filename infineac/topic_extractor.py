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

# seed_topic_list = [
#     ["drug", "cancer", "drugs", "doctor"],
#     ["windows", "drive", "dos", "file"],
#     ["space", "launch", "orbit", "lunar"],
# ]

# topic_model = BERTopic(seed_topic_list=seed_topic_list)


def bert_advanced(
    docs,
    representation_model,
    embedding_model=None,
    vectorizer_model=None,
    nr_topics=None,
):
    # Fine-tune your topic representations
    topic_model = BERTopic(
        representation_model=representation_model,
        calculate_probabilities=True,
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        nr_topics=nr_topics,
        # seed_topic_list=seed_topic_list,
        low_memory=True,
        verbose=True,
    )
    topics, probs = topic_model.fit_transform(docs)
    return topic_model, topics, probs
