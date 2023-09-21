import argparse

import spacy_stanza
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance
from sentence_transformers import SentenceTransformer

import infineac.constants as constants
import infineac.helper as helper
from infineac.pipeline import pipeline

PATH = "data/transcripts/"


def get_args():
    parser = argparse.ArgumentParser(
        "This script generates topics from with BERTopic"
        + "from a given list of files containing earnings calls transcripts."
    )

    parser.add_argument(
        "-p",
        "--path",
        type=str,
        default=PATH,
        help="Path to directory of earnings calls transcripts.",
    )

    parser.add_argument(
        "-pe",
        "--preload_events",
        type=str,
        default=False,
        help="Path to pickle/lz4 file containing the events.",
    )

    parser.add_argument(
        "-pc",
        "--preload_corpus",
        type=str,
        default=False,
        help="Path to pickle/lz4 file containing the corpus.",
    )

    parser.add_argument(
        "-y",
        "--year",
        type=int,
        default=constants.BASE_YEAR,
        help="Year to filter events by - all events before this year will be removed.",
    )

    parser.add_argument(
        "-k",
        "--keywords",
        type=str,
        nargs="*",
        default=["russia", "ukraine"],
        help="Keywords to filter events by"
        + "- all events not containing these keywords will be removed.",
    )

    parser.add_argument(
        "-s",
        "--sections",
        type=str,
        default="all",
        choices=["all", "presentation", "qa"],
        help="Section/s to extract passages from.",
    )

    parser.add_argument(
        "-w",
        "--window",
        type=int,
        nargs=2,
        default=0,
        help="Context window size in sentences.",
    )

    parser.add_argument(
        "-par",
        "--paragraphs",
        type=bool,
        default=False,
        help="Whether to include subsequent paragraphs.",
    )

    parser.add_argument(
        "-j",
        "--join",
        type=bool,
        default=True,
        help="Whether to join adjacent sentences.",
    )

    parser.add_argument(
        "-a",
        "--answers",
        type=bool,
        default=True,
        help="Whether to extract answers from the Q&A section"
        + "if keywords are present in the preceding question.",
    )

    parser.add_argument(
        "-rk",
        "--remove_keywords",
        type=bool,
        default=True,
        help="Whether to remove keywords from the extracted passages.",
    )

    parser.add_argument(
        "-rn",
        "--remove_names",
        type=bool,
        default=True,
        help="Whether to remove participant names from the extracted passages.",
    )

    parser.add_argument(
        "-rs",
        "--remove_strategies",
        type=bool,
        default=True,
        help="Whether to remove strategies from the extracted passages.",
    )

    parser.add_argument(
        "-ra",
        "--remove_additional_words",
        type=bool,
        default=True,
        help="Whether to remove additional stopwords from the extracted passages.",
    )

    parser.add_argument(
        "-t",
        "--threshold",
        type=int,
        default=1,
        help="All documents with equal or less words than"
        + "the threshold are removed from the corpus.",
    )

    return parser.parse_args()


if "__main__" == __name__:
    args = get_args()
    path = args.path
    preload_events = args.preload_events
    preload_corpus = args.preload_corpus
    year = args.year
    keywords = args.keywords
    sections = args.sections
    context_window_sentence = args.window
    subsequent_paragraphs = args.paragraphs
    join_adjacent_sentences = args.join
    extract_answers = args.answers
    remove_keywords = args.remove_keywords
    remove_names = args.remove_names
    remove_strategies = args.remove_strategies
    remove_additional_stopwords = args.remove_additional_words
    threshold = args.threshold

    nlp_model = spacy_stanza.load_pipeline("en", processors="tokenize, lemma")
    nlp_model.add_pipe("sentencizer")

    inspired_model = KeyBERTInspired()
    sentence_model_light = SentenceTransformer("all-MiniLM-L6-v2")
    mmr_model = MaximalMarginalRelevance(diversity=0.4)
    chain_model_1 = [inspired_model, mmr_model]
    representation_model = {
        "Main": inspired_model,
        "Chain: Inspired - MMR": chain_model_1,
    }

    results_df, results_comp, topics, categories_count = pipeline(
        path=path,
        preload_events=preload_events,
        preload_corpus=preload_corpus,
        nlp_model=nlp_model,
        year=year,
        keywords=keywords,
        sections=sections,
        context_window_sentence=context_window_sentence,
        join_adjacent_sentences=join_adjacent_sentences,
        subsequent_paragraphs=subsequent_paragraphs,
        extract_answers=extract_answers,
        remove_keywords=remove_keywords,
        remove_names=remove_names,
        remove_strategies=remove_strategies,
        remove_additional_stopwords=remove_additional_stopwords,
        representation_model=representation_model,
        embedding_model=sentence_model_light,
        threshold=threshold,
    )

    print("\nCategories extracted:")
    print(categories_count)

    print("\nWriting results to file")
    helper.save_data(results_df, "results_df.lz4")
    results_df.write_excel("results_df.xlsx")
    results_comp.write_excel("results_comp.xlsx")
    topics.write_excel("topics.xlsx")
