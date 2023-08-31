import argparse
from pathlib import Path

import spacy_stanza

import infineac.helper as helper
import infineac.process_event as process_event

main_dir = Path(__file__).resolve().parents[1]
data_dir = main_dir / "data"

PATH = str(data_dir / "events.lz4")


def get_args():
    parser = argparse.ArgumentParser(
        "This scripts creates a filtered and preprocessed corpus of earnings calls"
        + "as a DataFrame from a given pickle or lz4 file"
        + "containing the earnings calls transcripts as a list of events."
        + "This file can be obtained by running the load_save_data.py script."
        + "The created corpus can then be used to train a topic model"
        + "(extract_topics.py)."
        + "The corpus/DataFrame is saved as a lz4 file."
    )

    parser.add_argument(
        "-p",
        "--path",
        type=str,
        default=PATH,
        help="Path to pickle/lz4 file containing the earnings calls transcripts",
    )

    parser.add_argument(
        "-y",
        "--year",
        type=int,
        default=2022,
        help="Year to filter events by - all events before this year will be removed",
    )

    parser.add_argument(
        "-k",
        "--keywords",
        type=str,
        nargs="+",
        default={"russia": 1, "ukraine": 1},
        help="Keywords to filter events by"
        + "- all events not containing these keywords will be removed",
    )

    parser.add_argument(
        "-w",
        "--window",
        type=int,
        nargs="+",
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
        "-r",
        "--remove",
        type=bool,
        default=True,
        help="Whether to remove keywords from the corpus.",
    )

    return parser.parse_args()


if "__main__" == __name__:
    args = get_args()
    path = args.path
    year = args.year
    keywords = args.keywords
    # keywords = ["russia", "ukraine"]
    context_window_sentence = args.window
    subsequent_paragraphs = args.paragraphs
    join_adjacent_sentences = args.join
    extract_answers = args.answers
    remove_additional_words = args.remove

    print(f"Loading data from {path}")
    events = helper.load_data(path)

    print(f"Filtering events by year {year} and keywords {keywords}")
    events_filtered = process_event.filter_events(events, year=year, keywords=keywords)

    print(f"Creating corpus from {len(events_filtered)} events")

    nlp_model = spacy_stanza.load_pipeline("en", processors="tokenize, lemma")
    nlp_model.add_pipe("sentencizer")

    corpus_df = process_event.events_to_corpus(
        events=events_filtered,
        keywords=keywords,
        context_window_sentence=context_window_sentence,
        subsequent_paragraphs=subsequent_paragraphs,
        join_adjacent_sentences=join_adjacent_sentences,
        extract_answers=extract_answers,
        nlp_model=nlp_model,
        remove_additional_words=remove_additional_words,
    )
    directory = Path(path).parents[0]
    name = str(directory / "corpus")

    print("Saving data to ", name + ".lz4")
    helper.save_data(corpus_df, name, compression=True)