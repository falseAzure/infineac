import argparse
from pathlib import Path

import infineac.file_loader as file_loader
import infineac.process_event as process_event
import infineac.topic_extractor as topic_extractor

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
        help="Path to directory of earnings calls transcripts",
    )

    parser.add_argument(
        "-t",
        "--type",
        type=str,
        default="basic",
        choices=["basic", "inspired"],
        help="Type of BERTopic model to use: basic or inspired",
    )

    return parser.parse_args()


if "__main__" == __name__:
    args = get_args()
    path = args.path

    files = list(Path(path).rglob("*.xml"))
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")

    events = file_loader.load_files_from_xml(files[0:500])

    print("\nFilter events")
    events_filtered = process_event.filter_events(
        events, year=2022, keywords={"russia": 1, "ukraine": 1}
    )

    print(f"Parsing {len(events_filtered)} events")
    docs = process_event.extract_passages_from_events(
        events_filtered, ["russia", "ukraine"]
    )

    print(f"Extracting topics from {len(docs)} documents")
    if args.type == "inspired":
        topics, probs = topic_extractor.bert_inspired(docs)
    elif args.type == "basic":
        topics, probs = topic_extractor.bert_basic(docs)

    print("Writing topics to output.txt\n")
    with open("output.txt", "w") as f:
        f.write("Topics:\n")
        f.write(topics)
        f.write("\n\n\nProbabilities:\n")
        f.write(probs)
