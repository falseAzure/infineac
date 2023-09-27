import argparse
from pathlib import Path

import infineac.file_loader as file_loader
import infineac.helper as helper

main_dir = Path(__file__).resolve().parents[1]
data_dir = main_dir / "data"

PATH = "data/transcripts/"


def get_args():
    parser = argparse.ArgumentParser(
        "This script loads, structures and saves earnings calls as a list of events"
        + "from a given directory of xml files containing the earnings calls"
        + "transcripts."
        + "The saved file is a pickle or lz4 file containing the list of events."
        + "This file can then be used to create a corpus (create_corpus.py)"
        + "for training a topic model (extract_topics.py)."
    )

    parser.add_argument(
        "-p",
        "--path",
        type=str,
        default=PATH,
        help="Path to directory of xml files containing the earnings calls transcripts",
    )

    parser.add_argument(
        "-c",
        "--compress",
        type=bool,
        default=False,
        help="Whether to compress the pickle file with lz4",
    )

    return parser.parse_args()


if "__main__" == __name__:
    args = get_args()
    path = args.path

    files = list(Path(path).rglob("*.xml"))[0:500]
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")
    events = file_loader.load_files_from_xml(files)

    name = str(data_dir / "events")
    if args.compress:
        file_ending = ".lz4"
    else:
        file_ending = ".pickle"
    print("Saving data to ", name + file_ending)
    helper.save_data(events, name, compression=args.compress)
