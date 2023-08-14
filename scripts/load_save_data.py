import argparse
from pathlib import Path

import infineac.file_loader as file_loader
import infineac.helper as helper

main_dir = Path(__file__).resolve().parents[1]
data_dir = main_dir / "data"

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
        "-c",
        "--compress",
        type=bool,
        default=False,
        help="Whether to compress the pickle file",
    )

    return parser.parse_args()


if "__main__" == __name__:
    args = get_args()
    path = args.path

    files = list(Path(path).rglob("*.xml"))
    print(f"Found {len(files)} files\n")
    print(f"Loading files from {files[0]} to {files[len(files) - 1]}")

    events = file_loader.load_files_from_xml(files[0:500])

    name = str(data_dir / "events")
    print("Saving data to pickle file")
    helper.save_data(events, name, compression=args.compress)
