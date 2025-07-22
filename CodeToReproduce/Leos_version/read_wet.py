"""Python script to download wet files from the common crawl, extract the relevant information and manage the resulting csv files"""

import argparse
import csv
import glob
import logging
import os

import pandas as pd

from datetime import datetime
from urllib.request import urlretrieve
from resiliparse.extract.html2text import extract_plain_text
from resiliparse.parse.html import HTMLTree
from resiliparse.parse.encoding import detect_encoding
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator

from helper_functions import (
    Bristol_postcode_finder,
    count_lines,
    construct_output_filename,
    decompress_gzip,
    extract_website,
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parsing arguments function"""
    parser = argparse.ArgumentParser(description="Reading one wet.paths file")
    parser.add_argument(
        "--wet_file",
        type=str,
        default="wet.paths",
        help="filename where the wet files are",
    )
    parser.add_argument(
        "--outputs_dir",
        type=str,
        default="outputs/",
        help="outputs directory where segments will be downloaded and processed",
    )
    parser.add_argument(
        "--num_chunks",
        type=int,
        default=1,
        help="Number of chunks. The total number of lines of the wet file should be divisible by the number of chunks",
    )
    parser.add_argument(
        "--server",
        type=str,
        default="https://data.commoncrawl.org/",
        help="server from where we download wet files",
    )
    parser.add_argument(
        "--crawl",
        type=str,
        default="202350",
        help="Crawl number - it normally follows the structure year + two digits (e.g. 202350)",
    )
    parser.add_argument(
        "--postcode_list",
        type=str,
        default="BristolPostcodeLookup.csv",
        help="File with postcodes list",
    )
    pars_args = parser.parse_args()
    print("the inputs are:")
    for arg in vars(pars_args):
        print(f"{arg} is {getattr(pars_args, arg)}")
    return pars_args


def get_text_original(raw_bytes):
    return raw_bytes.decode("utf-8", "ignore")


def get_text_tree(raw_bytes):
    enc = detect_encoding(raw_bytes)
    try:
        tree = HTMLTree.parse_from_bytes(raw_bytes, encoding=enc)
        text = tree.body()
    except Exception:
        text = ""
    return text


def get_text_html2text(raw_bytes):
    text = extract_plain_text(
        raw_bytes,
        list_bullets=False,
    )
    return text


def extract_from_segment(
    output_filename, csv_filename, url_crawl, BristolPostcodeLookup
):
    """Function to extract text and postcode of websites and write it in the csv file"""

    with open(csv_filename, "w", newline="") as output_csv:
        csv_writer = csv.writer(output_csv)

        # open the file, naming the reader "stream"
        with open(output_filename, "rb") as stream:
            # loop over each record within "stream" using the ArchiveIterator from warcio
            for record in ArchiveIterator(stream):
                # Check if the current record has the type "response" - conversion as wet file
                if record.rec_type == "conversion":
                    # lookup the uri (web address) of the record
                    uri = record.rec_headers.get_header("WARC-Target-URI")
                    language = record.rec_headers.get_header(
                        "WARC-Identified-Content-Language"
                    )

                    # check if the web address contains ".co.uk/" and the language is English
                    if (".co.uk/" in uri) & (language == "eng"):
                        website = extract_website(uri)
                        text = record.content_stream().read().decode("utf-8", "ignore")
                        postcodes = Bristol_postcode_finder(
                            text, BristolPostcodeLookup
                        )  # do postcode search for Bristol postcodes
                        text = text.lower()  # all into lowercase

                        if (
                            postcodes is not None
                        ):  # Check if there are Bristol postcodes
                            csv_writer.writerow(
                                [uri, website, postcodes, url_crawl, text]
                            )  ##cclocation


def merge_csvs(crawl, output_dir):
    """function to merge all the csvs from the different segments"""

    csv_pattern = f"{output_dir}crawldata{crawl}segment*.csv"
    csv_files = sorted(glob.glob(csv_pattern))
    if not csv_files:
        raise FileNotFoundError(f"No files matched pattern: {csv_pattern}")

    output_name = os.path.join(output_dir, f"df{crawl}.csv")

    logger.info(f"Merging {len(csv_files)} files...")

    with open(output_name, mode="w", newline="") as output_csv_file:
        output_csv_writer = csv.writer(output_csv_file)
        output_csv_writer.writerow(
            ["url", "parent_url", "postcodes", "cc_url", "content"]
        )
        for file in csv_files:
            with open(file, "r") as csv_file:
                csv_reader = csv.reader(csv_file)
                # Iterate over each row in the CSV file and write it to the output CSV
                for row in csv_reader:
                    output_csv_writer.writerow(row)
    logger.info(f"Saved merged csv to {output_name}")

    logger.info("Deleting segment csvs")
    for file in csv_files:
        os.remove(file)


def main(
    wet_paths_filename,
    server,
    crawl,
    output_dir,
    BristolPostcodeLookup,
):
    """Function to download wet files, and extract and process information"""

    start = datetime.now()
    datetime_str = str(start)

    # Setup logging
    logger.setLevel(logging.DEBUG)
    log_dir = f"{output_dir}/logs"
    os.makedirs(log_dir, exist_ok=True)
    handler = logging.FileHandler(f"{log_dir}/{datetime_str}.log")
    logger.addHandler(handler)

    logger.info("---Reading wet paths---")
    num_lines = count_lines(wet_paths_filename)
    logger.info(f"Reading {num_lines} wet files")
    with open(wet_paths_filename, "r", encoding="utf-8") as wet_paths:
        for wet_path in tqdm(wet_paths):
            wet_path = wet_path.strip()
            url_crawl = server + wet_path

            # .wet filename
            output_filename = construct_output_filename(wet_path, crawl)
            if output_filename is None:
                continue
            output_filename = os.path.join(output_dir, output_filename)

            csv_name = output_filename.replace(".wet", ".csv")
            if os.path.exists(csv_name):
                continue

            # Download
            urlretrieve(url_crawl, output_filename + ".gz")

            # Decompress
            decompress_gzip(output_filename + ".gz", output_filename)

            # Extract texts to csv file
            extract_from_segment(
                output_filename, csv_name, url_crawl, BristolPostcodeLookup
            )

            # Remove .wet file
            os.remove(output_filename)
    logger.info("Finished downloading and extracting wet files")
    logger.info("Merging csvs")
    merge_csvs(crawl, output_dir)

    end = datetime.now()
    run_time = end - start
    logger.info("--End of Script-------")
    logger.info(f"running time: {run_time}")


if __name__ == "__main__":
    args = parse_args()

    wet_paths_filename = args.wet_file
    num_chunks = args.num_chunks
    server = args.server
    crawl = args.crawl
    postcode_list = args.postcode_list
    output_dir = args.outputs_dir

    BristolPostcodeLookup = pd.read_csv(postcode_list)

    num_lines = count_lines(wet_paths_filename)
    if num_lines % num_chunks != 0:
        raise ValueError(
            "The number of lines in the wet file should be divisible by the number of chunks"
        )

    main(wet_paths_filename, server, crawl, output_dir, BristolPostcodeLookup)
