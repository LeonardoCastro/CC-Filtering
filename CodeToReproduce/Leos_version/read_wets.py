""""""

import argparse
import csv
import glob
import logging
import os
import re

from datetime import datetime
from urllib.request import urlretrieve
from tqdm import tqdm
from warcio.archiveiterator import ArchiveIterator

from helper_functions import (
    Bristol_postcode_finder,
    construct_output_filename,
    decompress_gzip,
    extract_website,
)


logger = logging.getLogger(__name__)


def parse_args():
    """Parsing arguments function"""
    parser = argparse.ArgumentParser(description="Reading a list of wet.paths by year")
    parser.add_argument(
        "--year",
        type=int,
        help="Year of the crawls that will be downloaded",
    )
    parser.add_argument(
        "--wet_paths_dir",
        type=str,
        default="wet_paths/",
        help="directory where wet.paths are found",
    )
    parser.add_argument(
        "--outputs_dir",
        type=str,
        default="outputs/",
        help="outputs directory where segments will be downloaded and processed",
    )
    parser.add_argument(
        "--server",
        type=str,
        default="https://data.commoncrawl.org/",
        help="server from where we download wet files",
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


def processing_wet_path(
    wet_paths_filename, server, crawl, output_dir, BristolPostcodeLookup
):
    """Function to process one wet.path file"""
    with open(wet_paths_filename, "r", encoding="utf-8") as wet_paths:
        for wet_path in wet_paths:
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


def get_crawl_from_text(year, string):
    """Functino to obtain the crawl number of a string"""
    pattern = f"{year}\d{2}"
    match = re.search(pattern, string)
    if match:
        return match.group(0)
    else:
        return None


def process_wet_paths_by_year(
    path_to_wet_paths, year, server, output_dir, BristolPostcodeLookup
):
    """
    Function to process the wet paths of a given year.
    The wet.paths filenames should include the crawls e.g. 202350_wet.paths
    """

    wet_paths_pattern = f"{path_to_wet_paths}{year}*wet.paths"
    wet_paths = sorted(glob.glob(wet_paths_pattern))

    logger.info(f"--Starting extraction of wet paths for {year}---")
    logger.info(f"There are {len(wet_paths)} files")
    for wet_path in tqdm(wet_paths):
        crawl = get_crawl_from_text(year, wet_path)
        if crawl is None:
            continue
        processing_wet_path(wet_path, server, crawl, output_dir, BristolPostcodeLookup)
        merge_csvs(crawl, output_dir)


if __name__ == "__main__":
    args = parse_args()

    path_to_wet_paths = args.wet_paths_dir
    year = args.year
    server = args.server
    output_dir = args.output_dir
    BristolPostcodeLookup = args.postcode_list

    start = datetime.now()
    datetime_str = str(start)

    # Setup logging
    logger.setLevel(logging.DEBUG)
    log_dir = f"{output_dir}/logs"
    os.makedirs(log_dir, exist_ok=True)
    handler = logging.FileHandler(f"{log_dir}/{datetime_str}.log")
    logger.addHandler(handler)

    process_wet_paths_by_year(
        path_to_wet_paths, year, server, output_dir, BristolPostcodeLookup
    )

    end = datetime.now()
    run_time = end - start
    logger.info("---End of Script---")
    logger.info(f"Running time: {run_time}")
