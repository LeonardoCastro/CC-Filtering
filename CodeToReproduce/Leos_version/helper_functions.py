"""Script with helper functions for wet downloader routines"""

import gzip
import os
import re
import shutil


def Bristol_postcode_finder(text, BristolPostcodeLookup):
    """Finder of Bristol postcodes"""

    postcodes = postcode_finder(text)
    # make sure matches begin with "BS"
    postcodes = [postcode for postcode in postcodes if postcode.startswith("BS")]
    # Filter postcodes to only include those found in BristolPostcodeLookup['pcds']
    matches = [
        postcode
        for postcode in postcodes
        if postcode in BristolPostcodeLookup["pcds"].values
    ]
    if matches:
        return matches


def count_lines(filepath):
    """function to count the lines of wet paths in a wet.paths file"""

    count = 0
    if ".gz" not in filepath:
        with open(filepath, "r", encoding="utf-8") as file:
            for _ in file:
                count += 1
    else:
        with gzip.open(filepath, "rt", encoding="utf-8") as file:
            for _ in file:
                count += 1
    return count


def construct_output_filename(wet_path, crawl):
    """function to extract segment from wet path and create an output filename"""
    match = re.search(r"(\d{5})(?=\.warc\.wet\.gz)", wet_path)
    if match:
        segment = match.group(1)
        return f"crawldata{crawl}segment{segment}.wet"
    else:
        return None


def decompress_gzip(filename_gz, out_filename):
    """function to decompress the gz file"""

    with gzip.open(filename_gz, "rb") as f_in:
        with open(out_filename, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(filename_gz)


def extract_website(url):
    """Exctract website from url"""

    website = re.sub(r"\.uk/.*", ".uk", url)  # removes everything after .uk/ and the /
    website = website.replace("https://", "").replace("http://", "")
    return website


def postcode_finder(text):
    """UK postcode finder (AB12C 3DE)"""

    postcodes = re.findall(
        r"\b[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2}\b", text
    )
    # https://stackoverflow.com/questions/378157/python-regular-expression-postcode-search
    return list(set(postcodes))
