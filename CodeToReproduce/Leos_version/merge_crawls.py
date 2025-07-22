"""Script to merge the different crawls"""

import argparse
import glob
import logging
import os

import pandas as pd


logger = logging.getLogger(__name__)


def parse_args():
    """Parsing arguments function"""
    parser = argparse.ArgumentParser(description="Summarization graph")
    parser.add_argument("--year", type=str, default="2021", help="year to process")
    parser.add_argument(
        "--chunksize",
        type=int,
        default=50000,
        help="Chunk size to process csvs",
    )
    parser.add_argument(
        "--database_path",
        type=str,
        default="rdsf_internet_archive/CC-filtering/",
        help="path to csvs",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="processed_ccs/",
        help="path to outputs",
    )
    pars_args = parser.parse_args()

    print("the inputs are:")
    for arg in vars(pars_args):
        print(f"{arg} is {getattr(pars_args, arg)}")
    return pars_args


def clean_and_count_pcs(pcs_str):
    """Clean and count postcodes"""

    pcs_list = pcs_str.split(",")
    pcs_unique = sorted(set([s.strip() for s in pcs_list.split(",")]))
    pcs_cleaned = ",".join(pcs_unique)
    return pcs_cleaned, len(pcs_unique)


def process_chunk(chunk):
    chunk.columns = ["url", "parent_url", "postcodes", "cc_url", "content"]

    # Collapse pcs by domain
    chunk["postcodes"] = chunk.groupby("domain")["postcodes"].transform(
        lambda x: ",".join(x.astype(str))
    )

    # Filter landing pages (only .co.uk/)
    chunk = chunk[chunk["url"].str.contains(r"\.co\.uk/$", regex=True, na=False)]

    # Drop duplicates by domain
    chunk = chunk.drop_duplicates(subset="url", keep="first")

    # Clean pcs
    chunk["postcodes"] = chunk["postcodes"].str.replace(r"[\[\]']", "", regex=True)
    chunk["postcodes"] = chunk["postcodes"].str.replace(", ", ",", regex=False)

    # Remove duplicate postcodes and count
    chunk[["postcodes", "postcodes.count"]] = chunk.apply(
        lambda row: pd.Series(clean_and_count_pcs(row["postcodes"])), axis=1
    )

    return chunk


def process_large_csv(path, chunksize):
    results = []

    for chunk in pd.read_csv(path, header=None, chunksize=chunksize):
        processed = process_chunk(chunk)
        results.append(processed)

    return pd.concat(results).drop_duplicates(subset="url")


def process_multiple_csvs(folder_path, year, output_csv_path=None, chunksize=50000):
    # Find CSV files starting with the year
    pattern = os.path.join(folder_path, f"df{year}*.csv")
    matching_files = glob.glob(pattern)

    if not matching_files:
        print(f"No files found for year {year} in {folder_path}")
        return

    all_processed = []

    for file_path in matching_files:
        print(f"Processing {file_path}...")
        processed_df = process_large_csv(file_path, chunksize)
        all_processed.append(processed_df)

    final_df = pd.concat(all_processed).drop_duplicates(subset="url")

    # Construct default output path if not provided
    if output_csv_path is None:
        output_csv_path = os.path.join(folder_path, f"landing_pages_cleaned_{year}.csv")
    else:
        os.makedirs(output_csv_path, exist_ok=True)
        output_csv_path = os.path.join(
            output_csv_path, f"landing_pages_cleaned_{year}.csv"
        )

    final_df.to_csv(output_csv_path, index=False)
    print(f"Saved merged output to: {output_csv_path}")


if __name__ == "__main__":
    args = parse_args()

    folder_path = args.database_path
    year = args.year
    output_csv_path = args.output_path
    chunksize = args.chunksize

    process_multiple_csvs(folder_path, year, output_csv_path, chunksize)
