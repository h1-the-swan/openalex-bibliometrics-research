# -*- coding: utf-8 -*-

DESCRIPTION = """gather works, using snapshot files directly from s3"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
from typing import List, Set, Union, Iterable, Optional
import gzip, json, glob
from io import BytesIO

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import boto3
import pandas as pd


def get_source_type(work) -> Union[str, None]:
    primary_location = work.get("primary_location")
    if primary_location:
        source = primary_location.get("source")
        if source:
            return source.get("type")
    return None


def get_publisher(work) -> Union[str, None]:
    primary_location = work.get("primary_location")
    if primary_location:
        source = primary_location.get("source")
        if source:
            return source.get("publisher")
    return None


def get_countries(work) -> Union[List[str], None]:
    authorships = work.get("authorships")
    if authorships:
        countries = []
        for authorship in authorships:
            this_countries = authorship.get("countries")
            if this_countries:
                for country in this_countries:
                    countries.append(country)
        if countries:
            # deduplicate
            return list(set(countries))
    return None


def get_row(work, source_ids_matched: Set[int]):
    work_id = work.get("id")
    work_prefix = "https://openalex.org/w"
    if (not work_id) or (work_prefix not in work_id.lower()):
        logger.error(f"invalid work id: {work['id']. Skipping}")
        return None
    work_id = work_id.lower().replace(work_prefix, "")
    try:
        work_id = int(work_id)
    except ValueError:
        logger.error(f"invalid work id: {work['id']. Skipping}")
        return None

    in_match_set = None
    source_prefix = "https://openalex.org/s"
    locations = work.get("locations")
    if locations:
        source_ids = []
        for location in locations:
            if location.get("source") and location.get("source").get("id"):
                source_id = location["source"]["id"]
                if source_prefix not in source_id.lower():
                    logger.error(
                        f"invalid source id: {location['source']['id']} (work id: {work['id']}). Skipping"
                    )
                    continue
                source_id = source_id.lower().replace(source_prefix, "")
                try:
                    source_id = int(source_id)
                except ValueError:
                    logger.error(
                        f"invalid source id: {location['source']['id']} (work id: {work['id']}). Skipping"
                    )
                    continue
                source_ids.append(source_id)
        source_ids = set(source_ids)
        if len(set.intersection(source_ids, source_ids_matched)):
            in_match_set = True
        else:
            in_match_set = False
    row = {
        "work_id": work_id,
        "in_match_set": in_match_set,
        "publication_year": work.get("publication_year"),
        "language": work.get("language"),
        "publisher": get_publisher(work),
        "type": work.get("type"),
        "type_crossref": work.get("type_crossref"),
        "countries": get_countries(work),
        "referenced_works_count": work.get("referenced_works_count"),
        "authors_count": work.get("authors_count"),
        "cited_by_count": work.get("cited_by_count"),
        "institutions_distinct_count": work.get("institutions_distinct_count"),
        "is_oa": work.get("open_access", {}).get("is_oa"),
        "oa_status": work.get("open_access", {}).get("oa_status"),
        "is_retracted": work.get("is_retracted"),
        "source_type": get_source_type(work),
        "has_doi": work.get("doi", None) is not None,
    }
    return row


def yield_output_df(
    snapshot_date,
    source_ids_matched: Set[int],
    files_to_ignore: Optional[Iterable] = None,
    chunksize=1000000,
):
    rows = []
    if files_to_ignore is None:
        files_to_ignore = []
    if snapshot_date == "current":
        bucket_name = "openalex"
        prefix = "data/works"
    else:
        bucket_name = "openalex-sandbox"
        prefix = f"snapshot-backups/openalex-jsonl/{snapshot_date}/data/works"
    logger.info(f"getting data from s3 bucket: {bucket_name}")
    bucket = boto3.resource("s3").Bucket(bucket_name)
    # jsonl_filenames = list(glob.glob(os.path.join(input_dir, 'data', 'works', '*', '*.gz')))
    # jsonl_filenames.reverse()
    for obj_summary in bucket.objects.filter(
        Prefix=prefix
    ):
        if obj_summary.key.endswith(".gz"):
            if obj_summary.key in files_to_ignore:
                logger.info(f"skipping object: {obj_summary.key}")
                continue
            logger.info(f"reading object: {obj_summary.key}")
            with obj_summary.get()["Body"] as body:
                content = body.read()
                with gzip.open(BytesIO(content), "rb") as gzip_file:
                    for work_json in gzip_file:
                        if not work_json.strip():
                            continue

                        work = json.loads(work_json)
                        row = get_row(work, source_ids_matched)
                        if row is None:
                            continue
                        rows.append(row)
                        if len(rows) >= chunksize:
                            yield (pd.DataFrame(rows))
                            rows = []
    # final data
    if len(rows) > 0:
        yield pd.DataFrame(rows)


def get_source_ids_matched(fname: str) -> Set[int]:
    fp = Path(fname)
    txt = fp.read_text()
    source_ids = []
    for item in txt.split("\n"):
        source_id = item.strip().replace("S", "")
        source_ids.append(int(source_id))
    return set(source_ids)


def main(args):
    snapshot_date = args.snapshot_date
    output_dir = Path(args.outdir)
    if not output_dir.exists():
        output_dir.mkdir()
    source_ids_matched = get_source_ids_matched(args.source_ids_matched)
    logger.info(f"{len(source_ids_matched)} source_ids_matched found")

    files_to_ignore = []
    if args.ignore:
        for line in Path(args.ignore).read_text().split("\n"):
            files_to_ignore.append(line.strip())
    logger.info(f"{len(files_to_ignore)} files to ignore")

    file_idx = 0
    for df in yield_output_df(
        snapshot_date, source_ids_matched, files_to_ignore=files_to_ignore
    ):
        while True:
            # don't overwrite existing files:
            # increment file_idx until we get a filename that doesn't already exist
            outfp = output_dir.joinpath(f"works_{file_idx:04}.parquet")
            if not outfp.exists():
                break
            file_idx += 1
        logger.info(f"writing to {outfp}")
        df.to_parquet(outfp, index=False)


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info("{:%Y-%m-%d %H:%M:%S}".format(datetime.now()))
    logger.info("pid: {}".format(os.getpid()))
    import argparse

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "snapshot_date", default="2023-11-20", help="openalex snapshot date"
    )
    parser.add_argument("source_ids_matched", help="file with matched source ids")
    parser.add_argument("outdir", help="output directory for csv files")
    parser.add_argument(
        "--ignore",
        help="path to file with newline separated filenames to ignore when reading the snapshot files",
    )
    parser.add_argument("--debug", action="store_true", help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug("debug mode is on")
    main(args)
    total_end = timer()
    logger.info(
        "all finished. total time: {}".format(format_timespan(total_end - total_start))
    )
