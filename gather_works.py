# -*- coding: utf-8 -*-

DESCRIPTION = """gather works"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
from typing import List, Set, Union
import gzip, json, glob

try:
    from humanfriendly import format_timespan
except ImportError:

    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)


import logging

root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import pandas as pd

def get_publisher(work) -> Union[str, None]:
    primary_location = work.get('primary_location')
    if primary_location:
        host_org = primary_location.get('host_organization')
        if host_org:
            prefix = "https://openalex.org/"
            return host_org.replace(prefix, '')
    return None

def get_countries(work) -> Union[List[str], None]:
    authorships = work.get('authorships')
    if authorships:
        countries = []
        for authorship in authorships:
            institutions = authorship.get('institutions')
            if institutions:
                for institution in institutions:
                    this_country = institution.get('country_code')
                    if this_country:
                        countries.append(this_country)
        if countries:
            # deduplicate
            return list(set(countries))
    return None



def get_row(work, source_ids_matched: Set[int]):
    work_id = work.get('id')
    work_prefix = "https://openalex.org/w"
    if (not work_id) or (work_prefix not in work_id.lower()):
        logger.error(f"invalid work id: {work['id']. Skipping}")
        return None
    work_id = work_id.lower().replace(work_prefix, '')
    try:
        work_id = int(work_id)
    except ValueError:
        logger.error(f"invalid work id: {work['id']. Skipping}")
        return None
    
    in_match_set = None
    source_prefix = "https://openalex.org/s"
    locations = work.get('locations')
    if locations:
        source_ids = []
        for location in locations:
            if location.get('source') and location.get('source').get('id'):
                source_id = location['source']['id']
                if source_prefix not in source_id.lower():
                    logger.error(f"invalid source id: {location['source']['id']} (work id: {work['id']}). Skipping")
                    continue
                source_id = source_id.lower().replace(source_prefix, '')
                try:
                    source_id = int(source_id)
                except ValueError:
                    logger.error(f"invalid source id: {location['source']['id']} (work id: {work['id']}). Skipping")
                    continue
                source_ids.append(source_id)
        source_ids = set(source_ids)
        if len(set.intersection(source_ids, source_ids_matched)):
            in_match_set = True
        else:
            in_match_set = False
    row = {
        'work_id': work_id,
        'in_match_set': in_match_set,
        'language': work.get('language'),
        'publisher': get_publisher(work),
        'type': work.get('type'),
        'type_crossref': work.get('type_crossref'),
        'countries': get_countries(work),
    }
    return row

def yield_output_df(input_dir, source_ids_matched: Set[int], chunksize=5000000):
    rows = []
    jsonl_filenames = list(glob.glob(os.path.join(input_dir, 'data', 'works', '*', '*.gz')))
    jsonl_filenames.reverse()
    for jsonl_file_name in jsonl_filenames:
        logger.info(jsonl_file_name)
        with gzip.open(jsonl_file_name, 'r') as works_jsonl:
            for work_json in works_jsonl:
                if not work_json.strip():
                    continue

                work = json.loads(work_json)
                row = get_row(work, source_ids_matched)
                if row is None:
                    continue
                rows.append(row)
                if len(rows) >= chunksize:
                    yield(pd.DataFrame(rows))
                    rows = []

def get_source_ids_matched(fname: str) -> Set[int]:
    fp = Path(fname)
    txt = fp.read_text()
    source_ids = []
    for item in txt.split('\n'):
        source_id = item.strip().replace('S', '')
        source_ids.append(int(source_id))
    return set(source_ids)

def main(args):
    snapshot_dir = args.input
    output_dir = Path(args.outdir)
    if not output_dir.exists():
        output_dir.mkdir()
    source_ids_matched = get_source_ids_matched(args.source_ids_matched)
    logger.info(f"{len(source_ids_matched)} source_ids_matched found")
    for i, df in enumerate(yield_output_df(snapshot_dir, source_ids_matched)):
        outfp = output_dir.joinpath(f"works_{i:04}.parquet")
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
    parser.add_argument("input", help="snapshot directory")
    parser.add_argument("source_ids_matched", help="file with matched source ids")
    parser.add_argument("outdir", help="output directory for csv files")
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
