# -*- coding: utf-8 -*-

DESCRIPTION = """match Scopus ISSNs to openalex"""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)

import logging
root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

import pandas as pd
import numpy as np

from util import entities_by_ids, OpenAlexID

def get_scopus_journals_matched(df):
    _dftmp = df.sort_values('openalex_id').drop_duplicates(subset=['scopus_id'])
    _dftmp['matched_to_openalex'] = df['openalex_id'].notna()
    num_scopus_sources = _dftmp['scopus_id'].nunique()
    num_openalex_sources = _dftmp['openalex_id'].nunique()
    num_scopus_matches = len(_dftmp[_dftmp['matched_to_openalex']==True].drop_duplicates(subset=['scopus_id']))
    logger.info(f"Number of scopus sources: {num_scopus_sources:,}")
    logger.info(f"Number of scopus sources matched to (at least one) OpenAlex source: {num_scopus_matches:,} ({num_scopus_matches/num_scopus_sources:.0%})")
    logger.info(f"Number of OpenAlex sources in the `also_in_scopus` set: {num_openalex_sources:,}")
    return _dftmp

def main(args):
    outdir = Path(args.outdir)
    if not outdir.exists():
        logger.info(f"creating output directory: {outdir}")
        outdir.mkdir()
    mailto = args.mailto
    df_scopus = pd.read_csv(args.scopus)
    df_scopus = df_scopus[df_scopus['Titles discontinued by Scopus due to quality issues'].isna()]
    # df_scopus = df_scopus[['Sourcerecord ID', 'title', 'Print-ISSN', 'E-ISSN', 'Active or Inactive', 'Title history indication', 'Related title to title history indication', 'Coverage', 'Source Type', """Publisher's Name""", """Publisher imprints grouped to main Publisher"""]]
    df_scopus.rename(columns={'Sourcerecord ID': 'scopus_id'}, inplace=True)
    logger.info(f"There are {len(df_scopus)} Scopus sources.")

    df_scopus_issns = df_scopus.melt(id_vars=['scopus_id'], value_vars=['Print-ISSN', 'E-ISSN']).dropna(subset=['value'])
    df_scopus_issns['value'] = df_scopus_issns['value'].apply(lambda x: f'{x[:4]}-{x[-4:]}'.upper())

    # #### ISSNs with duplicate Scopus sources
    # 
    # Strategy (for now): deduplicate, take highest scopus_id

    logger.info(f"There are {df_scopus_issns['value'].duplicated().sum()} ISSNs with duplicate Scopus sources.")
    logger.info(f"Before deduplication, number of scopus sources: {len(df_scopus_issns)}")
    df_scopus_issns = df_scopus_issns.sort_values('scopus_id', ascending=False).drop_duplicates(subset=['value'])
    logger.info(f"After deduplication, number of scopus sources: {len(df_scopus_issns)}")

    # ### Get matches from OpenAlex

    api_endpoint = 'sources'
    issns = df_scopus_issns['value'].apply(lambda x: f'{x[:4]}-{x[-4:]}').tolist()
    params = {
        'mailto': mailto,
        'select': 'id,display_name,issn_l,issn,host_organization,host_organization_name,host_organization_lineage,works_count,cited_by_count,ids,country_code,alternate_titles,abbreviated_title,type'
    }
    issn_matched_sources = []
    num_api_calls = 0
    for r in entities_by_ids(issns, api_endpoint=api_endpoint, filterkey='issn', params=params):
        issn_matched_sources.extend(r.json()['results'])
        num_api_calls += 1
    logger.info(f'{num_api_calls} api calls made')

    data = []
    seen_ids = set()
    for source in issn_matched_sources:
        openalex_source_id = OpenAlexID(source['id'])
        if openalex_source_id.id_short in seen_ids:
            continue
        for issn in source['issn']:
            data.append({
                'openalex_id': openalex_source_id.id_short,
                'openalex_display_name': source['display_name'],
                'openalex_issn': issn,
            })
        seen_ids.add(openalex_source_id.id_short)
    df_openalex_sources = pd.DataFrame(data)


    df_scopus_to_openalex = df_scopus_issns.merge(df_openalex_sources.drop_duplicates(), how='left', left_on='value', right_on='openalex_issn')
    df_scopus_to_openalex = df_scopus_to_openalex.merge(df_scopus, on='scopus_id')
    df_scopus_to_openalex = df_scopus_to_openalex.drop(columns=['variable', 'value', 'openalex_issn']).drop_duplicates()

    get_scopus_journals_matched(df_scopus_to_openalex).to_csv(outdir.joinpath('scopus_sources_matched_and_unmatched.csv'))


    openalex_sources_in_scopus = df_scopus_to_openalex['openalex_id'].dropna().astype(str).unique().tolist()
    outdir.joinpath('openalex_sources_having_scopus_matches.txt').write_text('\n'.join(openalex_sources_in_scopus))

    # # %%
    # data = []
    # for source in issn_matched_sources:
    #     openalex_source_id = OpenAlexID(source['id'])
    #     if openalex_source_id.id_short in openalex_sources_in_scopus:
    #         data.append({
    #             'openalex_id': openalex_source_id.id_short,
    #             'display_name': source['display_name'],
    #             'issn_l': source['issn_l'],
    #             'works_count': source['works_count'],
    #             'cited_by_count': source['cited_by_count'],
    #             'country_code': source['country_code'],
    #             'host_organization_name': source['host_organization_name'],
    #             'source_type': source['type'],
    #         })
    # df_openalex_sources_in_scopus = pd.DataFrame(data)


    # # %%
    # print(df_openalex_sources_in_scopus.shape)
    # df_openalex_sources_in_scopus.drop_duplicates(inplace=True)
    # print(df_openalex_sources_in_scopus.shape)


    # # %%
    # print(f"Number of OpenAlex sources matched in Scopus (by ISSN): {df_openalex_sources_in_scopus['openalex_id'].nunique():,}")
    # print(f"Number of OpenAlex works linked to those sources: {df_openalex_sources_in_scopus['works_count'].sum():,}")
    # print(f"Number of OpenAlex citations linked to those sources: {df_openalex_sources_in_scopus['cited_by_count'].sum():,}")




if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    logger.info("pid: {}".format(os.getpid()))
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("scopus", help="path to scopus sources csv file")
    parser.add_argument("outdir", help="path to output directory")
    parser.add_argument("--mailto", required=True, help="email address to use for OpenAlex API queries")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))