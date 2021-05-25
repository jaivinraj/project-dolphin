from rightmove_webscraper import RightmoveData


import logging

import os


# logging config
# global logger
logger = logging.getLogger(__name__)
# handler = logging.StreamHandler()
# formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
# handler.setFormatter(formatter)
# logger.addHandler(handler)
# logger.setLevel(logging.INFO)


def scrape_rightmove(url):
    """Scrape data from RightMove and write to sqlite database"""
    logger.info("Starting RightMove scraping")
    logger.info("Scraping from RightMove...")
    rm = RightmoveData(url)
    logger.info("Fetched results")
    df = rm.get_results
    return df
    # if os.path.exists(db_path):
    #     raise Exception("This searchname already exists.Please choose another.")
    # logger.info("Writing to database...")
    # with sql.connect(db_path) as conn:
    #     df.reset_index().rename(columns={"index": "id"}).to_sql(
    #         "raw_property_data", con=conn
    #     )
    # logger.info("Completed writing to database")