"""This module geocodes addresses into lat/long coordinates"""

from absl import app
from absl import flags

import logging

import sys

sys.path.append("/app")

from db_utils import get_engine, get_table_creation_query

import geocoding as gc
import os


import pandas as pd


FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")

# global logger
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


def main(_):

    engine = get_engine(user, password, host)

    # ## Define search parameters

    searchname = FLAGS.searchname

    # ## Create View for Addresses to Process

    q_view = f"""CREATE OR REPLACE VIEW {searchname}.addresses_to_process AS
    SELECT u.id,u.address
    FROM {searchname}.unique_addresses u
    INNER JOIN
    {searchname}.address_ids_to_process a
    on a.address_id=u.id
    """

    with engine.connect() as conn:
        conn.execute(q_view)

    # ## Load data

    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {searchname}.addresses_to_process", con=conn)

    logger.info(f"Loaded {len(df)} addresses to geocode")

    df_results = gc.query_addresses(df)

    logger.info(f"Geocoded {len(df_results)} addresses")

    # ## Create geocoded table

    # cols = {
    #     "latitude": "DECIMAL(10,6)",
    #     "longitude": "DECIMAL(10,6)",
    #     "address_id": "INTEGER",
    #     "geocoded_address": "VARCHAR(256)",
    # }

    # index_cols = ["address_id"]
    # unique_cols = ["address_id"]

    # create_q = get_table_creation_query(
    #     "geocoded_addresses", cols, searchname, index_cols, unique_cols
    # )

    # with engine.connect() as conn:
    #     conn.execute(create_q)

    # ## Add to table

    with engine.connect() as conn:
        df_results[["id", "address", "latitude", "longitude"]].rename(
            columns={"id": "address_id", "address": "geocoded_address"}
        ).to_sql(
            "geocoded_addresses",
            schema=searchname,
            index=False,
            con=conn,
            if_exists="append",
        )

    # ## Note invalid addresses

    q_add_invalid = f"""INSERT INTO {searchname}.invalid_addresses (address_id)
    SELECT a.address_id
    FROM   {searchname}.address_ids_to_process a
    WHERE  NOT EXISTS (
    SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
    FROM   {searchname}.geocoded_addresses g
    WHERE  g.address_id = a.address_id
    );
    """

    with engine.connect() as conn:
        conn.execute(q_add_invalid)


if __name__ == "__main__":
    app.run(main)
