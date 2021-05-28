"""This module snaps properties to the closest node on the walking network."""

from absl import app
from absl import flags

import logging

import sys

sys.path.append("/app")


import pandas as pd


from db_utils import get_engine, get_table_creation_query

import dataloader as loader


import numpy as np
import os

import geopandas as gpd
from network_utils import get_closest_osmids

logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")


# ## Connect to Database


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


def main(_):
    engine = get_engine(user, password, host)

    # ## Define search parameters

    searchname = FLAGS.searchname

    # ## Load data

    q_addresses = f"""SELECT c.* FROM 
    {searchname}.bng_coords c
    INNER JOIN
    {searchname}.address_ids_to_process a
    ON c.address_id=a.address_id"""

    logger.info("Loading road nodes...")
    with engine.connect() as conn:
        df_nodes = pd.read_sql("SELECT * FROM node_coords", con=conn)
    logger.info("Loading addressses...")
    df_addresses = loader.load_sql(q_addresses, user=user, password=password, host=host)

    # ## Snap to network

    coords_nodes = df_nodes[["eastings", "northings"]].values

    logger.info("Getting closest nodes")
    closest_osmids = get_closest_osmids(
        df_addresses[["eastings", "northings"]].values,
        coords_nodes,
        df_nodes.osmid.values,
    )

    output = pd.DataFrame(
        {
            "address_id": df_addresses.address_id,
            "osmid": closest_osmids,
        }
    )

    # cols = {
    #     "address_id": "INTEGER",
    #     "osmid": "BIGINT",
    # }

    # index_cols = ["address_id", "osmid"]
    # unique_cols = ["address_id"]

    # create_q = get_table_creation_query(
    #     "address_nodes", cols, searchname, index_cols, unique_cols
    # )

    # with engine.connect() as conn:
    #     conn.execute(create_q)

    logger.info("Saving outputs")
    with engine.connect() as conn:
        output.to_sql(
            "address_nodes",
            schema=searchname,
            index=False,
            con=conn,
            if_exists="append",
        )


if __name__ == "__main__":
    app.run(main)
