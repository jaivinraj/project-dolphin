#!/usr/bin/env python
# coding: utf-8

# # Snap to the Road Network


import logging

import sys

sys.path.append("/app")

# import scraping as sc

import pandas as pd

from jinja2 import Template

from db_utils import get_engine, get_table_creation_query

from gis_utils import get_gdf_coords, get_closest_idxs, get_closest_pois_slow


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

import logging
from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")


# ## Connect to Database

# In[3]:


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


# In[4]:


def main(_):
    engine = get_engine(user, password, host)

    # ## Define search parameters

    # In[5]:

    searchname = FLAGS.searchname

    # ## Load data

    # In[6]:

    q_addresses = f"""SELECT c.* FROM 
    {searchname}.bng_coords c
    INNER JOIN
    {searchname}.address_ids_to_process a
    ON c.address_id=a.address_id"""

    # In[7]:
    logger.info("Loading road nodes...")
    with engine.connect() as conn:
        df_nodes = pd.read_sql("SELECT * FROM node_coords", con=conn)
    # In[8]:
    logger.info("Loading addressses...")
    df_addresses = loader.load_sql(q_addresses, user=user, password=password, host=host)

    # ## Snap to network

    # In[9]:
    coords_nodes = df_nodes[["eastings", "northings"]].values

    # In[10]:
    logger.info("Getting closest nodes")
    closest_osmids = get_closest_osmids(
        df_addresses[["eastings", "northings"]].values,
        coords_nodes,
        df_nodes.osmid.values,
    )

    # In[11]:

    output = pd.DataFrame(
        {
            "address_id": df_addresses.address_id,
            "osmid": closest_osmids,
        }
    )

    # In[12]:

    cols = {
        "address_id": "INTEGER",
        "osmid": "BIGINT",
    }

    index_cols = ["address_id", "osmid"]
    unique_cols = ["address_id"]

    # In[13]:

    create_q = get_table_creation_query(
        "address_nodes", cols, searchname, index_cols, unique_cols
    )

    # In[14]:

    with engine.connect() as conn:
        conn.execute(create_q)

    # In[15]:
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
