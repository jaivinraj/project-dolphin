#!/usr/bin/env python
# coding: utf-8

# ## Complete Processing


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
from constants import DATA_PATH
import pickle
import osmnx as ox
import networkx as nx


# In[3]:

from absl import app
from absl import flags

FLAGS = flags.FLAGS


flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")


# global logger
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ## Connect to Database

# In[4]:


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


# In[5]:


def main(_):
    engine = get_engine(user, password, host)

    # ## Define search parameters

    # In[6]:

    searchname = FLAGS.searchname

    # ## Create Temporary Table

    # In[7]:
    logger.info("Creating temporary table to store processed addresses...")
    with engine.connect() as conn:
        conn.execute(
            f"""CREATE TEMPORARY TABLE processed_addresses AS
        SELECT * FROM {searchname}.address_ids_to_process"""
        )

    # In[8]:
    logger.info("Adding temporary table to completed addresses")
    with engine.connect() as conn:
        conn.execute(
            f"""INSERT INTO {searchname}.completed_addresses (address_id)
        SELECT address_id from processed_addresses"""
        )


if __name__ == "__main__":
    app.run(main)
