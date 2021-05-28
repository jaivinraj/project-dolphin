#!/usr/bin/env python
# coding: utf-8


import logging

import sys

sys.path.append("/app")

# import scraping as sc

import pandas as pd

from jinja2 import Template

from db_utils import get_engine, get_table_creation_query

import geocoding as gc
import os


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


def main(_):

    engine = get_engine(user, password, host)

    # ## Define search parameters

    # In[6]:

    searchname = FLAGS.searchname

    # ## Create View for Addresses to Process

    # In[7]:

    q_view = f"""CREATE OR REPLACE VIEW {searchname}.addresses_to_process AS
    SELECT u.id,u.address
    FROM {searchname}.unique_addresses u
    INNER JOIN
    {searchname}.address_ids_to_process a
    on a.address_id=u.id
    """

    # In[8]:

    with engine.connect() as conn:
        conn.execute(q_view)

    # ## Load data

    # In[9]:

    with engine.connect() as conn:
        df = pd.read_sql(f"SELECT * FROM {searchname}.addresses_to_process", con=conn)

    # In[10]:

    logger.info(f"Loaded {len(df)} addresses to geocode")

    # In[11]:

    df_results = gc.query_addresses(df)

    # In[12]:

    logger.info(f"Geocoded {len(df_results)} addresses")

    # ## Create geocoded table

    # In[13]:

    cols = {
        "latitude": "DECIMAL(10,6)",
        "longitude": "DECIMAL(10,6)",
        "address_id": "INTEGER",
        "geocoded_address": "VARCHAR(256)",
    }

    index_cols = ["address_id"]
    unique_cols = ["address_id"]

    # In[14]:

    create_q = get_table_creation_query(
        "geocoded_addresses", cols, searchname, index_cols, unique_cols
    )

    # In[15]:

    with engine.connect() as conn:
        conn.execute(create_q)

    # ## Add to table

    # In[16]:

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

    # In[17]:

    q_add_invalid = f"""INSERT INTO {searchname}.invalid_addresses (address_id)
    SELECT a.address_id
    FROM   {searchname}.address_ids_to_process a
    WHERE  NOT EXISTS (
    SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
    FROM   {searchname}.geocoded_addresses g
    WHERE  g.address_id = a.address_id
    );
    """

    # In[18]:

    with engine.connect() as conn:
        conn.execute(q_add_invalid)


if __name__ == "__main__":
    app.run(main)
