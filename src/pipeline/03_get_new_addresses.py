#!/usr/bin/env python
# coding: utf-8


import logging

import sys

sys.path.append("/app")

import pandas as pd

from jinja2 import Template

from db_utils import get_engine, get_table_creation_query
import os


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")
# In[3]:


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
    searchname = FLAGS.searchname

    cols = {"address": "VARCHAR(256)", "property_id": "INTEGER", "url": "TEXT"}

    unique_cols = ["address"]

    index_cols = ["address", "property_id"]

    logger.info("Creating unique addresses table...")

    create_q = get_table_creation_query(
        "unique_addresses", cols, searchname, index_cols, unique_cols
    )

    # In[9]:

    with engine.connect() as conn:
        conn.execute(create_q)

    # ## Merge into Unique Addresses Table

    logger.info("Merging in new addresses...")
    q_merge_in_new = f"""INSERT INTO {searchname}.unique_addresses (address,property_id,url)
    select distinct on (address) address,id,url
    FROM {searchname}.new_data n
    ORDER BY address,id desc
    ON CONFLICT (address)
    DO UPDATE
    SET
        property_id=EXCLUDED.property_id,
        url=EXCLUDED.url
    """

    with engine.connect() as conn:
        conn.execute(q_merge_in_new)

    cols = {"address_id": "INTEGER"}

    unique_cols = ["address_id"]

    index_cols = ["address_id"]

    create_q = get_table_creation_query(
        "completed_addresses", cols, searchname, index_cols, unique_cols
    )

    # In[14]:

    with engine.connect() as conn:
        conn.execute(create_q)

    # ### Invalid table

    # In[15]:

    create_q = get_table_creation_query(
        "invalid_addresses", cols, searchname, index_cols, unique_cols
    )

    # In[16]:

    with engine.connect() as conn:
        conn.execute(create_q)

    # ## Create view for unprocessed addresses

    # In[17]:

    q_view = f"""CREATE OR REPLACE VIEW {searchname}.address_ids_to_process AS
    SELECT u.id as address_id
    FROM {searchname}.unique_addresses u
    WHERE
        (
        NOT EXISTS (
        SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
        FROM   {searchname}.completed_addresses c
        WHERE  c.address_id = u.id
        )
        )
        AND
        (
        NOT EXISTS (
        SELECT  -- SELECT list mostly irrelevant; can just be empty in Postgres
        FROM   {searchname}.invalid_addresses i
        WHERE  i.address_id = u.id
        )
        )
    """

    with engine.connect() as conn:
        conn.execute(q_view)


if __name__ == "__main__":
    app.run(main)
