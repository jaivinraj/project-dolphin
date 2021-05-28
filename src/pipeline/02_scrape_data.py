#!/usr/bin/env python
# coding: utf-8

# # Scrape Data from Rightmove


import logging

import sys

sys.path.append("/app")

import scraping as sc

import pandas as pd

from jinja2 import Template

import os

from db_utils import get_engine, get_table_creation_query


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


from absl import app
from absl import flags

FLAGS = flags.FLAGS

flags.DEFINE_string(
    "url",
    "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=REGION%5E70331&maxBedrooms=1&minBedrooms=0&maxPrice=2000&minPrice=500&propertyTypes=&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=furnished&keywords=%22",
    "URL for RightMove search",
)

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")


def main(_):

    engine = get_engine(user, password, host)

    # ## Define search parameters

    # In[6]:

    url = FLAGS.url

    # In[7]:

    searchname = FLAGS.searchname

    # In[8]:

    tablename = "raw_data"

    # ## Create table
    cols = {
        "price": "INTEGER",
        "type": "VARCHAR(256)",
        "address": "VARCHAR(256)",
        "url": "TEXT",
        "agent_url": "TEXT",
        "postcode": "VARCHAR(32)",
        "number_bedrooms": "INTEGER",
        "search_date": "TIMESTAMP",
    }

    index_cols = ["url", "search_date", "address"]

    # In[10]:

    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {searchname}")
        conn.execute(get_table_creation_query(tablename, cols, searchname, index_cols))

    # ## Scrape Data
    df = sc.scrape_rightmove(url)

    # ## Save to Database

    # In[12]:

    with engine.connect() as conn:
        df.to_sql(
            tablename, schema=searchname, index=False, con=conn, if_exists="append"
        )

    # ## Create table for new data

    # In[13]:

    cols_newdata = {
        "property_id": "INTEGER",
        "address": "VARCHAR(256)",
        "url": "TEXT",
    }

    # In[14]:

    index_cols_newdata = ["id", "address", "url"]

    # In[15]:

    with engine.connect() as conn:
        #     conn.execute(f"CREATE SCHEMA IF NOT EXISTS {searchname}")
        conn.execute(
            get_table_creation_query(
                "new_data", cols_newdata, searchname, index_cols_newdata
            )
        )

    # ## Populate new data table

    # In[16]:

    with engine.connect() as conn:
        conn.execute(f"DELETE FROM {searchname}.new_data")
        conn.execute(
            f"""INSERT INTO {searchname}.new_data (property_id,url,address)
            SELECT rd.id AS property_id,rd.url,rd.address

    FROM {searchname}.{tablename} rd

    INNER JOIN (SELECT MAX(search_date) AS Maxsearch_date

        FROM {searchname}.{tablename}

    ) groupedrd

    ON rd.search_date = groupedrd.Maxsearch_date""",
        )

    # with engine.connect() as conn:
    #     conn.execute(f"DROP SCHEMA {searchname} CASCADE")


if __name__ == "__main__":
    app.run(main)
