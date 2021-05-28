"""This module converts new addresses to British National Grid (BNG) coordinates."""

from absl import app
from absl import flags

import logging

import sys

sys.path.append("/app")

# import scraping as sc

import pandas as pd

from jinja2 import Template

from db_utils import get_engine, get_table_creation_query

from gis_utils import convert_bng
import os


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

    # ## Define search parameters

    searchname = FLAGS.searchname

    # ## Load data

    q_load = f"""SELECT g.* FROM 
    {searchname}.geocoded_addresses g
    INNER JOIN
    {searchname}.address_ids_to_process a
    ON g.address_id=a.address_id"""

    with engine.connect() as conn:
        df = pd.read_sql(q_load, con=conn)

    logger.info(f"Loaded {len(df)} addresses to convert")

    # ## Perform Conversion

    gdf_bng = convert_bng(df)

    # ## Create table

    # cols = {
    #     "eastings": "DECIMAL(14,6)",
    #     "northings": "DECIMAL(14,6)",
    #     "address_id": "INTEGER",
    # }

    # index_cols = ["address_id"]
    # unique_cols = ["address_id"]

    # create_q = get_table_creation_query(
    #     "bng_coords", cols, searchname, index_cols, unique_cols
    # )

    # with engine.connect() as conn:
    #     conn.execute(create_q)

    # ## Save results

    with engine.connect() as conn:
        gdf_bng[["address_id", "eastings", "northings"]].to_sql(
            "bng_coords", schema=searchname, index=False, if_exists="append", con=conn
        )


if __name__ == "__main__":
    app.run(main)
