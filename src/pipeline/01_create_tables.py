"""01_create_tables

This module creates tables in the database to be populated by later processing stages.
"""

from absl import app
from absl import flags

import logging

import sys

import os

sys.path.append("/app")


from db_utils import get_engine, get_table_creation_query


FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")

# specify specs of each table
TABLE_SPECS = {
    "raw_data": {
        "coldict": {
            "price": "INTEGER",
            "type": "VARCHAR(256)",
            "address": "VARCHAR(256)",
            "url": "TEXT",
            "agent_url": "TEXT",
            "postcode": "VARCHAR(32)",
            "number_bedrooms": "INTEGER",
            "search_date": "TIMESTAMP",
        },
        "index_cols": ["url", "search_date", "address"],
        "unique_cols": [],
    },
    "new_data": {
        "coldict": {
            "property_id": "INTEGER",
            "address": "VARCHAR(256)",
            "url": "TEXT",
        },
        "index_cols": ["id", "address", "url"],
        "unique_cols": ["property_id"],
    },
    "unique_addresses": {
        "coldict": {"address": "VARCHAR(256)", "property_id": "INTEGER", "url": "TEXT"},
        "unique_cols": ["address"],
        "index_cols": ["address", "property_id"],
    },
    "completed_addresses": {
        "coldict": {"address_id": "INTEGER"},
        "unique_cols": ["address_id"],
        "index_cols": ["address_id"],
    },
    "invalid_addresses": {
        "coldict": {"address_id": "INTEGER"},
        "unique_cols": ["address_id"],
        "index_cols": ["address_id"],
    },
    "geocoded_addresses": {
        "coldict": {
            "latitude": "DECIMAL(10,6)",
            "longitude": "DECIMAL(10,6)",
            "address_id": "INTEGER",
            "geocoded_address": "VARCHAR(256)",
        },
        "index_cols": ["address_id"],
        "unique_cols": ["address_id"],
    },
    "bng_coords": {
        "coldict": {
            "eastings": "DECIMAL(14,6)",
            "northings": "DECIMAL(14,6)",
            "address_id": "INTEGER",
        },
        "index_cols": ["address_id"],
        "unique_cols": ["address_id"],
    },
    "closest_pois": {
        "coldict": {
            "eastings": "DECIMAL(14,6)",
            "northings": "DECIMAL(14,6)",
            "poi_id": "INTEGER",
            "address_id": "INTEGER",
            "poi_category": "VARCHAR(64)",
        },
        "index_cols": ["poi_id", "address_id", "poi_category"],
        "unique_cols": [],
    },
    "address_nodes": {
        "coldict": {
            "address_id": "INTEGER",
            "osmid": "BIGINT",
        },
        "index_cols": ["address_id", "osmid"],
        "unique_cols": ["address_id"],
    },
    "poi_distances": {
        "coldict": {
            "poi_id": "INTEGER",
            "address_id": "INTEGER",
            "poi_category": "VARCHAR(64)",
            "distance": "DECIMAL(10,3)",
        },
        "index_cols": ["poi_id", "address_id", "poi_category"],
        "unique_cols": [],
    },
}


def main(_):
    engine = get_engine(user, password, host)
    searchname = FLAGS.searchname
    logger.info("Creating schema if doesn't exist...")
    with engine.connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {searchname}")
    for table_name in TABLE_SPECS:
        logger.info(f"Creating table {table_name} if doesn't exist...")
        create_q = get_table_creation_query(
            tablename=table_name, schema=searchname, **TABLE_SPECS[table_name]
        )
        with engine.connect() as conn:
            conn.execute(create_q)


if __name__ == "__main__":
    app.run(main)
