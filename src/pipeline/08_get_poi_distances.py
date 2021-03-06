"""This module gets distances to the closest POI of each type along the road network"""

from absl import app
from absl import flags

import logging

import sys

sys.path.append("/app")

# import scraping as sc

import pandas as pd


from db_utils import get_engine, get_table_creation_query


import os

from network_utils import get_closest_osmids
from constants import DATA_PATH
import pickle
import osmnx as ox
import networkx as nx


logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s [%(name)s] %(levelname)-8s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


FLAGS = flags.FLAGS

flags.DEFINE_string("searchname", "tamzin", "Name for search", short_name="s")

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


# In[5]:
def main(_):

    engine = get_engine(user, password, host)

    # ## Define search parameters

    searchname = FLAGS.searchname

    # ## Load Data

    with engine.connect() as conn:
        df_closest_pois = pd.read_sql(
            f"""
        SELECT c.eastings,c.northings,c.poi_id,c.address_id,c.poi_category FROM {searchname}.closest_pois c
        INNER JOIN {searchname}.address_ids_to_process a
        ON c.address_id=a.address_id""",
            con=conn,
        )
        df_addressnodes = pd.read_sql(
            f"""
        SELECT n.* FROM {searchname}.address_nodes n
        INNER JOIN {searchname}.address_ids_to_process a
        ON n.address_id=a.address_id""",
            con=conn,
        )
        df_poinodes = pd.read_sql("SELECT * FROM poi_nodes", con=conn)
        df_nodes = pd.read_sql("SELECT * FROM node_coords", con=conn)

    logger.info("Attempting to load network...")
    try:
        with open(
            os.path.join(DATA_PATH, "roadnet_greaterlondon_walking.pkl"), "rb"
        ) as f:
            walknet = pickle.load(f)
            logger.info("Loaded network")
    except EOFError as e:
        logger.warning("unable to load from pickle. Loading from graphml")
        walknet = ox.load_graphml(
            os.path.join(DATA_PATH, "roadnet_greaterlondon_walking.graphml")
        )
        logger.info("saving new network")
        with open(
            os.path.join(DATA_PATH, "roadnet_greaterlondon_walking.pkl"), "wb"
        ) as f:
            walknet = pickle.dump(walknet, f)
        logger.info("saved new network as pickle")

    df_closest_pois = df_closest_pois.join(
        df_addressnodes.set_index("address_id")[["osmid"]].rename(
            columns={"osmid": "osmid_address"}
        ),
        on="address_id",
    )

    df_points = df_closest_pois.join(
        df_poinodes.set_index(["poi_id", "poi_category"])[["osmid"]].rename(
            columns={"osmid": "osmid_poi"}
        ),
        on=["poi_id", "poi_category"],
        how="inner",
    )

    df_points

    if df_points.empty:
        df_points["shortest_path_distance"] = []
    else:
        df_points["shortest_path_distance"] = df_points.apply(
            lambda x: nx.shortest_path_length(
                walknet, x.osmid_address, x.osmid_poi, weight="length"
            ),
            axis=1,
        )

    df_polygons = df_closest_pois[
        ~df_closest_pois.poi_category.isin(df_points.poi_category.unique())
    ]

    coords_nodes = df_nodes[["eastings", "northings"]].values

    closest_osmids_poly = get_closest_osmids(
        df_polygons[["eastings", "northings"]].values,
        coords_nodes,
        df_nodes.osmid.values,
    )

    df_polygons["osmid_poi"] = closest_osmids_poly

    if df_polygons.empty:
        df_polygons["shortest_path_distance"] = []
    else:
        df_polygons["shortest_path_distance"] = df_polygons.apply(
            lambda x: nx.shortest_path_length(
                walknet, x.osmid_address, x.osmid_poi, weight="length"
            ),
            axis=1,
        )

    # ## Combine

    df_comb = df_points.append(df_polygons, ignore_index=True)

    # ## Save

    # cols = {
    #     "poi_id": "INTEGER",
    #     "address_id": "INTEGER",
    #     "poi_category": "VARCHAR(64)",
    #     "distance": "DECIMAL(10,3)",
    # }

    # index_cols = ["poi_id", "address_id", "poi_category"]
    # unique_cols = []

    # create_q = get_table_creation_query(
    #     "poi_distances", cols, searchname, index_cols, unique_cols
    # )

    # with engine.connect() as conn:
    #     conn.execute(create_q)

    with engine.connect() as conn:
        df_comb[["address_id", "poi_category", "shortest_path_distance"]].rename(
            columns={"shortest_path_distance": "distance"}
        ).to_sql(
            "poi_distances",
            schema=searchname,
            index=False,
            con=conn,
            if_exists="append",
        )


if __name__ == "__main__":
    app.run(main)
