#!/usr/bin/env python
# coding: utf-8

# ## Get Closest POIs


# In[2]:


import logging

import sys

sys.path.append("/app")

# import scraping as sc

import pandas as pd

from jinja2 import Template

from db_utils import get_engine, get_table_creation_query

from gis_utils import get_gdf_coords, get_closest_idxs, get_closest_pois_slow

from shapely.ops import nearest_points


import dataloader as loader


import numpy as np
import os

import geopandas as gpd


# In[3]:


import logging
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

# In[5]:


user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")


def closest_loc_point_polygon(point, polygon):
    return nearest_points(polygon, point)[0]


# In[6]:


def main(_):
    engine = get_engine(user, password, host)

    # ## Define search parameters

    # In[7]:

    searchname = FLAGS.searchname

    # ## Load data

    # In[8]:

    q_load = f"""SELECT g.* FROM 
    {searchname}.geocoded_addresses g
    INNER JOIN
    {searchname}.address_ids_to_process a
    ON g.address_id=a.address_id"""

    # In[9]:

    with engine.connect() as conn:
        df = pd.read_sql(q_load, con=conn)

    # In[10]:

    logger.info(f"Loaded {len(df)} addresses")

    # ## Load data

    # ### POIs

    # In[11]:

    pois = loader.load_poi_gdfs(user=user, password=password, host=host)

    # ### Addresses

    # In[12]:

    q_addresses = f"""SELECT c.* FROM 
    {searchname}.bng_coords c
    INNER JOIN
    {searchname}.address_ids_to_process a
    ON c.address_id=a.address_id"""

    # In[13]:

    df_addresses = loader.load_sql(q_addresses, user=user, password=password, host=host)

    df_addresses = loader.load_sql(
        f"SELECT * FROM {searchname}.bng_coords",
        user=user,
        password=password,
        host=host,
    )
    # ## Get coordinates of POIs

    # In[14]:

    is_point = {
        category: np.all(pois[category].geometry.geom_type == "Point")
        for category in pois
    }
    point_cats = [i for i in is_point if is_point[i]]
    logger.info("Calculating for point pois:" + ",".join(point_cats))

    # In[15]:

    coords = {
        category: get_gdf_coords(pois[category])
        for category in pois
        if is_point[category]
    }

    # ## Get closest POIs for point-based POIs

    # In[16]:

    closest_idxs = {
        category: get_closest_idxs(
            df_addresses[["eastings", "northings"]].values, coords[category]
        )
        for category in coords
    }

    # In[17]:

    closest_ids = {
        category: pois[category].id.values[closest_idxs[category]]
        for category in closest_idxs
    }

    poly_cats = [i for i in is_point if not is_point[i]]
    logger.info("Calculating for polygon pois:" + ",".join(poly_cats))
    # ## Get closest POIs for polygon-based POIs

    # In[18]:

    # In[19]:

    polygons = {
        category: pois[category].set_index("id").geometry
        for category in pois
        if not is_point[category]
    }

    # In[20]:

    # In[21]:

    gdf_addresses = gpd.GeoDataFrame(
        df_addresses,
        geometry=gpd.points_from_xy(df_addresses.eastings, df_addresses.northings),
    )

    # test_point = gdf_addresses.geometry[1]test_polygon = polygons["park"][polygons["park"].area > 1000_000].iloc[0]p1, p2 = nearest_points(test_polygon, test_point)test_gser = gpd.GeoSeries([test_point, test_polygon])results_gser = gpd.GeoSeries([p1])import matplotlib.pyplot as pltfig, ax = plt.subplots(figsize=(16, 9))
    # test_gser.plot(ax=ax)
    # results_gser.plot(ax=ax, color="red")gdf_addressespois["park"].plot()polygons
    # In[22]:

    closest_ids_poly = {
        category: get_closest_pois_slow(
            polygons[category].reset_index(), gdf_addresses
        )[0]
        for category in polygons
    }

    # In[23]:

    closest_points_poly = {
        category: gpd.GeoSeries(
            [
                closest_loc_point_polygon(point, poly)
                for point, poly in zip(
                    gdf_addresses.geometry,
                    polygons[category].loc[closest_ids_poly[category]].values,
                )
            ]
        )
        for category in polygons
    }

    # ## Create Outputs

    # In[24]:

    output_dfs_points = {
        category: pd.DataFrame(
            {
                "address_id": df_addresses.address_id.values,
                "poi_id": closest_ids[category],
                "eastings": None,
                "northings": None,
                "poi_category": category,
            }
        )
        for category in closest_ids
    }

    # In[25]:

    output_dfs_polys = {
        category: pd.DataFrame(
            {
                "address_id": df_addresses.address_id.values,
                "poi_id": closest_ids_poly[category],
                "eastings": closest_points_poly[category].x.values,
                "northings": closest_points_poly[category].y.values,
                "poi_category": category,
            }
        )
        for category in closest_ids_poly
    }

    # ## Create table

    # In[26]:

    cols = {
        "eastings": "DECIMAL(14,6)",
        "northings": "DECIMAL(14,6)",
        "poi_id": "INTEGER",
        "address_id": "INTEGER",
        "poi_category": "VARCHAR(64)",
    }

    index_cols = ["poi_id", "address_id", "poi_category"]
    unique_cols = []

    # In[27]:

    create_q = get_table_creation_query(
        "closest_pois", cols, searchname, index_cols, unique_cols
    )

    # In[28]:

    q_unique = f"""CREATE UNIQUE INDEX IF NOT EXISTS address_poi_category_closest_pois_categoryx
            ON {searchname}.closest_pois (poi_category,address_id);
            ALTER TABLE {searchname}.closest_pois DROP CONSTRAINT IF EXISTS unique_address_poi_category_closest_pois;

            ALTER TABLE {searchname}.closest_pois 
            ADD CONSTRAINT unique_address_poi_category_closest_pois
            UNIQUE USING INDEX address_poi_category_closest_pois_categoryx;"""

    # In[29]:

    with engine.connect() as conn:
        conn.execute(create_q)
        conn.execute(q_unique)

    with engine.connect() as conn:
        conn.execute(f"DROP TABLE {searchname}.closest_pois")
    # ## Create Outputs

    # In[30]:

    output = pd.concat([output_dfs_points[i] for i in output_dfs_points])
    output = output.append(
        pd.concat([output_dfs_polys[i] for i in output_dfs_polys]), ignore_index=True
    )

    # In[31]:

    with engine.connect() as conn:
        output.to_sql(
            "closest_pois", schema=searchname, index=False, if_exists="append", con=conn
        )


if __name__ == "__main__":
    app.run(main)
