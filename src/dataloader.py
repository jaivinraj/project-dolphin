from db_utils import get_engine
import pandas as pd
import geopandas as gpd
import os
from constants import DATA_PATH, POI_CATEGORIES


def load_sql(query, **kwargs):
    engine = get_engine(**kwargs)
    with engine.connect() as conn:
        df = pd.read_sql(query, con=conn)
    return df


def load_poi_geo(cat="parks", **kwargs):
    gdf = gpd.read_file(os.path.join(DATA_PATH, "pois", f"{cat}.shp"))
    df = load_sql(f"SELECT * FROM pois.{cat}", **kwargs)
    return gdf.join(df.set_index("id"), on="id")


def load_poi_gdfs(**kwargs):
    dfs = {}
    for category in POI_CATEGORIES:
        if category == "tubes":
            gdf_tube = load_poi_geo("stations", **kwargs)
            gdf_tube = gdf_tube[
                gdf_tube["network"].str.contains("London Underground").fillna(False)
            ]
            dfs[category] = gdf_tube
        else:
            dfs[category] = load_poi_geo(category, **kwargs)
    return dfs