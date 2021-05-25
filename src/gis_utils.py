from tqdm import tqdm
import logging
import geopandas as gpd
from scipy.spatial.distance import cdist
from scipy.spatial import cKDTree
import pandas as pd
import numpy as np

# logging config
logger = logging.getLogger(__name__)


def convert_bng(df):
    logger.info("Converting points to GeoDataFrame...")
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude"], df["latitude"], crs="EPSG:4326"),
    )
    logger.info("Converting crs...")
    gdf_bng = gdf.to_crs("EPSG:27700")
    logger.info("Getting bng coordinates")
    gdf_bng["eastings"] = gdf_bng["geometry"].x
    gdf_bng["northings"] = gdf_bng["geometry"].y
    return gdf_bng


def get_gdf_coords(gdf):
    """Get numpy array of coordinates from a GeoDataFrame

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        With point geometries

    Returns
    -------
    np.array
        Array of coordinates
    """
    return np.array([[p.geometry.x, p.geometry.y] for p in gdf.itertuples()])


def get_closest_idxs(coords, coords_ref):
    """Get index of closest coordinates (using cKDTree)

    Parameters
    ----------
    coords : np.array
        Coordinates for which we seek the closest reference
    coords_ref : np.array
        Reference coordinates

    Returns
    -------
    np.array
        Indices of closest rows in reference
    """
    tree = cKDTree(data=coords_ref)
    _, i = tree.query(coords)
    return i


def get_distance_geoser_point(gdf, point, metric="euclidean"):
    points = [(i.geometry.x, i.geometry.y) for i in gdf.itertuples()]
    return pd.Series(
        cdist(points, [(point.x, point.y)], metric=metric).T[0], index=gdf.index
    )


def get_closest_poi_slow(gdf_pois, point, metric="euclidean"):
    if metric == "euclidean":
        dists = gdf_pois.set_index("id").distance(point)
    else:
        dists = get_distance_geoser_point(gdf_pois, point, metric=metric)
    minid = dists.idxmin()
    mindist = dists.loc[minid]
    return minid, mindist


def get_closest_pois_slow(gdf_pois, gdf, metric="euclidean"):
    distances = []
    minids = []
    for i, row in tqdm(gdf.iterrows(), total=len(gdf)):
        minid, mindist = get_closest_poi_slow(gdf_pois, row["geometry"], metric=metric)
        minids += [minid]
        distances += [mindist]
    return minids, distances