from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import logging
from tqdm import tqdm
import pandas as pd

logger = logging.getLogger(__name__)


def query_address(address, rowid=0, max_tries=5):
    """Attempt to geocode a single address

    Parameters
    ----------
    address : str
        address of
    rowid : int, optional
        row id (used for logging), by default 0
    max_tries : int, optional
        Maximum number of times to retry. May be necessary if there is a
        disconnection, by default 5

    Returns
    -------
    dict
        with values for id, original address, new address, latitude and
        longitude
    """
    logger.debug(f"Querying for rowid {rowid}")
    geolocator = Nominatim(user_agent=f"househunting-application-{rowid}")
    retry = True
    failed = False
    n_tries = 0
    while retry:
        logger.debug("Querying address...")
        try:
            location = geolocator.geocode(address)
            retry = False
        except Exception as e:
            n_tries += 1
            if n_tries >= max_tries:
                retry = False
                logger.debug(f"Unable to make call for rowid {rowid}. Continuing")
            else:
                logger.debug(f"Failed on try {n_tries}. Trying again")
                location = None
    logger.debug("Querying complete")
    if location is None or failed:
        logger.debug(f"No address found for rowid {rowid}")
        return {
            "id": rowid,
            "search_address": address,
            "address": None,
            "latitude": None,
            "longitude": None,
        }
    return {
        "id": rowid,
        "search_address": address,
        "address": location.address,
        "latitude": location.latitude,
        "longitude": location.longitude,
    }


def query_addresses(df, address_col="address"):
    """Query addresses for a dataframe

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing a column for address
    address_col : str, optional
        Name of column containing address, by default "address"

    Returns
    -------
    [type]
        [description]
    """
    n_target = len(df)
    query_results = []
    logger.info("Querying geolocations...")
    for i, row in tqdm(df.iterrows(), total=len(df)):
        address_quer = (
            row[address_col] if "UK" in row[address_col] else row[address_col] + ", UK"
        )
        query_results += [query_address(address_quer, row["id"], max_tries=5)]
    logger.info("Compiling results...")
    df_results = pd.DataFrame(query_results)
    df_results = df_results[~df_results["latitude"].isna()]
    n_fetched = len(df_results)
    logger.info(
        f"{n_fetched} of {n_target} successfully fetched ({100*n_fetched/n_target:.2f}%)"
    )
    return df_results