from scipy.spatial import cKDTree
import logging

logger = logging.getLogger(__name__)


def get_closest_osmids(coords, coords_nodes, osmids):
    tree = cKDTree(data=coords_nodes)
    _, i = tree.query(coords)
    return osmids[i]


def attempt_load_roadnet():
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
    return walknet