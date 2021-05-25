from scipy.spatial import cKDTree


def get_closest_osmids(coords, coords_nodes, osmids):
    tree = cKDTree(data=coords_nodes)
    _, i = tree.query(coords)
    return osmids[i]