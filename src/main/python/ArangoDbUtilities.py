import json
import os
from pprint import pprint
import random

from arango import ArangoClient

ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
ARANGO_ROOT_PASSWORD = os.getenv("ARANGO_DB_PASSWORD", "")
SYS_DB = ARANGO_CLIENT.db("_system", username="root", password=ARANGO_ROOT_PASSWORD)

DATA_DIR = "../data"

NSFOREST_DIR = f"{DATA_DIR}/nsforest-2024-06-27"


def create_or_get_database(database_name):
    """Create or get an ArangoDB database.

    Parameters
    ----------
    database_name : str
        Name of the database to create or get

    Returns
    -------
    db : arango.database.StandardDatabase
        Database
    """
    # Create database, if needed
    if not SYS_DB.has_database(database_name):
        print(f"Creating ArangoDB database: {database_name}")
        SYS_DB.create_database(database_name)

    # Connect to database
    print(f"Getting ArangoDB database: {database_name}")
    db = ARANGO_CLIENT.db(database_name, username="root", password=ARANGO_ROOT_PASSWORD)

    return db


def delete_database(database_name):
    """Delete an ArangoDB database.

    Parameters
    ----------
    database_name : str
        Name of the database to delete

    Returns
    -------
    None
    """
    # Delete database, if needed
    if SYS_DB.has_database(database_name):
        print(f"Deleting ArangoDB database: {database_name}")
        SYS_DB.delete_database(database_name)


def create_or_get_graph(db, graph_name):
    """Create or get an ArangoDB database graph.

    Parameters
    ----------
    db : arango.database.StandardDatabase
        Database
    graph_name : str
        Name of the graph to create or get

    Returns
    -------
    graph : arango.graph.Graph
        Database graph
    """
    # Create, or get the graph
    if not db.has_graph(graph_name):
        print(f"Creating database graph: {graph_name}")
        graph = db.create_graph(graph_name)
    else:
        print(f"Getting database graph: {graph_name}")
        graph = db.graph(graph_name)

    return graph


def delete_graph(db, graph_name):
    """Delete an ArangoDB database graph.

    Parameters
    ----------
    db : arango.database.StandardDatabase
        Database
    graph_name : str
        Name of the graph to delete

    Returns
    -------
    None
    """
    # Delete the graph
    if db.has_graph(graph_name):
        print(f"Deleting database graph: {graph_name}")
        db.delete_graph(graph_name)


def create_or_get_vertex_collection(graph, vertex_name):
    """Create, or get an ArangoDB database graph vertex collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    vertex_name : str
        Name of the vertex collection to create or get

    Returns
    -------
    collection : arango.collection.VertexCollection
        Graph vertex collection
    """
    # Create, or get the vertex collection
    if not graph.has_vertex_collection(vertex_name):
        print(f"Creating graph vertex collection: {vertex_name}")
        collection = graph.create_vertex_collection(vertex_name)
    else:
        print(f"Getting graph vertex collection: {vertex_name}")
        collection = graph.vertex_collection(vertex_name)

    return collection


def delete_vertex_collection(graph, vertex_name):
    """Delete an ArangoDB database graph vertex collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    vertex_name : str
        Name of the vertex collection to delete

    Returns
    -------
    None
    """
    # Delete the vertex collection
    if graph.has_vertex_collection(vertex_name):
        print(f"Deleting graph vertex collection: {vertex_name}")
        graph.delete_vertex_collection(vertex_name)


def create_or_get_edge_collection(graph, from_vertex_name, to_vertex_name):
    """Create, or get an ArangoDB database edge collection from and
    to the specified vertices.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    from_vertex : str
        Name of the vertex collection from which the edge originates
    to_vertex : str
        Name of the vertex collection to which the edge terminates

    Returns
    -------
    collection : arango.collection.EdgeCollection
        Graph edge collection
    collection_name : str
        Name of the edge collection
    """
    # Create, or get the edge collection
    collection_name = f"{from_vertex_name}-{to_vertex_name}"
    if not graph.has_edge_definition(collection_name):
        print(f"Creating edge definition: {collection_name}")
        collection = graph.create_edge_definition(
            edge_collection=collection_name,
            from_vertex_collections=[f"{from_vertex_name}"],
            to_vertex_collections=[f"{to_vertex_name}"],
        )
    else:
        print(f"Getting edge collection: {collection_name}")
        collection = graph.edge_collection(collection_name)

    return collection, collection_name


def delete_edge_collection(graph, edge_name):
    """Delete an ArangoDB database graph edge definition and collection.

    Parameters
    ----------
    graph : arango.graph.Graph
        Graph
    edge_name : str
        Name of the edge definition and collection to delete

    Returns
    -------
    None
    """
    # Delete the collection
    if graph.has_edge_definition(edge_name):
        print(f"Deleting graph edge definition and collection: {edge_name}")
        graph.delete_edge_definition(edge_name)


def create_analyzers(database_name):
    """Create n-gram and text analyzers in the named database.

    Parameters
    ----------
    database_name : str
        Name of the database in which to create the analyzers

    Returns
    -------
    None
    """
    db = create_or_get_database(database_name)
    db.create_analyzer(
        name=f"n-gram",
        analyzer_type="ngram",
        properties={
            "min": 3,
            "max": 4,
            "preserveOriginal": True,
            "streamType": "utf8",
            "startMarker": "",
            "endMarker": "",
        },
        features=["frequency", "position", "norm"],
    )
    db.create_analyzer(
        name=f"text_en_no_stem",
        analyzer_type="text",
        properties={
            "locale": "en",
            "case": "lower",
            "accent": False,
            "stemming": False,
            "edgeNgram": {
                "min": 3,
                "max": 12,
                "preserveOriginal": True,
            },
        },
        features=["frequency", "position", "norm"],
    )


def delete_analyzers(database_name):
    """Delete n-gram and text analyzers in the named database.

    Parameters
    ----------
    database_name : str
        Name of the database in which to delete the analyzer

    Returns
    -------
    None
    """
    db = create_or_get_database(database_name)
    db.delete_analyzer(f"{database_name}::n-gram", ignore_missing=True)
    db.delete_analyzer(f"{database_name}::text_en_no_stem", ignore_missing=True)


def create_view(
    database_name,
    collection_maps_name="../../../data/cell-kn-mvp-collection-maps-2025-04-23.json",
):

    # Populate the view properties from the collections map
    with open(collection_maps_name, "r") as fp:
        collection_maps = json.load(fp)
    properties = {
        "writebufferSizeMax": 33554432,
        "id": "74447522",
        "storedValues": [],
        "consolidationPolicy": {
            "type": "tier",
            "segmentsBytesFloor": 2097152,
            "segmentsBytesMax": 5368709120,
            "segmentsMax": 10,
            "segmentsMin": 1,
            "minScore": 0,
        },
        "writebufferActive": 0,
        "links": {},
        "commitIntervalMsec": 1000,
        "consolidationIntervalMsec": 1000,
        "globallyUniqueId": "h1D09A664A5DB/74447522",
        "cleanupIntervalStep": 2,
        "primarySort": [],
        "primarySortCompression": "lz4",
        "writebufferIdle": 64,
    }
    for collection_map in collection_maps:
        vertex_name = collection_map[0]
        vertex_labels = collection_map[1]["individual_labels"]
        properties["links"][vertex_name] = {}
        properties["links"][vertex_name]["analyzers"] = ["identity"]
        properties["links"][vertex_name]["fields"] = {}
        for vertex_label in vertex_labels:
            properties["links"][vertex_name]["fields"][vertex_label] = {
                "analyzers": ["text_en", "text_en_no_stem", "n-gram"]
            }
        properties["links"][vertex_name]["includeAllFields"] = False
        properties["links"][vertex_name]["storeValues"] = "none"
        properties["links"][vertex_name]["trackListPositions"] = False

    db = create_or_get_database(database_name)
    if database_name == "Cell-KN-Phenotypes":
        keys = list(properties["links"].keys())
        for key in keys:
            if key not in [
                "CHEMBL",
                "CL",
                "CS",
                "CSD",
                "GS",
                "MONDO",
                "NCBITaxon",
                "PUB",
                "UBERON",
            ]:
                del properties["links"][key]

    db.create_view(
        name="indexed",
        view_type="arangosearch",
        properties=properties,
    )


def delete_view(database_name):
    db = create_or_get_database(database_name)
    db.delete_view("indexed")


def print_vertex_examples(database_name, graph_name):
    """Delete the bigram analyzer in the named database.

    Parameters
    ----------
    database_name : str
        Name of the database in which to delete the analyzer
    graph_name : str
        Name of the graph to create or get

    Returns
    -------
    None
    """
    # Get the database and graph
    db = create_or_get_database(database_name)
    graph = create_or_get_graph(db, graph_name)

    # Collect relevant vertex names
    vertex_names = []
    for collection in db.collections():
        if collection["type"] != "document" or collection["name"][0] == "_":
            continue
        vertex_names.append(collection["name"])

    # Select one vertex randomly
    random.seed(a=0, version=2)
    for vertex_name in sorted(vertex_names):
        vertex_collection = create_or_get_vertex_collection(graph, vertex_name)
        vertex_keys = list(vertex_collection.keys())
        vertex_key = vertex_keys[random.randint(0, vertex_collection.count() - 1)]
        vertex = vertex_collection.get(vertex_key)
        print()
        print(vertex_name)
        print()
        pprint(vertex)
