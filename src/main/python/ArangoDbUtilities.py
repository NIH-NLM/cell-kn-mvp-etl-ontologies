import ast
from glob import glob
import os
from traceback import print_exc

from arango import ArangoClient
import pandas as pd

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
