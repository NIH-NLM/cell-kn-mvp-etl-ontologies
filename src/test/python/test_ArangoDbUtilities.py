import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

from arango import ArangoClient

sys.path.insert(0, str(Path("../../main/python").resolve()))
print(sys.path)

import ArangoDbUtilities as adb


class TestArangoDB(unittest.TestCase):

    def setUp(self):

        # Stop any ArangoDB instance
        self.sh_dir = Path(__file__).parents[2] / "main" / "shell"
        subprocess.run(["./stop-arangodb.sh"], cwd=self.sh_dir)

        # Start an ArangoDB instance using the test data directory
        self.arangodb_dir = Path(__file__).parent / "arangodb"
        os.environ["ARANGO_DB_HOME"] = str(self.arangodb_dir)
        subprocess.run(["./start-arangodb.sh"], cwd=self.sh_dir)

        # Connect to ArangoDB
        self.arango_url = "http://localhost:8529"
        self.arango_client = ArangoClient(hosts=self.arango_url)
        self.arango_root_password = os.environ["ARANGO_DB_PASSWORD"]
        self.sys_db = self.arango_client.db(
            "_system", username="root", password=self.arango_root_password
        )

        # Define common names
        self.database_name = "database"
        self.graph_name = "graph"
        self.from_vertex_name = "from_vertex"
        self.to_vertex_name = "to_vertex"

    def test_create_or_get_database(self):

        self.assertFalse(self.sys_db.has_database(self.database_name))

        adb.create_or_get_database(self.database_name)
        self.assertTrue(self.sys_db.has_database(self.database_name))

    def test_delete_database(self):

        adb.create_or_get_database(self.database_name)
        self.assertTrue(self.sys_db.has_database(self.database_name))

        adb.delete_database(self.database_name)
        self.assertFalse(self.sys_db.has_database(self.database_name))

    def test_create_or_get_graph(self):

        db = adb.create_or_get_database(self.database_name)
        self.assertFalse(db.has_graph(self.graph_name))

        adb.create_or_get_graph(db, self.graph_name)
        self.assertTrue(db.has_graph(self.graph_name))

    def test_delete_graph(self):

        db = adb.create_or_get_database(self.database_name)
        adb.create_or_get_graph(db, self.graph_name)
        self.assertTrue(db.has_graph(self.graph_name))

        adb.delete_graph(db, self.graph_name)
        self.assertFalse(db.has_graph(self.graph_name))

    def test_create_or_get_vertex_collection(self):

        db = adb.create_or_get_database(self.database_name)
        graph = adb.create_or_get_graph(db, self.graph_name)
        self.assertFalse(graph.has_vertex_collection(self.from_vertex_name))

        adb.create_or_get_vertex_collection(graph, self.from_vertex_name)
        self.assertTrue(graph.has_vertex_collection(self.from_vertex_name))

    def test_delete_vertex_collection(self):

        db = adb.create_or_get_database(self.database_name)
        graph = adb.create_or_get_graph(db, self.graph_name)
        adb.create_or_get_vertex_collection(graph, self.from_vertex_name)
        self.assertTrue(graph.has_vertex_collection(self.from_vertex_name))

        adb.delete_vertex_collection(graph, self.from_vertex_name)
        self.assertFalse(graph.has_vertex_collection(self.from_vertex_name))

    def test_create_or_get_edge_collection(self):

        db = adb.create_or_get_database(self.database_name)
        graph = adb.create_or_get_graph(db, self.graph_name)
        adb.create_or_get_vertex_collection(graph, self.from_vertex_name)
        adb.create_or_get_vertex_collection(graph, self.to_vertex_name)
        collection_name = f"{self.from_vertex_name}-{self.to_vertex_name}"
        self.assertFalse(graph.has_edge_definition(collection_name))

        adb.create_or_get_edge_collection(
            graph, self.from_vertex_name, self.to_vertex_name
        )
        self.assertTrue(graph.has_edge_definition(collection_name))

    def test_delete_edge_collection(self):

        db = adb.create_or_get_database(self.database_name)
        graph = adb.create_or_get_graph(db, self.graph_name)
        adb.create_or_get_vertex_collection(graph, self.from_vertex_name)
        adb.create_or_get_vertex_collection(graph, self.to_vertex_name)
        edge_name = f"{self.from_vertex_name}-{self.to_vertex_name}"
        adb.create_or_get_edge_collection(
            graph, self.from_vertex_name, self.to_vertex_name
        )
        self.assertTrue(graph.has_edge_definition(edge_name))

        adb.delete_edge_collection(graph, edge_name)
        self.assertFalse(graph.has_edge_definition(edge_name))

    def tearDown(self):

        # Stop the ArangoDB instance using the test data directory
        subprocess.run(["./stop-arangodb.sh"], cwd=self.sh_dir)

        # Remove ArangoDB test data directory
        shutil.rmtree(self.arangodb_dir)
