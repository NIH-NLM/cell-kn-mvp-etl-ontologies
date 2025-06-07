import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest

from arango import ArangoClient

sys.path.insert(0, str(Path("../../main/python").resolve()))

import OntologyParserLoader as opl

SH_DIR = Path(__file__).parents[2] / "main" / "shell"
ARANGODB_DIR = Path(__file__).parent / "arangodb"
ARANGO_URL = "http://localhost:8529"
ARANGO_CLIENT = ArangoClient(hosts=ARANGO_URL)
ARANGO_ROOT_PASSWORD = os.environ["ARANGO_DB_PASSWORD"]


class OntologyParserLoaderTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        # Stop any ArangoDB instance
        subprocess.run(["./stop-arangodb.sh"], cwd=SH_DIR)

        # Start an ArangoDB instance using the test data directory
        os.environ["ARANGO_DB_HOME"] = str(ARANGODB_DIR)
        subprocess.run(["./start-arangodb.sh"], cwd=SH_DIR)

    @unittest.skip("TODO")
    def test_update_ontologies(self):
        pass

    @unittest.skip("TODO")
    def test_parse_obo(self):
        pass

    @unittest.skip("TODO")
    def test_parse_term(self):
        pass

    # Not needed since not used in parsing ontology or loading ArangoDB
    # def test_count_triple_types(self):
    #     pass

    @unittest.skip("TODO")
    def test_collect_fnode_triples(self):
        pass

    @unittest.skip("TODO")
    def test_collect_bnode_triple_sets(self):
        pass

    @unittest.skip("TODO")
    def test_create_bnode_triples_from_bnode_triple_sets(self):
        pass

    @unittest.skip("TODO")
    def test_create_bnode_triples_from_bnode_triple_set(self):
        pass

    @unittest.skip("TODO")
    def test_get_fnode(self):
        pass

    @unittest.skip("TODO")
    def test_load_triples_into_adb_graph(self):
        pass

    @unittest.skip("TODO")
    def test_create_or_get_vertices_from_triple(self):
        pass

    @unittest.skip("TODO")
    def test_create_or_get_vertex(self):
        pass

    @unittest.skip("TODO")
    def create_or_get_edge_from_triple(self):
        pass

    @unittest.skip("TODO")
    def create_or_get_edge(self):
        pass

    @unittest.skip("TODO")
    def update_vertex_from_triple(self):
        pass

    def test_main(self):
        """Compare actual and expected macrophage vertex and edges,
        obtaining expected values by manual inspection of the
        macrophage OWL file.
        """
        # Parse macrophage OWL file and load the result into ArangoDB
        opl.main(parameters=["--test"])

        # Connect to ArangoDB
        db = ARANGO_CLIENT.db("Cell-KN-v1.5", username="root", password=ARANGO_ROOT_PASSWORD)
        graph = db.graph("CL-Test")

        # Get the actual macrophage vertex
        vertex_collection = graph.vertex_collection("CL")
        key = "0000235"
        self.assertTrue(vertex_collection.has(key))
        a_vertex = vertex_collection.get(key)

        # Define the expected macrophage vertex
        e_vertex = {
            "_key": "0000235",
            "_id": "CL/0000235",
            "_rev": "_jPkjjC6---",
            "term": "CL_0000235",
            "hasExactSynonym": "histiocyte",
            "hasDbXref": [
                "MESH:D008264",
                "ZFA:0009141",
                "FMA:63261",
                "BTO:0000801",
                "CALOHA:TS-0587",
                "FMA:83585",
                "PMID:16213494",
                "GOC:tfm",
                "GO_REF:0000031",
                "PMID:1919437",
                "GOC:add",
            ],
            "comment": "Morphology: Diameter 30_M-80 _M, abundant cytoplasm, low N/C ratio, eccentric nucleus. Irregular shape with pseudopods, highly adhesive. Contain vacuoles and phagosomes, may contain azurophilic granules; markers: Mouse & Human: CD68, in most cases CD11b. Mouse: in most cases F4/80+; role or process: immune, antigen presentation, & tissue remodelling; lineage: hematopoietic, myeloid.",
            "label": "macrophage",
            "id": "CL:0000235",
            "definition": [
                "A mononuclear phagocyte present in variety of tissues, typically differentiated from monocytes, capable of phagocytosing a variety of extracellular particulate material, including immune complexes, microorganisms, and dead cells."
            ],
        }

        # Assert equal vertex keys
        self.assertTrue(sorted(a_vertex.keys()) == sorted(e_vertex.keys()))

        # Assert equal vertex values, ignoring the revision
        for e_key, e_value in e_vertex.items():
            if e_key == "_rev":
                continue
            a_value = a_vertex[e_key]
            if type(e_value) == list:
                self.assertTrue(type(a_value) == list)
                self.assertTrue(sorted(a_value) == sorted(e_value))
            else:
                self.assertTrue(a_value == e_value)

        # Get macrophage edges to CL terms, then assert equal labels
        edge_collection = graph.edge_collection("CL-CL")
        keys = ["0000235-0000113", "0000235-0000145", "0000235-0000766"]
        for key in keys:
            self.assertTrue(edge_collection.has(key))
            edge = edge_collection.get(key)
            self.assertTrue(edge["label"] == "subClassOf")
        key = "0000235-0000576"
        self.assertTrue(edge_collection.has(key))
        edge = edge_collection.get(key)
        self.assertTrue(edge["label"] == "develops from")

        # Get macrophage edges to GO terms, then assert equal labels
        edge_collection = graph.edge_collection("CL-GO")
        key = "0000235-0031268"
        self.assertTrue(edge_collection.has(key))
        edge = edge_collection.get(key)
        self.assertTrue(edge["label"] == "capable of")

        # Get macrophage edges to NCBITaxon terms, then assert equal labels
        edge_collection = graph.edge_collection("CL-NCBITaxon")
        key = "0000235-9606"
        self.assertTrue(edge_collection.has(key))
        edge = edge_collection.get(key)
        self.assertTrue(edge["label"] == "present in taxon")

    @classmethod
    def tearDownClass(cls):

        # Stop the ArangoDB instance using the test data directory
        subprocess.run(["./stop-arangodb.sh"], cwd=SH_DIR)

        # Remove ArangoDB test data directory
        shutil.rmtree(ARANGODB_DIR)
