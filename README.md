# Cell Knowledge Network Extraction, Translation, and Loading of Ontologies

## Motivation

The Cell Knowledge Network (Cell KN) pilot aims to create a
comprehensive cell phenotype knowledge network that integrates
knowledge about diseases and drugs to facilitate discovery of new
biomarkers and therapeutic targets. To maximize interoperability of
the derived knowledge with knowledge about genes, pathways, diseases,
and drugs from other NLM/NCBI resources, the knowledge will be derived
in the form of semantically-structured assertions of
subject-predicate-object triple statements which are compatible with
semantic web technologies, and storage using graph databases, such as
the [ArangoDB](https://arangodb.com/) database system.

One important semantic web technology, the [Web Ontology Language
(OWL)](https://en.wikipedia.org/wiki/Web_Ontology_Language), provides
a family of knowledge representation languages for authoring
ontologies. [Ontologies](https://en.wikipedia.org/wiki/Ontology_(information_science))
encompasses a representation, formal naming, and definitions of the
categories, properties, and relations between the concepts, data, or
entities that pertain to domains of discourse. The
[BioPortal](https://bioportal.bioontology.org/) and [Open Biological
and Biomedical Ontology Foundry](https://obofoundry.org/) provide
comprehensive repositories of biomedical ontologies, many of which
serve as a significant source of knowledge for the Cell KN.

The NCBI Information Resources Branch has extensive experience with
ArangoDB, including performance comparison testing with Neo4j,
interaction with the ArangoDB developers, and use in production.

## Purpose

The `cell-kn-etl-ontology` repository provides Python modules and a
Java package for parsing and loading ontologies into an ArangoDB
instance.

## Ontologies

All terms from the following ontologies have been selected for loading
into the Cell KN:

- [CL](http://purl.obolibrary.org/obo/cl.owl): Cell Ontology provides
  a structured controlled vocabulary for cell types in animals
- [UBERON](http://purl.obolibrary.org/obo/uberon/uberon-base.owl):
  Uberon multi-species anatomy ontology covers animals and bridges
  multiple species-specific ontologies
- [NCBITaxon](http://purl.obolibrary.org/obo/ncbitaxon/subsets/taxslim.owl):
  An ontology representation of the NCBI organismal taxonomy
- [GO](https://purl.obolibrary.org/obo/go/extensions/go-plus.owl):
  Gene Ontology describes the function of genes and gene products
- [MONDO](http://purl.obolibrary.org/obo/mondo/mondo-simple.owl):
  Mondo Disease Ontology harmonizes multiple disease resources to
  yield a coherent merged ontology
- [PATO](http://purl.obolibrary.org/obo/pato.owl): Phenotype And Trait
  Ontology describes phenotypic qualities (properties, attributes or
  characteristics)
- [MmusDv](http://purl.obolibrary.org/obo/mmusdv.owl): Mouse
  Developmental Stages describes life cycle stages for Mus Musculus
- [HsapDv](http://purl.obolibrary.org/obo/hsapdv.owl): Human
  Developmental Stages describes life cycle stages for Humans
- [SO](http://purl.obolibrary.org/obo/so.owl) Sequence Ontology
  provides terms and relationships used to describe the features and
  attributes of biological sequences.
 
Note that some terms from the following ontologies will also be loaded
if the term is contained in one of the selected ontologies:

- [CHEBI](http://purl.obolibrary.org/obo/chebi.owl): A structured
  classification of molecular entities of biological interest focusing
  on 'small' chemical compounds
- [PR](http://purl.obolibrary.org/obo/pr.owl): An ontological
  representation of protein-related entities

Finally, identifiers from [CHEMBL](), a manually curated database of
bioactive molecules with drug-like properties, will also appear.

## Dependencies

### Java

Java SE 21 and Maven 3 or compatible are required to generate the
Javadocs, test, and package. Do each of these as follows:
```
$ mvn javadoc:javadoc
$ mvn test
$ mvn clean package -DskipTests
```

### Python

Python 3.12 and Poetry are required to generate the Sphinx
documentation, test, and run. Install the dependencies as follows:
```
$ python3.12 -m venv .poetry
$ source .poetry/bin/activate
$ python -m pip install -r .poetry.txt
$ deactivate
$ python3.12 -m venv .venv
$ source .venv/bin/activate
$ .poetry/bin/poetry install
```
Generate the Sphinx documentation as follows:
```
$ cd docs/python
$ make clean html
```
Test as follows:
```
$ cd src/test/python
$ python -m unittest OntologyParserLoaderTestCase.py
```

### Data

The Python and Java classes require the ontology files to reside in
`data/obo`. Create and populate this directory as follows:
```
$ mkdir data/obo
$ cd src/main/python
$ python OntologyParserLoader.py --update
```

### Docker

Install [Docker Desktop](https://docs.docker.com/desktop/).

### ArangoDB

An ArangoDB docker image can be downloaded and a container started as
follows:
```
$ cd src/main/shell
$ export ARANGO_DB_HOME="<some-path>/arangodb"
$ export ARANGO_DB_PASSWORD="<some-password>"
$ ./start-arangodb.sh
```

## Usage

Run the Java ontology triple loader as follows:
```
$ export ARANGO_DB_HOST=127.0.0.1
$ export ARANGO_DB_PORT=8529
$ export ARANGO_DB_HOME="<some-path>/arangodb"
$ export ARANGO_DB_PASSWORD="<some-password>"
$ java -cp target/cell-kn-etl-ontologies-1.0.jar gov.nih.nlm.OntologyGraphBuilder
```

Run the Python ontology parser and loader (now deprecated) as follows:
```
$ export ARANGO_DB_PASSWORD="<some-password>"
$ cd src/main/python
$ python OntologyParserLoader.py --full
```
