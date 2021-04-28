# %%
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import pandas as pd
import os

from config import SERVER_ROOT
# %%


class Neo4jApp:

    def __init__(self, uri, user, password):
        self.node_types = [
            "anatomy",
            "biological_process",
            "cellular_component",
            "disease",
            "drug",
            "effect/phenotype",
            "exposure",
            "gene/protein",
            "molecular_function",
            "pathway"
        ]
        self.data_path = 'https://drug-gnn-models.s3.us-east-2.amazonaws.com/collaboration_delivery/'

        try:
            self.driver = GraphDatabase.driver(
                uri, auth=(user, password), encrypted=False)
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.driver is not None:
            self.driver.close()

    def clean_database(self):
        with self.driver.session() as session:
            session.run('MATCH (n) DETACH DELETE n')
            print('delete all nodes')

    def create_index(self):

        with self.driver.session() as session:
            for node_type in self.node_types:
                session.run(
                    'CREATE INDEX ON :{node_type}(id)'.format(node_type))

    def init_database(self):
        self.clean_database()
        self.build_attention()
        self.add_prediction()

    def build_attention(self):

        attention_path = os.path.join(self.data_path, 'attention_all.csv')
        attentions = pd.read_csv(attention_path, dtype={
                                 'x_id': 'string', 'y_id': 'string'})
        attentions = attentions[~attentions['relation'].str.contains(
            'contraindication')]
        with self.driver.session() as session:
            for idx, row in attentions.iterrows():
                query = (
                    'MERGE (node1: `{x_type}` {{ id: "{x_id}", type: "{x_type}", name: "{x_name}" }}) '
                    'MERGE (node2: `{y_type}` {{ id: "{y_id}", type: "{y_type}", name: "{y_name}" }}) '
                    'MERGE (node1)-[: {relation} {{ relation: "{relation}", layer1_att: {layer1_att}, layer2_att: {layer2_att} }} ]->(node2) '
                    'RETURN node1, node2'
                ).format(
                    x_type=row['x_type'],
                    y_type=row['y_type'],
                    x_id=row['x_id'],
                    y_id=row['y_id'],
                    x_name=row['x_name'],
                    y_name=row['y_name'],
                    layer1_att=row['layer1_att'],
                    layer2_att=row['layer2_att'],
                    relation=row['relation']
                )

                results = session.run(query)

    def build_attention2(self):
        with self.driver.session() as session:

            for node_type in self.node_types:
                query = (
                    'USING PERIODIC COMMIT 5000 '
                    'LOAD CSV WITH HEADERS FROM $attention_path AS row '
                    'MERGE (node1: Node { id: row.x_id, type:row.x_type, name: row.x_name }) '
                    'MERGE (node2: Node { id: row.x_id, type:row.x_type, name: row.x_name }) '
                    'MERGE (node1)-[: Relation { layer1_att: row.layer1_att, layer2_att: row.layer2_att ]->(node2) '
                )

                result = session.run(
                    query, attention_path=self.attention_path)
                print([record['node'] for record in result])

    def add_prediction(self):
        prediction = pd.read_pickle(os.path.join(
            data_path, 'result.pkl'))['prediction']

        with self.driver.session() as session:
            for disease in predictions["rev_indication"]:
                drugs = prediction["rev_indication"][disease]
                for item in sorted(
                    drugs.items(), key=lambda item: item[1], reverse=True
                )[:20]:
                    [drug_id, score] = item
                    query = (
                        'MATCH (node1: `disease` {{ id: "{x_id}" }}) '
                        'MATCH (node2: `drug` {{ id: "{y_id}" }}) '
                        'CREATE (node1)-[: Prediction {{ score: {score} }} ]->(node2) '
                        'RETURN node1, node2'
                    ).format(
                        x_id=disease,
                        y_id=drug_id,
                        score=score
                    )
                    result = session.run(query)


# %%
if __name__ == "__main__":
    scheme = "bolt"
    is_local = False
    if not is_local:
        host_name = "ec2-3-134-76-210.us-east-2.compute.amazonaws.com"
        password = "i-0ecb29664ffd46993"
    else:
        host_name = "localhost"
        password = 'password'

    port = 7687
    user = "neo4j"

    # create driver
    url = "{scheme}://{host_name}:{port}".format(
        scheme=scheme, host_name=host_name, port=port)
    app = Neo4jApp(url, user, password)

    # result = app.create_friendship('lili', 'aa')
    app.clean_database()
    app.build_attention()
    app.close()

# %%
