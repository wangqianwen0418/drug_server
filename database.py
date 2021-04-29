# %%
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import pandas as pd
import os

from config import SERVER_ROOT
# %%


class Neo4jApp:

    def __init__(self, is_local):
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
        # self.data_path = 'https://drug-gnn-models.s3.us-east-2.amazonaws.com/collaboration_delivery/'
        self.data_path = './collab_delivery/'

        if not is_local:
            host_name = "ec2-3-134-76-210.us-east-2.compute.amazonaws.com"
            password = "i-0ecb29664ffd46993"
        else:
            host_name = "localhost"
            password = 'password'

        scheme = "bolt"
        port = 7687
        user = "neo4j"

        # create driver
        uri = "{scheme}://{host_name}:{port}".format(
            scheme=scheme, host_name=host_name, port=port)
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
                    ' CREATE INDEX IF NOT EXISTS FOR (n: `{}` ) ON (n.id) '.format(node_type))

    def init_database(self):
        self.clean_database()
        self.create_index()
        self.build_attention()
        self.add_prediction()

    def build_attention(self):

        attention_path = os.path.join(self.data_path, 'attention_prune.csv')
        attentions = pd.read_csv(attention_path, dtype={
                                 'x_id': 'string', 'y_id': 'string'})

        with self.driver.session() as session:
            for idx, row in attentions.iterrows():
                if abs(row['layer1_att']) + abs(row['layer2_att']) < 0.1:
                    continue
                query = (
                    'MERGE (node1: `{x_type}` {{ id: "{x_id}" }}) '
                    'ON CREATE SET node1.name = "{x_name}", node1.type="{x_type}" '
                    'MERGE (node2: `{y_type}` {{ id: "{y_id}" }}) '
                    'ON CREATE SET node2.name = "{y_name}", node2.type="{y_type}"  '
                    'MERGE (node1)-[: {relation} {{ relation: "{relation}", layer1_att: {layer1_att}, layer2_att: {layer2_att} }} ]->(node2) '
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
                session.run(query)
                long_query = []

    def build_attention2(self):
        attention_path = os.path.join(self.data_path, 'attention_all.csv')

        with self.driver.session() as session:
            query = (
                'USING PERIODIC COMMIT 5000 '
                'LOAD CSV WITH HEADERS FROM $attention_path AS row '
                'MERGE (node1: Node { id: row.x_id, type: row.x_type }) '
                'ON CREATE SET node1.name = row.x_name '
                'MERGE (node2: Node { id: row.y_id, type: row.y_type }) '
                'ON CREATE SET node2.name = row.y_name '
                'CREATE (node1)-[: Relation { layer1_att: row.layer1_att, layer2_att: row.layer2_att, relation: row.relation } ]->(node2) '
            )

            result = session.run(
                query, attention_path=attention_path)

    def add_prediction(self):
        prediction = pd.read_pickle(os.path.join(
            data_path, 'result.pkl'))['prediction']

        with self.driver.session() as session:
            for disease in prediction["rev_indication"]:
                drugs = prediction["rev_indication"][disease]
                for item in sorted(
                    drugs.items(), key=lambda item: item[1], reverse=True
                )[:20]:
                    [drug_id, score] = item
                    query = (
                        'MATCH (node1: `disease` {{ id: "{x_id}" }}) '
                        'MATCH (node2: `drug` {{ id: "{y_id}" }}) '
                        'CREATE (node1)-[: Prediction {{ score: {score}, relation: rev_indication }} ]->(node2) '
                        'RETURN node1, node2'
                    ).format(
                        x_id=disease,
                        y_id=drug_id,
                        score=score
                    )
                    result = session.run(query)


# %%
if __name__ == "__main__":
    is_local = False

    app = Neo4jApp(is_local=is_local)

    app.init_database()
    app.close()

# %%
