# %%
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import pandas as pd
import os

from config import SERVER_ROOT
# %%


class Neo4jApp:

    def __init__(self, server, password='reader_password', user='reader'):
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
        self.batch_size = 10000
        # self.data_path = 'https://drug-gnn-models.s3.us-east-2.amazonaws.com/collaboration_delivery/'
        self.data_path = './collab_delivery/'
        self.database = 'neo4j'

        scheme = "bolt"
        port = 7687

        if server == 'local':
            host_name = "localhost"
            password = 'password'
            user = 'neo4j'

        elif server == 'community':
            # community version, password is instance id
            host_name = "ec2-3-134-76-210.us-east-2.compute.amazonaws.com"

        else:
            # enterprise version, password is instance id
            host_name = 'ec2-18-219-188-110.us-east-2.compute.amazonaws.com'

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
        with self.driver.session(database=self.database) as session:
            session.run('MATCH (n) DETACH DELETE n')
            print('delete all nodes')

    def create_index(self):

        with self.driver.session(database=self.database) as session:
            for node_type in self.node_types:
                session.run(
                    ' CREATE INDEX IF NOT EXISTS FOR (n: `{}` ) ON (n.id) '.format(node_type))

    def init_database(self):
        self.clean_database()
        self.create_index()
        print('build attention graph...')
        self.build_attention()
        print('build prediction graph...')
        self.add_prediction()
        print('database initialization finished')

    def build_attention(self):

        attention_path = os.path.join(self.data_path, 'attention_prune.csv')
        attentions = pd.read_csv(attention_path, dtype={
            'x_id': 'string', 'y_id': 'string'})

        lines = []
        x_type = ''
        y_type = ''
        relation = ''

        def get_query(x_type, y_type, relation):
            query = (
                'UNWIND $lines as line '
                'MERGE (node1: `{x_type}` {{ id: line.x_id }}) '
                'ON CREATE SET node1.name = line.x_name '
                'MERGE (node2: `{y_type}` {{ id: line.y_id }}) '
                'ON CREATE SET node2.name = line.y_name  '
                'MERGE (node1)-[e: `{relation}` ]->(node2) '
                'ON CREATE SET e.layer1_att = line.layer1_att, e.layer2_att= line.layer2_att '
            ).format(x_type=x_type,  y_type=y_type, relation=relation)
            return query

        with self.driver.session(database=self.database) as session:
            for idx, row in attentions.iterrows():
                if idx % self.batch_size == 0:
                    if len(lines) > 0:
                        query = get_query(x_type, y_type, relation)
                        session.run(query, lines=lines)
                    x_type = row['x_type']
                    y_type = row['y_type']
                    relation = row['relation']
                    lines = []
                elif row['x_type'] == x_type and row['y_type'] == y_type and relation == relation:

                    lines += [{
                        'x_id': row['x_id'], 'y_id': row['y_id'],
                        'x_name': row['x_name'], 'y_name': row['y_name'],
                        'layer1_att': row['layer1_att'],
                        'layer2_att': row['layer2_att']
                    }]
                else:
                    query = get_query(x_type, y_type, relation)
                    session.run(query, lines=lines)
                    x_type = row['x_type']
                    y_type = row['y_type']
                    relation = row['relation']
                    lines = []
            query = get_query(x_type, y_type, relation)
            session.run(query, lines=lines)

    def add_prediction(self):
        prediction = pd.read_pickle(os.path.join(
            self.data_path, 'result.pkl'))['prediction']

        query = (
            'UNWIND $lines as line '
            'MATCH (node1: disease { id: line.x_id }) '
            'MATCH (node2: drug { id: line.y_id }) '
            'CREATE (node1)-[: Prediction { score: line.score, relation: "rev_indication" } ]->(node2) '
            'RETURN node1, node2'
        )
        lines = []

        with self.driver.session(database=self.database) as session:
            for disease in prediction["rev_indication"]:
                drugs = prediction["rev_indication"][disease]
                top_drugs = sorted(
                    drugs.items(), key=lambda item: item[1], reverse=True
                )[:20]
                if len(lines) >= self.batch_size:
                    session.run(query, lines=lines)
                    lines = []
                else:
                    lines += [
                        {'x_id': disease, 'y_id': item[0], 'score':float(item[1])} for item in top_drugs
                    ]
            session.run(query, lines=lines)

    def query_disease(self):
        query = (
            'MATCH (node:disease)-[:Prediction]->(:drug)'
            'RETURN node'
        )
        with self.driver.session(database=self.database) as session:
            results = session.run(query)
            res = [record['node']['id'] for record in results]
        return res

    def query_predicted_drugs(self, disease_id):
        query = (
            'MATCH (:disease { id: $id })-[edge:Prediction]->(node:drug)'
            'RETURN node, edge ORDER BY edge.score DESC'
        )
        with self.driver.session(database=self.database) as session:
            results = session.run(query, id=disease_id)
            res = [{'score': record['edge']['score'],
                    'drug_id': record['node']['id']} for record in results]
        return res

    def query_attention(self, node_id, node_type):
        query = (
            'MATCH  (p: {node_type} {{ id: "{node_id}" }})-[rel]->(neighbor) '
            'WITH neighbor, rel '
            'ORDER BY rel.layer1_att + rel.layer2_att '
            'WITH collect({{ neighbor: neighbor, rel: rel }})[..{k1}] AS neighbors_and_rels '
            'UNWIND neighbors_and_rels AS neighbor_and_rel '
            'WITH neighbor_and_rel.neighbor AS neighbor, '
            'neighbor_and_rel.rel AS rel '
            'MATCH(neighbor)-[rel2]->(neighbor2) '
            'WITH neighbor,rel, neighbor2, rel2 '
            'ORDER BY rel2.layer1_att + rel2.layer2_att '
            'WITH neighbor, rel, '
            'collect([neighbor2, rel2])[0..{k2}] AS neighbors_and_rels2 '
            'UNWIND neighbors_and_rels2 AS neighbor_and_rel2 '
            'RETURN neighbor, rel, neighbor_and_rel2[0] AS neighbor2, neighbor_and_rel2[1] AS rel2 '
        ).format(node_type=node_type, node_id=node_id, k1=5, k2=5)

        with self.driver.session(database=self.database) as session:
            results = session.run(query)
            # session.run leads to lazy result fetch.
            # Might change to session.read_transaction later
            results = [
                [
                    {'node': record['neighbor'], 'rel': record['rel']},
                    {'node': record['neighbor2'], 'rel': record['rel2']}
                ]
                for record in results
            ]

        def getId(n):
            return n['nodeId']

        def insertChild(children, items, depth):
            if depth >= len(items):
                return children
            children_ids = list(map(getId, children))
            node = items[depth]['node']
            rel = items[depth]['rel']
            try:
                index = children_ids.index(node['id'])

            except Exception:  # item does not exist
                children.append({
                    'nodeId': node['id'],
                    'nodeType': list(node.labels)[0],
                    'score': rel['layer1_att'] + rel['layer2_att'] if depth == 0 else rel['layer1_att'],
                    'edgeInfo': rel.type,
                    'children': []
                })
                index = 0
            children[index]['children'] = insertChild(
                children[index]['children'], items, depth + 1)

            return children

        children = []
        for i in range(len(results)):
            children = insertChild(children, results[i], 0)

        tree = {
            'nodeId': node_id,
            'nodeType': node_type,
            'score': 1,
            'edgeInfo': '',
            'children': children
        }

        return tree


# %%
