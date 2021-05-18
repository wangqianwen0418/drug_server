# %%
import logging

from flask.globals import session
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

import pandas as pd
import os

from config import SERVER_ROOT

from flask import current_app, g
# %%


def get_db():
    if 'db' not in g:
        db = Neo4jApp(server='enterprise')
        db.create_session()
        g.db = db
    return g.db


class Neo4jApp:
    k1 = 10  # upper limit of children for root node
    k2 = 5  # upper limit of children for hop-1 nodes

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
        self.database = 'drug'

        scheme = "bolt"
        port = 7687

        if server == 'local':
            host_name = "localhost"
            password = 'password'
            user = 'neo4j'
            self.database = 'neo4j'

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
                uri, auth=(user, password), encrypted=False, max_connection_lifetime=400)
        except Exception as e:
            print("Failed to create the driver:", e)

    def create_session(self):
        self.session = self.driver.session(database=self.database)

    def close_session(self):
        self.session.close()

    def close_driver(self):
        if self.driver is not None:
            self.driver.close()

    def clean_database(self):
        if not self.session:
            self.create_session()

        def delete_all(tx):
            tx.run('MATCH (n) DETACH DELETE n')
        self.session.write_transaction(delete_all)
        print('delete all nodes')

    def create_index(self):

        if not self.session:
            self.create_session()

        def create_singel_index(tx, node_label):
            tx.run(
                'CREATE INDEX IF NOT EXISTS FOR (n: `{}` ) ON (n.id)'.format(node_label))

        for node_type in self.node_types:
            self.session.write_transaction(
                create_singel_index, node_type)

    def init_database(self):
        self.clean_database()
        self.create_index()
        print('build attention graph...')
        self.build_attention()
        print('build prediction graph...')
        self.add_prediction()
        print('database initialization finished')

    def build_attention(self):

        if not self.session:
            self.create_session()

        attention_path = os.path.join(self.data_path, 'attention_all.csv')
        attentions = pd.read_csv(attention_path, dtype={
            'x_id': 'string', 'y_id': 'string'})

        lines = []
        x_type = ''
        y_type = ''
        relation = ''

        def commit_batch_attention(tx, x_type, y_type, relation, lines):
            query = (
                'UNWIND $lines as line '
                'MERGE (node1: `{x_type}` {{ id: line.x_id }}) '
                'ON CREATE SET node1.name = line.x_name '
                'MERGE (node2: `{y_type}` {{ id: line.y_id }}) '
                'ON CREATE SET node2.name = line.y_name  '
                'MERGE (node1)-[e: `{relation}` ]->(node2) '
                'ON CREATE SET e.layer1_att = line.layer1_att, e.layer2_att= line.layer2_att '
            ).format(x_type=x_type,  y_type=y_type, relation=relation)
            tx.run(query, lines=lines)

        session = self.session
        for idx, row in attentions.iterrows():
            if idx % self.batch_size == 0:
                # fulfil batchsize, commit lines
                if len(lines) > 0:
                    session.write_transaction(
                        commit_batch_attention, x_type, y_type, relation, lines=lines)
                x_type = row['x_type']
                y_type = row['y_type']
                relation = row['relation']
                lines = []
            elif row['x_type'] == x_type and row['y_type'] == y_type and relation == relation:
                # add new line
                lines += [{
                    'x_id': row['x_id'], 'y_id': row['y_id'],
                    'x_name': row['x_name'], 'y_name': row['y_name'],
                    'layer1_att': row['layer1_att'],
                    'layer2_att': row['layer2_att']
                }]
            else:
                # commit previous lines, change x y type
                session.write_transaction(
                    commit_batch_attention, x_type, y_type, relation, lines=lines)
                x_type = row['x_type']
                y_type = row['y_type']
                relation = row['relation']
                lines = []
        session.write_transaction(
            commit_batch_attention, x_type, y_type, relation, lines=lines)

    def add_prediction(self):

        if not self.session:
            self.create_session()

        prediction = pd.read_pickle(os.path.join(
            self.data_path, 'result.pkl'))['prediction']

        def commit_batch_prediction(tx, lines):
            query = (
                'UNWIND $lines as line '
                'MATCH (node1: disease { id: line.x_id }) '
                'MATCH (node2: drug { id: line.y_id }) '
                'CREATE (node1)-[: Prediction { score: line.score, relation: "rev_indication" } ]->(node2) '
                'RETURN node1, node2'
            )
            tx.run(query, lines=lines)

        lines = []

        for disease in prediction["rev_indication"]:
            drugs = prediction["rev_indication"][disease]
            top_drugs = sorted(
                drugs.items(), key=lambda item: item[1], reverse=True
            )[:50]
            if len(lines) >= self.batch_size:
                self.session.write_transaction(
                    commit_batch_prediction, lines=lines)
                lines = []
            else:
                lines += [
                    {'x_id': disease, 'y_id': item[0], 'score':float(item[1])} for item in top_drugs
                ]
        self.session.write_transaction(commit_batch_prediction, lines=lines)

    def query_diseases(self):

        if not self.session:
            self.create_session()

        def commit_diseases_query(tx):
            query = (
                'MATCH (node:disease)-[:Prediction]->(:drug)'
                'RETURN node'
            )
            results = tx.run(query)
            disease_ids = [record['node']['id'] for record in results]
            disease_ids = list(set(disease_ids))
            return disease_ids

        return self.session.read_transaction(commit_diseases_query)

    def query_predicted_drugs(self, disease_id):

        def commit_drugs_query(tx, disease_id):
            query = (
                'MATCH (:disease { id: $id })-[edge:Prediction]->(node:drug)'
                'RETURN node, edge ORDER BY edge.score DESC'
            )
            results = tx.run(query, id=disease_id)
            drug_ids = [{'score': record['edge']['score'],
                         'drug_id': record['node']['id']} for record in results]
            return drug_ids

        drug_ids = self.session.read_transaction(
            commit_drugs_query, disease_id)

        return drug_ids

    @staticmethod
    def get_tree(results, node_type, node_id):
        """
        Params:
            reuslts: return from query, Array<[{node, rel}, [node, rel]]>
            node_type: type of root node
            node_id: id of root node
        Return:
            tree
        """
        def insertChild(children, items, depth, skip_nodes):
            if depth >= len(items):
                return children
            children_ids = list(map(lambda n: n['nodeId'], children))
            node = items[depth]['node']
            if node['id'] in skip_nodes:
                return children
            rel = items[depth]['rel']
            try:
                index = children_ids.index(node['id'])

            except Exception:  # item does not exist
                children.append({
                    'nodeId': node['id'],
                    'nodeType': list(node.labels)[0],
                    'score':  (rel['layer1_att'] + rel['layer2_att']) if depth == 0 else rel['layer1_att'],
                    'edgeInfo': rel.type,
                    'children': []
                })
                index = 0
            children[index]['children'] = insertChild(
                children[index]['children'], items, depth + 1, skip_nodes)

            return children

        children = []
        for i in range(len(results)):
            children = insertChild(children, results[i], 0, [node_id])

        tree = {
            'nodeId': node_id,
            'nodeType': node_type,
            'score': 1,
            'edgeInfo': '',
            'children': children
        }

        return tree

    @staticmethod
    def commit_attention_query(tx, node_type, node_id):
        query = (
            'MATCH  (p: {node_type} {{ id: "{node_id}" }})<-[rel]-(neighbor) '
            'WHERE NOT (p)-[:Prediction]-(neighbor) '
            'WITH neighbor, rel '
            'ORDER BY (rel.layer1_att ) DESC '
            'WITH collect([ neighbor, rel])[..{k1}] AS neighbors_and_rels '
            'UNWIND neighbors_and_rels AS neighbor_and_rel '
            'WITH neighbor_and_rel[0] AS neighbor, '
            'neighbor_and_rel[1] AS rel '
            'MATCH(neighbor)<-[rel2]-(neighbor2) WHERE NOT (neighbor)-[:Prediction]-(neighbor2) '
            'WITH neighbor,rel, neighbor2, rel2 '
            'ORDER BY rel2.layer1_att DESC '
            'WITH neighbor, rel, '
            'collect([neighbor2, rel2])[0..{k2}] AS neighbors_and_rels2 '
            'UNWIND neighbors_and_rels2 AS neighbor_and_rel2 '
            'RETURN neighbor, rel, neighbor_and_rel2[0] AS neighbor2, neighbor_and_rel2[1] AS rel2 '
        ).format(node_type=node_type, node_id=node_id, k1=Neo4jApp.k1, k2=Neo4jApp.k2)

        results = tx.run(query)

        results = [
            [
                {'node': record['neighbor'], 'rel': record['rel']},
                {'node': record['neighbor2'], 'rel': record['rel2']}
            ]
            for record in results
        ]

        return results

    def query_attention(self, node_id, node_type):
        if not self.session:
            self.create_session()

        results = self.session.read_transaction(
            Neo4jApp.commit_attention_query, node_type, node_id)

        tree = self.get_tree(results, node_type, node_id)

        return tree

    def query_metapath_summary(self, root_nodes):

        query = (
            'UNWIND $nodes as node '
            'MATCH  (p: node.type {{ id: node.id }})<-[rel]-(neighbor) '
            'WHERE NOT (p)-[:Prediction]-(neighbor) '
            'WITH neighbor, rel '
            'ORDER BY (rel.layer1_att ) DESC '
            'WITH collect([ neighbor, rel])[..{k1}] AS neighbors_and_rels '
            'UNWIND neighbors_and_rels AS neighbor_and_rel '
            'WITH neighbor_and_rel[0] AS neighbor, '
            'neighbor_and_rel[1] AS rel '
            'MATCH(neighbor)<-[rel2]-(neighbor2) WHERE NOT (neighbor)-[:Prediction]-(neighbor2) '
            'WITH neighbor,rel, neighbor2, rel2 '
            'ORDER BY rel2.layer1_att DESC '
            'WITH neighbor, rel, '
            'collect([neighbor2, rel2])[0..{k2}] AS neighbors_and_rels2 '
            'UNWIND neighbors_and_rels2 AS neighbor_and_rel2 '
            'RETURN neighbor, rel, neighbor_and_rel2[0] AS neighbor2, neighbor_and_rel2[1] AS rel2 '
        ).format(k1=Neo4jApp.k1, k2=Neo4jApp.k2)


# %%
