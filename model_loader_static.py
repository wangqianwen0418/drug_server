# %%
import json
import matplotlib.pyplot as plt
from argparse import ArgumentParser
import pickle
import copy
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score, average_precision_score
from tqdm.auto import tqdm
import pandas as pd
import argparse
import numpy as np
import math
import urllib.request
import os
import scipy.io

from functools import lru_cache

# %%


class ModelLoader():
    def __init__(self, data_path=None):
        if (not data_path):
            raise ValueError('data path is not provided')
        else:
            self.data_path = data_path

        if not os.path.isdir(self.data_path):
            if 's3' not in self.data_path:
                raise IOError('data path {} does not exist'.format(data_path))

        self.df_train = None
        self.predictions = None
        self.attentions = None

    def load_predictions(self):
        '''
        load stored prediction results
        '''
        data_path = self.data_path
        # with open(os.path.join(data_path, 'result.pkl'), 'rb') as f:
        #     results = pickle.load(f)
        results = pd.read_pickle(os.path.join(data_path, 'result.pkl'))
        self.predictions = results['prediction']

    @lru_cache(maxsize=10)
    def get_diseases(self):
        '''
        :return: string[]
        '''
        if not self.predictions:
            self.load_predictions()
        return [key for key in self.predictions["rev_indication"]]

    def get_drug_disease_prediction(self, disease_id=None, drug_id=None, rel="indication", top_n=10):
        '''
        :param rel: (string), relationship, either "contraindication", "indication", or "off-label"
        :param drug_id: index of drug
        :param disease_id: index of disease
        :param top_n: number of predictions returned
        :return: array of {score:number, drug_id: string, disease_id: string}, sorted by score, length = top_n if not drug_id else 1
        '''
        if not self.predictions:
            self.load_predictions()
        preds_all = self.predictions

        if disease_id is None and drug_id is None:
            raise ValueError('Expected either drug_id or disease_id args')

        if rel not in ['contraindication', 'indication', 'off-label']:
            raise ValueError(
                'rel must be "contraindication", "indication", "off-label"')

        if drug_id is None:
            drugs = preds_all['rev_{}'.format(rel)][disease_id]

            return [{"score": item[1], "drug_id": item[0]} for item in sorted(
                drugs.items(), key=lambda item: item[1], reverse=True
            )[:top_n]]

        if disease_id is None:
            diseases = preds_all[rel][drug_id]
            return [{"score": x[1], "disease_id": x[0]} for x in sorted(
                enumerate(diseases), key=lambda x: x[1], reverse=True
            )[:top_n]]

        else:
            score = preds_all[rel][drug_id][disease_id]
            return [{"score": score, "drug_id": drug_id, "disease_id": disease_id}]

    @lru_cache(maxsize=32)
    def get_node_attention(self, node_type, node_id, thr=10):
        if self.attentions is None:
            attention_path = os.path.join(self.data_path, 'attention_all.csv')
            attentions = pd.read_csv(attention_path, dtype={
                                     'x_id': 'string', 'y_id': 'string'})
            attentions = attentions[~attentions['relation'].str.contains(
                'contraindication')]
            self.attentions = attentions

        else:
            attentions = self.attentions

        used_nodes = [node_id]

        def get_children(node_id, node_type, depth, attentions, thr):

            rows = attentions[(attentions['y_id'] == node_id)
                              & (attentions['y_type'] == node_type)
                              ].sort_values('layer1_att', ascending=False
                                            ).head(thr)
            children = []
            for idx, row in rows.iterrows():
                child = {}
                if (row['x_id'] not in used_nodes):
                    # used_nodes.append(row['y_id']) #comment this out, only disable the duplication of root node
                    child['nodeId'] = row['x_id']
                    child['nodeType'] = row['x_type']

                    child['score'] = row['layer1_att'] if depth == 2 \
                        else row['layer1_att'] + row['layer2_att']
                    child['edgeInfo'] = row['relation']

                    if depth < 2:
                        child['children'] = get_children(
                            child['nodeId'], child['nodeType'], depth+1, attentions, int(thr/2))
                    else:
                        child['children'] = []
                    children.append(child)

            return children

        attention = {
            'nodeId': node_id,
            'nodeType': node_type,
            'score': 1,
            'edgeInfo': '',
            'children': get_children(node_id, node_type, 1, attentions, thr)
        }

        return attention

    def get_subgraph(self, drug_id, disease_id):
        return 0


# %%
if __name__ == "__main__":
    model_loader = ModelLoader()

    model_loader.load_model()
    model_loader.get_prediction_from_df(model_loader.df_test)
    model_loader.get_drug_disease_prediction(disease_id=17494)
    model_loader.get_drug_disease_prediction(
        drug_id=1159, rel='contraindication')
