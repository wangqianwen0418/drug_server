import json
import numpy as np

import flask
from flask import request, jsonify, Blueprint, current_app, g
from utils import better_json_encoder

api = Blueprint('api', __name__)

api.json_encoder = better_json_encoder(flask.json.JSONEncoder)

######################
# API Starts here
######################


@api.route('/test', methods=['GET'])
def test():
    return 'api test'


@api.route('/diseases', methods=['GET'])
def get_diseases():
    '''
    :return: diseaseID[]
    '''
    model_loader = current_app.config['MODEL_LOADER']
    return jsonify(model_loader.get_diseases())


@api.route('/attention', methods=['GET'])
def get_attention():
    '''
    :return: diseaseID[]
    E.g.: [base_url]/api/attention?disease=0&drug=0
    '''
    disease_id = request.args.get('disease', None, type=str)
    drug_id = request.args.get('drug', None, type=str)
    model_loader = current_app.config['MODEL_LOADER']
    attention = {}
    attention['disease'] = model_loader.get_node_attention(
        'disease', disease_id)
    attention['drug'] = model_loader.get_node_attention(
        'drug', drug_id)

    return jsonify(attention)


@api.route('/drug_predictions', methods=['GET'])
def get_drug_predictions():
    '''
    get drug predictions
    E.g.: [base_url]/api/drug_predictions?disease_id=17494&top_n=10

    :return: {score:number, drug_id: int, disease_id: int }[]
    '''
    disease_id = request.args.get('disease_id', None, type=str)
    top_n = request.args.get('top_n', 10, type=int)
    model_loader = current_app.config['MODEL_LOADER']
    predictions = model_loader.get_drug_disease_prediction(
        disease_id=disease_id, top_n=top_n)
    return jsonify(predictions)
