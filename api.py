import json
import numpy as np

import flask
from flask import request, jsonify, Blueprint, current_app, g
from utils import better_json_encoder

from database import get_db

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
    db = get_db()
    return jsonify(db.query_diseases())


@api.route('/attention', methods=['GET'])
def get_attention():
    '''
    :return: diseaseID[]
    E.g.: [base_url]/api/attention?disease=0&drug=0
    '''
    disease_id = request.args.get('disease', None, type=str)
    drug_id = request.args.get('drug', None, type=str)

    db = get_db()
    attention = {}
    attention['disease'] = db.query_attention(disease_id, 'disease')
    attention['drug'] = db.query_attention(drug_id, 'drug')

    return jsonify(attention)


@api.route('/drug_predictions', methods=['GET'])
def get_drug_predictions():
    '''
    get drug predictions
    E.g.: [base_url]/api/drug_predictions?disease_id=17494&top_n=10

    :return: {score:number, id: string }[]
    '''
    disease_id = request.args.get('disease_id', None, type=str)
    top_n = request.args.get('top_n', 10, type=int)
    db = get_db()
    predictions = db.query_predicted_drugs(
        disease_id=disease_id)
    return jsonify(predictions)
