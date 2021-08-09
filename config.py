import os
from key import S3_KEY, S3_SECRET, S3_BUCKET
# from model_loader_static import ModelLoader
SERVER_ROOT = os.path.dirname(os.path.abspath(__file__))


class Config(object):
    FRONT_ROOT = os.path.join(SERVER_ROOT, 'build')
    DATA_FOLDER = os.path.join(SERVER_ROOT, 'data')
    STATIC_FOLDER = os.path.join(SERVER_ROOT, 'build/static')
    # MODEL_LOADER = ModelLoader(os.path.join(SERVER_ROOT, 'collab_delivery/'))
    # MODEL_LOADER = ModelLoader('s3://drug-gnn-models/collaboration_delivery/')
    S3_KEY = S3_KEY
    S3_SECRET = S3_SECRET
    S3_BUCKET = S3_BUCKET

    S3_LOCATION = 'http://{}.s3.amazonaws.com/'.format(S3_BUCKET)
    SECRET_KEY = os.urandom(32)


class ProductionConfig(Config):
    pass


class DevelopmentConfig(Config):
    pass


class TestingConfig(Config):
    pass
