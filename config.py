import os
from model_loader_static import ModelLoader
SERVER_ROOT = os.path.dirname(os.path.abspath(__file__))


class Config(object):
    MODEL_FOLDER = os.path.join(SERVER_ROOT, 'collab_delivery/')
    FRONT_ROOT = os.path.join(SERVER_ROOT, 'build')
    DATA_FOLDER = os.path.join(SERVER_ROOT, 'data')
    STATIC_FOLDER = os.path.join(SERVER_ROOT, 'build/static')
    MODEL_LOADER = ModelLoader(os.path.join(SERVER_ROOT, 'collab_delivery/'))


class ProductionConfig(Config):
    pass


class DevelopmentConfig(Config):
    pass


class TestingConfig(Config):
    pass
