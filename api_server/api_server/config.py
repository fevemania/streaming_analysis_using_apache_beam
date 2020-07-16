import os
import sys

basedir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
#basedir = os.getcwd()
#DB_CONFIG = {
#    "PROJECT_ID": "project_name",
#    "DB_NAME": "prac",
#    "TABLE_NAME": "car_machine_registration"
#}
#CAR_INFO_TABLE = '{}.{}.{}'.format(DB_CONFIG['PROJECT_ID'], DB_CONFIG['DB_NAME'], DB_CONFIG['TABLE_NAME'])

# SQLite URI compatible
WIN = sys.platform.startswith('win')
if WIN:
    prefix = 'sqlite:///'
else:
    prefix = 'sqlite:////'

class BaseConfig(object):
#    SQLALCHEMY_TRACK_MODIFICATIONS = False
#    SQLALCHEMY_RECORD_QUERIES = True
    PROJECT_ID = "project_name"
    PUBSUB_TOPIC = "dedup"

class DevelopmentConfig(BaseConfig):
    pass
#    SQLALCHEMY_DATABASE_URI = 'bigquery://project_name/prac'
    #SQLALCHEMY_DATABASE_URI = prefix + os.path.join(basedir, 'data-dev.db')

class TestingConfig(BaseConfig):
    pass
#    SQLALCHEMY_DATABASE_URI = 'bigquery://project_name/prac' #'sqlite:///' # in-memory database
 
config = {
    'development': DevelopmentConfig,
    'testing': TestingConfig
} 
