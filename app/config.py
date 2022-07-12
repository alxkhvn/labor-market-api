import os


class Config:
    DEBUG = False
    DBNAME = os.environ['DBNAME']
    HOST = os.environ['HOST']
    PORT = os.environ['PORT']
    USER = os.environ['USER']
    PASSWORD = os.environ['PASSWORD']
