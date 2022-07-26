from flask import Flask, jsonify, request
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from config import Config
from sqlalchemy.ext.declarative import declarative_base
from schemas import TempRawdataSchema
from flask_apispec import marshal_with
import pandas as pd
from logic import all_data_operations

app = Flask(__name__)
app.config.from_object(Config)

client = app.test_client()

db_user = app.config["USER"]
host = app.config["HOST"]
port = app.config["PORT"]
db_name = app.config["DBNAME"]
db_table = app.config["DBTABLE"]
db_password = app.config["PASSWORD"]

engine = create_engine(f'postgresql://{db_user}:{db_password}@{host}:{port}/{db_name}')


session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = session.query_property()

from models import TempRawdata

Base.metadata.create_all(bind=engine)


@app.route('/labor', methods=['GET'])
def get_list():
    tbl = TempRawdata.get_data_list('2022-06-01', '2022-06-05')
    df = pd.DataFrame(tbl.get_json())
    final_df = all_data_operations(df)
    return jsonify(final_df)


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


if __name__ == '__main__':
    app.run()
