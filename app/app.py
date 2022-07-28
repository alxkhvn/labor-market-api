from flask import Flask, jsonify, request
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from config import Config
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from logic import all_data_operations
from datetime import datetime

today = datetime.today().strftime("%Y-%m-%d")

app = Flask(__name__)
app.config.from_object(Config)

client = app.test_client()

# db_user = app.config["USER"]
# host = app.config["HOST"]
# port = app.config["PORT"]
# db_name = app.config["DBNAME"]
# db_table = app.config["DBTABLE"]
# db_password = app.config["PASSWORD"]
#
# engine = create_engine(f'postgresql://{db_user}:{db_password}@{host}:{port}/{db_name}')

database_url = 'postgres://qeklulawkyaukv:9c32130f9549a3f4976886e9ff3c14ff175324ecd6663fc4696c988ca58d2dce@ec2-54-208-104-27.compute-1.amazonaws.com:5432/d3sf6nhpsnmj14'

engine = create_engine(database_url)

session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = session.query_property()

from models import TempRawdata

Base.metadata.create_all(bind=engine)


@app.route('/labor', methods=['GET'])
def get_list():
    try:
        args = request.args
        print(args)
        start_date = args.get('start_date', type=str)
        end_date = args.get('end_date', default=today, type=str)
        tbl = TempRawdata.get_data_list(start_date, end_date)
        df = pd.DataFrame(tbl.get_json())
        final_df = all_data_operations(df)
    except Exception as e:
        return {'message': str(e)}, 400
    return jsonify(final_df)


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


if __name__ == '__main__':
    app.run()
