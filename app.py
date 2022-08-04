import sqlalchemy as db
from flask import Flask, jsonify, request
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
from config import Config
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from logic import all_data_operations
from datetime import datetime
from flask_apispec import marshal_with
from schemas import TempRawdataSchema
import json
from config import Config

today = datetime.today().strftime("%Y-%m-%d")

app = Flask(__name__)
app.config.from_object(Config)


client = app.test_client()

# engine = create_engine(
#     f'postgresql://{app.config["USER"]}:{app.config["PASSWORD"]}@{app.config["HOST"]}:{app.config["PORT"]}/{app.config["DBNAME"]}')

engine = create_engine(Config.SQLALCHEMY_DATABASE_URI)

session = scoped_session(sessionmaker(
    autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = session.query_property()


Base.metadata.create_all(bind=engine)


class TempRawdata(Base):
    __tablename__ = 'raw_data_tbl'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(500))
    area = db.Column(db.String(500))
    salary = db.Column(db.String(500))
    experience = db.Column(db.String(500))
    schedule = db.Column(db.String(500))
    employment = db.Column(db.String(500))
    key_skills = db.Column(db.String(500))
    specializations = db.Column(db.String(500))
    published_at = db.Column(db.Date)

    @classmethod
    @marshal_with(TempRawdataSchema(many=True))
    def get_data_list(cls, start_date, end_date):
        try:
            tbl = cls.query.filter(cls.published_at.between(start_date, end_date)).all()
            session.commit()
        except Exception:
            session.rollback()
            raise
        return tbl


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
    return json.dumps(final_df, ensure_ascii=False).encode('utf8')


@app.teardown_appcontext
def shutdown_session(exception=None):
    session.remove()


if __name__ == '__main__':
    app.run()
