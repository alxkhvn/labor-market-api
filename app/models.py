from app import db, Base, session
from schemas import TempRawdataSchema
from flask_apispec import marshal_with


class TempRawdata(Base):
    __tablename__ = 'temp_raw_data_tbl'
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
