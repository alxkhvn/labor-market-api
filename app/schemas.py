from marshmallow import Schema, fields


class TempRawdataSchema(Schema):
    id = fields.Integer(required=True)
    name = fields.String(dump_only=True)
    area = fields.String(dump_only=True)
    salary = fields.String(dump_only=True)
    experience = fields.String(dump_only=True)
    schedule = fields.String(dump_only=True)
    employment = fields.String(dump_only=True)
    key_skills = fields.String(dump_only=True)
    specializations = fields.String(dump_only=True)
    published_at = fields.DateTime()
