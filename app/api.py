from flask import Flask, jsonify, request
from config import Config
import datetime
from models import *

today = datetime.datetime.today()

app = Flask(__name__)
app.config.from_object(Config)

client = app.test_client()

db_user = app.config["USER"]
host = app.config["HOST"]
port = app.config["PORT"]
db_name = app.config["DBNAME"]
db_table = app.config["DBTABLE"]
db_password = app.config["PASSWORD"]


@app.route('/labor', methods=['GET'])  # route() decorator tells Flask what URL should trigger our function
def get_list():
    data = conn_to_db(db_user, host, port, db_name, db_password, db_table)

    # Make filter by params
    params = request.json
    data = filter_by_params(data, params['start_date'], params['end_date'])

    # Do all operations with data
    df_correct_form(data)
    hh_regions_to_edunav(data)
    hh_ids_to_edunav(data)
    salary_df = create_salary_df(data)
    avg_salary(salary_df)
    change_currency(salary_df)
    create_id_vac_dicts()
    fill_id_vac_dict(salary_df, salary_id_vac_dict)
    fill_id_vac_dict(data, df_id_vac_dict)

    main_dic = {}

    for i in edunav_id_list:
        try:
            ans = combine_stat(i, data, salary_df)
            main_dic[i] = ans
        except:
            pass

    return jsonify(main_dic)
