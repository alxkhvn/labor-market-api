from flask import Flask, jsonify, request
import datetime
from main import *

today = datetime.datetime.today()

app = Flask(__name__)

client = app.test_client()



db = create_engine('postgresql://postgres:batman10@localhost:5432/HeadHunter').connect()

df = pd.read_sql_table('raw_data_tbl', db)

df = df.loc[:, ['id', 'name', 'area', 'salary', 'experience', 'schedule',
                'employment', 'key_skills', 'specializations', 'published_at']]


def filter_df(data, start_date, end_date=today):
    data = data[(data['published_at'] > start_date) & (data['published_at'] < end_date)]
    return data


@app.route('/labor', methods=['GET'])  # route() decorator tells Flask what URL should trigger our function
def get_list():
    params = request.json
    data = conn_to_db()

    '''Make filter by params'''
    data = filter_df(data, params['start_date'], params['end_date'])

    '''Do all operations with data'''
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
