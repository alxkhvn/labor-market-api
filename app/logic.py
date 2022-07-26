import numpy as np
import pandas as pd
import json
from kzt_exchangerates import Rates
from sqlalchemy import create_engine


types_df = pd.read_csv('../files/types.csv')['types']
matching_df = pd.read_csv('../files/hh_matching_with_types.csv', dtype=types_df.to_dict())
matching_dict = dict(zip(matching_df.spec_id.astype(str), matching_df.id_edunav.astype(int)))

region_df = pd.read_csv('../files/regions.csv')
region_dict = dict(zip(region_df.name, region_df.areas))

rates = Rates()
exchange_dict = rates.get_exchange_rates(from_kzt=True)['rates']

edunav_df = pd.read_csv('../files/Edunav profs.csv')
edunav_id_list = edunav_df['Код по Эдунаву'].astype(int).tolist()
edunav_id_name_dict = dict(zip(edunav_df['Код по Эдунаву'].astype(int), edunav_df['Название профессии RU'].astype(str)))

replace_dic = {"'": '"', "None": '"None"', "False": '"False"', "True": '"True"'}

salary_id_vac_dict = {}
df_id_vac_dict = {}


def filter_by_params(data, start_date, end_date):
    data = data[(data['published_at'] > start_date) & (data['published_at'] < end_date)]
    data = data.loc[:, ['id', 'name', 'area', 'salary', 'experience', 'schedule',
                    'employment', 'key_skills', 'specializations', 'published_at']]
    return data


def replace_all(text, dic):
    for i, j in dic.items():
        if text is not None:
            text = text.replace(i, j)
    return text


def loads(obj):
    try:
        return json.loads(obj)
    except (TypeError, ValueError):
        return obj


def df_correct_form(data_i):
    data = data_i.copy()

    data.fillna(value=np.NAN, inplace=True)
    data['id'] = data.id
    data['name'] = data.name
    data['specializations'] = data.specializations.apply(
        lambda x: loads(replace_all(x, replace_dic)) if x is not np.NAN else x)
    data['area'] = data.area.apply(
        lambda x: loads(replace_all(x, replace_dic))['name'] if x is not np.NAN else x)
    data['s_from'] = data.salary.apply(
        lambda x: loads(replace_all(x, replace_dic))['from'] if x is not np.NAN or None else x)
    data['s_to'] = data.salary.apply(
        lambda x: loads(replace_all(x, replace_dic))['to'] if x is not np.NAN else x)
    data['currency'] = data.salary.apply(
        lambda x: loads(replace_all(x, replace_dic))['currency'] if x is not np.NAN else x)
    data['experience'] = data.experience.apply(
        lambda x: loads(replace_all(x, replace_dic))['name'] if x is not np.NAN else x)
    data['schedule'] = data.schedule.apply(
        lambda x: loads(replace_all(x, replace_dic))['name'] if x is not np.NAN else x)
    data['employment'] = data.employment.apply(
        lambda x: loads(replace_all(x, replace_dic))['name'] if x is not np.NAN else x)
    data['key_skills'] = data.key_skills.apply(
        lambda x: loads(replace_all(x, replace_dic)) if x is not np.NAN else x)
    data['published_at'] = data.published_at
    data.replace(to_replace=['None', None], value=np.nan, inplace=True)
    return data


def hh_regions_to_edunav(data_i):
    data = data_i.copy()
    data.replace({"area": region_dict}, inplace=True)
    return data


def single_id_to_edunav(row):
    arr = set()
    for el in row:
        try:
            arr.add(matching_dict[el['id']])
        except:
            pass
    return arr


def hh_ids_to_edunav(data_i):
    data = data_i.copy()
    data['specializations'] = data.specializations.apply(lambda row: single_id_to_edunav(row))
    return data


def create_salary_df(data_i):
    salary_data = data_i.copy()
    salary_data = salary_data[salary_data.s_from.notna() | salary_data.s_to.notna()]
    return salary_data


def avg_salary(salary_data_i):
    salary_data = salary_data_i.copy()
    salary_data['s_to'] = np.where(salary_data['s_from'].notna() & salary_data['s_to'].isna(),
                                   salary_data['s_from'] * 1.1, salary_data['s_to'])
    salary_data['s_from'] = np.where(salary_data['s_to'].notna() & salary_data['s_from'].isna(),
                                     salary_data['s_to'] * 0.91, salary_data['s_from'])
    log_avg = np.round_(np.sqrt(np.multiply(salary_data['s_from'], salary_data['s_to'])))
    salary_data.insert(loc=6, column='avg', value=log_avg)
    return salary_data


def to_kzt(row):
    try:
        r = row['avg'] * exchange_dict[row['currency']]
        return r
    except:
        pass


def change_currency(salary_data_i):
    salary_data = salary_data_i.copy()
    kzt = salary_data[salary_data.currency != 'KZT'].replace({'currency': {'RUR': 'RUB', 'BYR': 'BYN'}}).apply(
        lambda row: to_kzt(row), axis=1)
    kzt = pd.DataFrame({'avg': kzt})
    salary_data.update(kzt)
    return salary_data


def create_id_vac_dicts():
    for i in edunav_id_list:
        salary_id_vac_dict[i] = []
        df_id_vac_dict[i] = []


def add_to_dict(row, dic):
    for spec in row.specializations:
        dic[spec].append(row.id)


def fill_id_vac_dict(data_i, dic):
    data = data_i.copy()
    data.apply(lambda row: add_to_dict(row, dic), axis=1)
    return data


def remove_outliers(data):
    upper_quartile = data['avg'].quantile(0.75)
    lower_quartile = data['avg'].quantile(0.25)
    iqr = (upper_quartile - lower_quartile) * 1.5
    quartile_set = (lower_quartile - iqr, upper_quartile + iqr)
    result = data[(data.avg >= quartile_set[0]) & (data.avg <= quartile_set[1])]
    return result


def q25(x):
    return x.quantile(0.25)


def q75(x):
    return x.quantile(0.75)


def salary_stat(spec, salary_data):
    temp_df = salary_data[salary_data.id.isin(salary_id_vac_dict[spec])]
    temp_df = remove_outliers(temp_df)

    avg_sal = temp_df['avg'].agg(['median', q25, q75]).to_dict()
    ex_table = temp_df.groupby('experience').avg.agg(['median', q25, q75]).to_dict()
    area_table = temp_df.groupby('area').avg.agg(['median', q25, q75]).to_dict()

    return avg_sal, ex_table, area_table


def num_stat(spec, data):
    temp_df = data[data.id.isin(df_id_vac_dict[spec])]
    vac_num = len(temp_df)
    vac_num_by_area = temp_df.area.value_counts()
    vac_num_by_area = vac_num_by_area.to_dict()
    schedule_tbl = temp_df.schedule.value_counts()
    schedule_tbl = schedule_tbl.to_dict()
    employment_tbl = temp_df.employment.value_counts()
    employment_tbl = employment_tbl.to_dict()
    return vac_num, vac_num_by_area, schedule_tbl, employment_tbl


def combine_stat(spec, data, salary_data):
    spec_name = edunav_id_name_dict[spec]
    avg_sal, ex_json, area_json = salary_stat(spec, salary_data)
    vac_num, vac_num_by_area, schedule_dist, employment_dist = num_stat(spec, data)
    result = {"id": spec,
              "name": spec_name,
              "avg_salary": avg_sal,
              "experience_salary": ex_json,
              "area_salary": area_json,
              "vacancy_num": vac_num,
              "vacancy_num_area": vac_num_by_area,
              "schedule_dist": schedule_dist,
              "employment_dist": employment_dist}

    return result


def all_data_operations(data):
    try:
        df = data.copy()

        df = df_correct_form(df)
        df = hh_regions_to_edunav(df)
        df = hh_ids_to_edunav(df)
        salary_df = create_salary_df(df)
        salary_df = avg_salary(salary_df)
        salary_df = change_currency(salary_df)
        create_id_vac_dicts()
        salary_df = fill_id_vac_dict(salary_df, salary_id_vac_dict)
        df = fill_id_vac_dict(df, df_id_vac_dict)

        main_dic = {}

        for i in edunav_id_list:
            try:
                ans = combine_stat(i, data, salary_df)
                main_dic[i] = ans
            except Exception:
                pass

        return main_dic

    except Exception:
        pass
