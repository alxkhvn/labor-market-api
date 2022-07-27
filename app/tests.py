from app import client


def test_get():
    res = client.get('/labor', json={'start_date': '2022-03-01', 'end_date': '2022-03-31'})

    assert res.status_code == 200

    assert len(res.get_json()) == 176
    assert res.get_json()[0]['id'] == 1
