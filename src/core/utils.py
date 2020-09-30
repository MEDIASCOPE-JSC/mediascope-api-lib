import json


def load_settings(settings_fname='settings.json'):
    with open(settings_fname) as datafile:
        jd = json.load(datafile)
        # TODO: Добавить client_id и client_secret в параметры
        return jd['username'], jd['passw']
