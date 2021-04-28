import json
import os


def load_settings(settings_fname='settings.json'):
    """
        Загрузить настройки из файла

        Parameters
        ----------

        settings_fname : str
            Имя файла с настройками, по умолчанию имя файла: settings.json
        Returns
        -------

        username : str
            Имя пользователя
        passw : str
            пароль пользователя
        root_url : str
            URL к API
        client_id : str
            Идентификатор клиента для доступа к API
        client_secret : str
            Секрет для доступа к API
        auth_server : str
            URL к серверу авторизации
    """

    if settings_fname is None:
        settings_fname = 'settings.json'

    with open(settings_fname) as datafile:
        jd = json.load(datafile)
        return jd['username'], \
               jd['passw'], \
               jd['root_url'], \
               jd['client_id'], \
               jd['client_secret'], \
               jd['auth_server']

