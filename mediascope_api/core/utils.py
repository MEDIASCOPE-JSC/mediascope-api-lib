import json
import os
import datetime as dt


def load_settings(settings_filename: str = 'settings.json'):
    """
        Загрузить настройки из файла

        Parameters
        ----------

        settings_filename : str
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

    if settings_filename is None:
        settings_filename = 'settings.json'

    with open(settings_filename) as datafile:
        jd = json.load(datafile)
        return jd['username'], \
               jd['passw'], \
               jd['root_url'], \
               jd['client_id'], \
               jd['client_secret'], \
               jd['auth_server']


def get_excel_filename(task_name: str, export_path: str = '../excel', add_date: bool = True) -> str:
    """
    Формирует название Excel файла для отчета

    Parameters
    ----------
    task_name : str
        Название отчета/задания
    export_path: : str
        Путь к файл
    add_date: str

    Returns
    -------
    """
    if not os.path.exists(export_path):
        os.mkdir(export_path)
    fname = task_name
    if add_date:
        fname += '-' + dt.datetime.now().strftime('%Y%m%d_%H%M%S')
    fname += '.xlsx'
    return os.path.join(export_path, fname)
