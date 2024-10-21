import json
import os
import datetime as dt
import pandas as pd
import requests
import subprocess


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
            jd['auth_server'], \
            jd['proxy_server'] if 'proxy_server' in jd else None


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


def get_dict_from_dataframe(df):
    """
    Формирует дикт из первой строки датафрейма пандас (используется в фильтре респондентов при передаче результата
    consumption target

    Parameters
    ----------
    df : dataframe

    Returns
    -------
    res: dict
    """
    res = {}
    if isinstance(df, pd.DataFrame):
        df_cons = df.rename(columns={"CommonWatchers": "respondent",
                                     "CommonNonWatchers": "respondent",
                                     "NGroupResp": "respondent",
                                     "NGroupDur": "respondent"
                                     })
        for col in df_cons.columns:
            if col == 'respondent':
                res[col] = df_cons.iloc[0][col]
            else:
                res[col] = json.loads(df_cons.iloc[0][col])
    return res


def format_time_column(dataframe, writer, column_names, sheet_name, index):
    """
    Изменяет формат колонки со временем при сохранении в Excel

    Parameters
    ----------
    dataframe : pandas dataframe
        Датафрейм с данными

    writer : excel writer object
        Объект для записи excel

    column_names : list
        Список колонок с данными времени

    sheet_name : str
        Имя листа

    index : boolean
        Наличие индекса в датафрейме с данными (True/False)

    """
    # получаем объекты файла excel
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    # добавляем формат отображения накопленного времени (не астрономического)
    time_format = workbook.add_format({"num_format": "[HH]:MM:SS"})
    for column_name in column_names:
        # получаем порядковый номер колонки
        col = dataframe.columns.get_loc(column_name)
        # если у нас добавляется индекс, то увеличиваем номер колонки
        if index:
            col = col + 1
        # получаем имя колонки по его номеру
        col_name = chr(ord('A') + col)
        # устанавливаем формат отображения данных в колонке
        worksheet.set_column(f"{col_name}:{col_name}", 10, cell_format=time_format)
        # перезаписываем данные колонки, т.к. для применения формата отображения данных требуется перезапись
        for row, timedelta in enumerate(dataframe[column_name], 1):
            worksheet.write(row, col, timedelta)

def get_csv_filename(task_name: str, export_path: str = '../csv', add_date: bool = True) -> str:
    """
    Формирует название CSV файла для отчета

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
    fname += '.csv'
    return os.path.join(export_path, fname)


def check_version():
    """
        Проверка установленна ли актуальная версия библиотеки

    """
    print("Получаем установленную версию библиотеки...")
    package = 'mediascope_api_lib'
    result = subprocess.run(['pip', 'show', package], stdout=subprocess.PIPE).stdout.decode('utf-8')
    if len(result):
        version_start_str = "Version: "
        start = result.find(version_start_str)
        if start == -1:
            print(f"Не найдена версия установленной библиотеки {package}")
            print(f"Проверьте полученный результат {result}")
        else:
            current_version = result[start + len(version_start_str) : result.find("\r\n", start)]
            print(f"Найдена установленная версия {current_version}")
            pypi_str = f'https://pypi.org/pypi/{package}/json'
            print(f"Проверяем актуальную версию на {pypi_str} ...")
            response = requests.get(pypi_str)
            if response.status_code == 200:
                latest_version = response.json()['info']['version']
                print(f"Найдена актуальная версия {latest_version}")
                if latest_version > current_version:
                    print(f"Требуется обновление библиотеки с версии {current_version} на {latest_version}")
                    print("Запускаем обновление...")
                    print(subprocess.run(['pip', 'install', package, "-U"], stdout=subprocess.PIPE).stdout.decode('utf-8'))
                else:
                    print("Обновление не требуется")
            else:
                print("Не могу проверить актуальную версию")
                print(response.text)
            response.close()
    else:
        print(f"Ошибка получения информации об установленной библиотеке {package}")
