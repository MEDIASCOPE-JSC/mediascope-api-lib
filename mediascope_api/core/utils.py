"""
Utils module
"""
import os
import json
import subprocess
import datetime as dt
import pandas as pd
import re
import requests

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

    with open(settings_filename, encoding='utf-8') as datafile:
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
                                     "CommonNotWatchers": "respondent",
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
    try:
        print("Получаем установленную версию библиотеки...")
        package = 'mediascope_api_lib'

        # Проверяем наличие pip
        try:
            result = subprocess.run(['pip', 'show', package],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    timeout=30,
                                    check=False)
            result_str = result.stdout.decode('utf-8')
        except subprocess.TimeoutExpired:
            print("Превышено время ожидания при проверке установленной версии")
            return
        except FileNotFoundError:
            print("Ошибка: pip не найден в системе")
            return
        except Exception as e:
            print(f"Ошибка при выполнении pip show: {e}")
            return

        if len(result_str):
            version_start_str = "Version: "
            start = result_str.find(version_start_str)
            if start == -1:
                print(f"Не найдена версия установленной библиотеки {package}")
                print(f"Проверьте полученный результат {result_str}")
            else:
                current_version = result_str[start + len(version_start_str) : result_str.find("\r\n", start)]
                print(f"Найдена установленная версия {current_version}")
                pypi_str = f'https://pypi.org/pypi/{package}/json'
                print(f"Проверяем актуальную версию на {pypi_str} ...")

                response = None
                try:
                    response = requests.get(pypi_str, timeout=10)
                    response.raise_for_status()  # Проверяем статус код

                    try:
                        data = response.json()
                        latest_version = data.get('info', {}).get('version')

                        if latest_version:
                            print(f"Найдена актуальная версия {latest_version}")
                            if latest_version > current_version:
                                print(f"Требуется обновление библиотеки с версии {current_version} на {latest_version}")
                                print("Запускаем обновление...")

                                try:
                                    update_result = subprocess.run(['pip', 'install', package, "-U"],
                                                                   stdout=subprocess.PIPE,
                                                                   stderr=subprocess.PIPE,
                                                                   timeout=120,
                                                                   check=False)

                                    if update_result.returncode == 0:
                                        print("Обновление успешно выполнено")
                                        if update_result.stdout:
                                            print(update_result.stdout.decode('utf-8'))
                                    else:
                                        print(f"Ошибка при обновлении (код {update_result.returncode})")
                                        if update_result.stderr:
                                            print(update_result.stderr.decode('utf-8'))
                                except subprocess.TimeoutExpired:
                                    print("Превышено время ожидания обновления (120 сек)")
                                except FileNotFoundError:
                                    print("Ошибка: pip не найден при попытке обновления")
                                except Exception as e:
                                    print(f"Непредвиденная ошибка при обновлении: {e}")
                            else:
                                print("Обновление не требуется")
                        else:
                            print("Не удалось получить версию из ответа PyPI")

                    except ValueError as e:
                        print(f"Ошибка декодирования JSON ответа: {e}")
                        print(f"Получен ответ: {response.text[:200]}...")

                except requests.exceptions.Timeout:
                    print(f"Внимание: Превышено время ожидания (10 сек) при подключении к {pypi_str}")
                    print("Продолжаем работу с текущей версией библиотеки")
                except requests.exceptions.ConnectionError:
                    print(f"Внимание: Нет подключения к {pypi_str}")
                    print("Продолжаем работу с текущей версией библиотеки")
                except requests.exceptions.HTTPError as e:
                    print(f"Внимание: HTTP ошибка при проверке версии: {e}")
                    print("Продолжаем работу с текущей версией библиотеки")
                except requests.exceptions.RequestException as e:
                    print(f"Внимание: Ошибка запроса при проверке версии: {e}")
                    print("Продолжаем работу с текущей версией библиотеки")
                finally:
                    if response is not None:
                        try:
                            response.close()
                        except:
                            pass  # Игнорируем ошибки при закрытии
        else:
            print(f"Ошибка получения информации об установленной библиотеке {package}")
            if result.stderr:
                print(f"stderr: {result.stderr.decode('utf-8')}")

    except Exception as e:
        print(f"Непредвиденная ошибка в функции check_version: {e}")
        # Не выбрасываем исключение, чтобы не блокировать инициализацию

def combine_dicts(*dicts):
    """
        Комбинация словарей (декартово произведение всех ключей и значений)

    """
    if not dicts:
        return {}

    # Инициализируем результат первым словарём в формате {key: (value,)}
    result = {k: (v,) for k, v in dicts[0].items()}

    # Последовательно комбинируем с остальными словарями
    for current_dict in dicts[1:]:
        new_result = {}
        for existing_key, existing_values in result.items():
            for current_key, current_value in current_dict.items():
                new_key = f"{existing_key}; {current_key}"
                new_value = existing_values + (current_value,)
                new_result[new_key] = new_value
        result = new_result

    return result

def convert_time_condition(condition_str):
    """
    Преобразует строку вида:
    '( TimeBand1 >= 100000 AND TimeBand1 < 180000 )'
    '( TimeBand1 < 230000 AND TimeBand1 >= 190000 )'
    в строку вида '10:00:00 - 18:00:00'

    Правило: первое число после >=, второе число после <
    """
    # Находим число после >=
    match_ge = re.search(r'>= (\d+)', condition_str)
    # Находим число после <
    match_lt = re.search(r'< (\d+)', condition_str)

    if match_ge and match_lt:
        start_time = match_ge.group(1)  # число после >=
        end_time = match_lt.group(1)    # число после <

        # Вставляем двоеточия
        start_formatted = insert_colons(start_time)
        end_formatted = insert_colons(end_time)

        return f"{start_formatted} - {end_formatted}"

    return condition_str

def insert_colons(num_str):
    """
    Вставляет двоеточия через каждые 2 цифры справа налево
    Всегда приводит к формату ЧЧ:ММ:СС (6 цифр с ведущими нулями)

    100000 -> 10:00:00
    180000 -> 18:00:00
    70000  -> 07:00:00
    260000 -> 26:00:00
    123400 -> 12:34:00
    """
    # Преобразуем в строку
    num_str = str(num_str)

    # Дополняем слева нулями до 6 символов (всегда ЧЧ:ММ:СС)
    num_str = num_str.zfill(6)

    # Разбиваем на пары по 2 цифры
    parts = [num_str[i:i+2] for i in range(0, len(num_str), 2)]

    # Соединяем двоеточиями
    return ':'.join(parts)