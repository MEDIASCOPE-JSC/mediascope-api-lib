import pytest
import time
import pandas as pd
import os

import sys
sys.path.insert(1, "../..")


@pytest.fixture(autouse=True) # задержка между тестами
def slow_down_tests():
    yield
    time.sleep(0.1)


@pytest.fixture(scope='session')
def catalogs():
    # для работы необходимо иметь файл settings.json с настройками доступа в каталоге с тестами
    from mediascope_api.crossweb import catalogs as cwc
    return cwc.CrossWebCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


@pytest.fixture(scope='session')
def tasks():
    # для работы необходимо иметь файл settings.json с настройками доступа в каталоге с тестами
    from mediascope_api.crossweb import tasks as cwt
    return cwt.CrossWebTask(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_media(catalogs, tasks):
    # Задаем период
    # Период указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
    date_filter = [('2022-09-01', '2022-09-01')]

    # Задаем фильтр по типам пользования интернетом
    usetype_filter = [1,2,3]

    # Задаем название ресурса для отображения в DataFrame
    project_name = 'Avito'

    # Задаем фильтр по географии, в нашем случае он не требуется
    geo_filter = None

    # Задаем фильтр по демографии, в нашем случае он не требуется
    demo_filter = None

    # Задаем фильтр по медиа, в нашем случае это ID ресурса Avito
    mart_filter = 'crossMediaResourceId = 1028'

    # Указываем список срезов, чтобы сформировать структуру расчета
    slices = ["researchMonth", "useTypeName"]

    # Указываем список статистик для расчета
    statistics = ['uni']

    # Формируем задание для API Cross Web в формате JSON
    task_json = tasks.build_task('media', project_name, date_filter, usetype_filter, geo_filter,
                                 demo_filter, mart_filter, slices, statistics)

    # Отправляем задание на расчет и ждем выполнения
    task_audience = tasks.wait_task(tasks.send_audience_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_audience), project_name = project_name)

    assert not df.empty


def test_total(catalogs, tasks):
    # Задаем период
    # Период указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
    date_filter = [('2022-09-01', '2022-09-01')]

    # Задаем фильтр по типам пользования интернетом
    usetype_filter = [1,2,3]

    # Задаем фильтр по географии, в нашем случае он не требуется
    geo_filter = None

    # Задаем фильтр по демографии
    demo_filter = None

    # Задаем фильтр по медиа, в нашем случае он не требуется
    mart_filter = None

    # Указываем список срезов, чтобы сформировать структуру расчета
    slices = ["researchMonth", "useTypeName"]

    # Указываем список статистик для расчета
    statistics = ['reach', 'reachPer', 'adr', 'adrPer']

    scales = None

    # Формируем задание для API Cross Web в формате JSON
    task_json = tasks.build_task('total', "Test", date_filter, usetype_filter, geo_filter,
                                 demo_filter, mart_filter, slices, statistics, scales)

    # Отправляем задание на расчет и ждем выполнения
    task_audience = tasks.wait_task(tasks.send_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_audience), project_name = "Test")

    assert not df.empty


def test_duplication(catalogs, tasks):
    # Задаем период
    # Период указывается в виде списка ('Начало', 'Конец'). Можно указать несколько периодов
    date_filter = [('2022-09-01', '2022-09-01')]

    # Задаем фильтр по типам пользования интернетом
    usetype_filter = [1,2,3]

    # Задаем фильтр по географии, в нашем случае он не требуется
    geo_filter = None

    # Задаем фильтр по демографии
    demo_filter = None

    # Задаем фильтр по медиа и фильтр по пересечению, в нашем случае это ID ресурсов Avito и Ozon
    mart_filter = 'crossMediaResourceId in (1028, 1094)'
    duplication_mart_filter = 'crossMediaResourceId in (1028, 1094)'

    # Указываем список срезов, чтобы сформировать структуру расчета
    slices = ["crossMediaResourceName", "duplicationCrossMediaResourceId"]

    # Указываем список статистик для расчета
    statistics = ['uni']

    scales = None

    # Формируем задание для API Cross Web в формате JSON
    task_json = tasks.build_task_media_duplication('media-duplication', "Test", date_filter=date_filter,
                                                   usetype_filter=usetype_filter, geo_filter=geo_filter,
                                                   demo_filter=demo_filter, mart_filter=mart_filter,
                                                   duplication_mart_filter=duplication_mart_filter,
                                                   slices=slices, statistics=statistics, scales=None)

    # Отправляем задание на расчет и ждем выполнения
    task_dupl = tasks.wait_task(tasks.send_media_duplication_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_dupl), project_name = "Test")

    assert not df.empty