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
    from mediascope_api.counter import catalogs as cwc
    return cwc.CounterCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


@pytest.fixture(scope='session')
def tasks():
    # для работы необходимо иметь файл settings.json с настройками доступа в каталоге с тестами
    from mediascope_api.counter import tasks as cwt
    return cwt.CounterTask(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_counter(catalogs, tasks):
    # Задаем название ресурса для отображения в DataFrame
    project_name = 'Test'

    # Задаем период. Фильтр обязательный
    date_filter = [('2023-06-01', '2023-06-01')]

    # Задаем фильтр по типу клиента (партнера), в данном случае перечисляем все:
    # Фильтр обязательный
    area_type_filter = ["audience", "advertisingCampaign", "advertisingNetwork"]

    # Задаем фильтр по профилю
    mart_filter = "advertisementAgencyId = 29"

    # Задаем список клиентов (партнеров)
    partner_filter = None#["mail_network"]

    # Фильтр по tmsec, задается списком, в нашем случае не требуется
    tmsec_filter = None

    # Задаем фильтр по географии
    geo_filter = "countryName = 'РОССИЯ'"

    # Фильтр по типам устройств
    device_type_filter=None

    # Указываем список срезов, чтобы сформировать структуру расчета
    slices=["partnerName", "tmsec", "researchDate",
            "advertisementAgencyName", "brandId",
            "countryName", "cityName", "provinceName"]

    # Указываем список статистик для расчета
    #statistics = ["hitsVisits", "uniqsVisits"]
    statistics = ["hitsVisits"]

    # Задаем процент данных на которых будет выполнен расчет
    # Для периода больше одного дня sampling не может быть равен 100%
    # sampling по умолчанию = 42 :)
    sampling=1

    # Формируем задание для API Cross Web в формате JSON
    task_json = tasks.build_task(task_name=project_name,
                                        date_filter=date_filter,
                                        area_type_filter=area_type_filter,
                                        partner_filter=partner_filter,
                                        tmsec_filter=tmsec_filter,
                                        geo_filter=geo_filter,
                                        device_type_filter=device_type_filter,
                                        mart_filter=mart_filter,
                                        slices=slices,
                                        statistics=statistics,
                                        sampling=sampling)

    # Отправляем задание на расчет и ждем выполнения
    task = tasks.wait_task(tasks.send_task(task_json))

    # Получаем результат
    tasks.result2table(tasks.get_result(task), project_name = project_name)
