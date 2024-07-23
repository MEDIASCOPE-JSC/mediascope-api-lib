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
    from mediascope_api.mediavortex import catalogs as cwc
    return cwc.MediaVortexCats(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


@pytest.fixture(scope='session')
def tasks():
    # для работы необходимо иметь файл settings.json с настройками доступа в каталоге с тестами
    from mediascope_api.mediavortex import tasks as cwt
    return cwt.MediaVortexTask(settings_filename=os.path.join(os.path.dirname(__file__), "settings.json"))


def test_timeband_overall_figures_basic_channels_figures(tasks):
    # Период указывается в виде списка ('Начало', 'Конец') в формате 'YYYY-MM-DD'.
    date_filter = [('2022-08-01', '2022-08-31')]

    # Задаем дни недели
    weekday_filter = None

    # Задаем тип дня
    daytype_filter = None

    # Задаем временной интервал
    time_filter = None

    # Задаем ЦА: Все 18+
    basedemo_filter = 'age >= 18'

    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    targetdemo_filter = None

    # Задаем место просмотра
    location_filter=None

    # Задаем каналы
    company_filter='tvNetId = 1'

    # Указываем список статистик для расчета
    statistics = ['RtgPer', 'Share', 'AvReachPer', 'ATVReach']

    # Указываем срезы: телесети
    slices = ['tvNetName']

    # Задаем условия сортировки: телесеть (от а до я)
    sortings = {"tvNetName":"ASC"}

    # Задаем опции расчета
    options = {
        "kitId": 1, #TV Index Russia all
        "totalType": "TotalChannels" #база расчета Share: Total Channels. Возможны опции: TotalTVSet, TotalChannelsThem
    }

    # Формируем задание для API TV Index в формате JSON
    task_json = tasks.build_timeband_task(date_filter=date_filter, weekday_filter=weekday_filter,
                                          daytype_filter=daytype_filter, company_filter=company_filter,
                                          time_filter=time_filter, basedemo_filter=basedemo_filter,
                                          targetdemo_filter=targetdemo_filter,location_filter=location_filter,
                                          slices=slices, statistics=statistics, sortings=sortings, options=options)

    # Отправляем задание на расчет и ждем выполнения
    task_timeband = tasks.wait_task(tasks.send_timeband_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_timeband))

    # Приводим порядок столбцов в соответствие с условиями расчета
    df = df[slices+statistics]

    assert not df.empty

def test_crosstab_top_programs_by_rating(tasks):
    # Задаем период
    # Период указывается в виде списка ('Начало', 'Конец') в формате 'YYYY-MM-DD'. Можно указать несколько периодов
    date_filter = [('2023-09-01', '2023-09-30')]

    # Задаем дни недели
    weekday_filter = None

    # Задаем тип дня
    daytype_filter = None

    # Задаем ЦА: Население 100+
    basedemo_filter = 'cube100Plus100Minus = 1'

    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    targetdemo_filter = None

    # Задаем место просмотра
    location_filter=None

    # Задаем каналы
    company_filter = 'tvNetId IN (1,2,4,10,11,12,13,16,40,60,83,84,86,204,205,206,255,257,258,259,260,286,326,329,340,356,376,393,502,545)'

    # Указываем фильтр программ: жанр - Телесериал и длительность более 300 секунд (5 минут)
    program_filter = 'programTypeId = 1 and programDuration >= 300 and programId = 303783'

    # Фильтр блоков
    break_filter = None

    # Фильтр роликов
    ad_filter = None

    # Указываем список срезов (группировок)
    slices = [
        'programId',
        'programName', #Название программы
        'tvCompanyName', #Телекомпания
        'programTypeName', #Программа жанр
    ]
    # Указываем список статистик для расчета
    statistics = ['QuantitySum','RtgPerAvg', 'ShareAvg']

    # Задаем условия сортировки: рейтинг (по убыв.)
    sortings = {'RtgPerAvg':'DESC'}

    # Задаем опции расчета
    options = {
        "kitId": 1, #TV Index Russia all
        "issueType": "PROGRAM" #Тип события - Программы
    }

    # Формируем задание для API TV Index в формате JSON
    task_json = tasks.build_crosstab_task(date_filter=date_filter, weekday_filter=weekday_filter,
                                          daytype_filter=daytype_filter, company_filter=company_filter,
                                          location_filter=location_filter, basedemo_filter=basedemo_filter,
                                          targetdemo_filter=targetdemo_filter,program_filter=program_filter,
                                          break_filter=break_filter, ad_filter=ad_filter,
                                          slices=slices, statistics=statistics, sortings=sortings, options=options)

    # Отправляем задание на расчет и ждем выполнения
    task_crosstab = tasks.wait_task(tasks.send_crosstab_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_crosstab))
    # Приводим порядок столбцов в соответствие с условиями расчета
    df = df[slices+statistics]

    assert not df.empty


def test_simple_programs_all_channels(tasks):
    # Задаем период
    # Период указывается в виде списка ('Начало', 'Конец') в формате 'YYYY-MM-DD'. Можно указать несколько периодов
    date_filter = [('2023-01-01', '2023-01-01')]

    # Задаем дни недели
    weekday_filter = None

    # Задаем тип дня
    daytype_filter = None

    # Задаем ЦА
    basedemo_filter = None

    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    targetdemo_filter = None

    # Задаем место просмотра
    location_filter=None

    # Задаем каналы
    company_filter = None

    # Указываем фильтр программ: продолжительность от 5 минут (300 секунд)
    program_filter = 'programDuration >= 300 and programSpotId = 4869837614'

    # Фильтр блоков
    break_filter = None

    # Фильтр роликов
    ad_filter = None

    # Указываем список срезов
    slices = ['programSpotId', #Программа ID выхода, обязательный атрибут!
              'researchDate', #Дата, обязательный атрибут!
              'programName', #Название программы
              'tvCompanyName', #Телекомпания
              'researchWeekDay', #День недели
              'programStartTime', #Программа время начала
              'programFinishTime', #Программа время окончания
              'programCategoryName', #Программа категория
              'programIssueDescriptionName', #Программа описание выпуска
              'programProducerYear' #Программа дата создания
              ]
    # Указываем список статистик для расчета
    statistics = ['RtgPer', 'Share']

    # Задаем условия сортировки: телекомпания (от а до я), дата (от старых к новым), время начала (по возр.)
    sortings = {'tvCompanyName':'ASC', 'researchDate':'ASC', 'programStartTime':'ASC'}

    # Задаем опции расчета
    options = {
        "kitId": 1 #TV Index Russia all
    }

    # Формируем задание для API TV Index в формате JSON
    task_json = tasks.build_simple_task(date_filter=date_filter, weekday_filter=weekday_filter,
                                        daytype_filter=daytype_filter, company_filter=company_filter,
                                        location_filter=location_filter, basedemo_filter=basedemo_filter,
                                        targetdemo_filter=targetdemo_filter,program_filter=program_filter,
                                        break_filter=break_filter, ad_filter=ad_filter,
                                        slices=slices, statistics=statistics, sortings=sortings, options=options)

    # Отправляем задание на расчет и ждем выполнения
    task_timeband = tasks.wait_task(tasks.send_simple_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_timeband))

    # Приводим порядок столбцов в соответствие с условиями расчета
    df = df[slices+statistics]

    assert not df.empty


def test_cities(tasks):
    # Период указывается в виде списка ('Начало', 'Конец') в формате 'YYYY-MM-DD'.
    date_filter = [('2022-01-01', '2022-06-30')]

    # Задаем дни недели
    weekday_filter = None

    # Задаем тип дня
    daytype_filter = None

    # Задаем временной интервал
    time_filter = None

    # Задаем ЦА: Все 25+
    basedemo_filter = 'age >= 25'

    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    targetdemo_filter = None

    # Задаем место просмотра: дом
    location_filter = 'locationId = 1'

    # Задаем каналы: РОССИЯ 1
    company_filter = 'tvNetId IN (2)'

    # Указываем список статистик для расчета
    statistics = ['ATVReach', 'Share', 'AvReachPer']

    # Указываем срезы
    slices = [
        'regionName', #регион
        'tvNetName', #телесеть
        'researchMonth'#месяц
    ]

    # Задаем условия сортировки: телесеть (от а до я), месяц (по возр.)
    sortings = {'tvNetName': 'ASC', 'researchMonth': 'ASC'}

    # Задаем опции расчета
    options = {
        "kitId": 3, #TV Index Cities
        "totalType": "TotalChannels" #база расчета Share: Total Channels. Возможны опции: TotalTVSet, TotalChannelsThem
    }

    regions_dict = {
        40: 'БАРНАУЛ',
        #18: 'ВЛАДИВОСТОК',
        #5: 'ВОЛГОГРАД',
        #8: 'ВОРОНЕЖ',
        #12: 'ЕКАТЕРИНБУРГ',
        #25: 'ИРКУТСК',
        #19: 'КАЗАНЬ',
        #45: 'КЕМЕРОВО',
        #23: 'КРАСНОДАР',
        #17: 'КРАСНОЯРСК',
        #1: 'МОСКВА',
        #4: 'НИЖНИЙ НОВГОРОД',
        #15: 'НОВОСИБИРСК',
        #21: 'ОМСК',
        #14: 'ПЕРМЬ',
        #9: 'РОСТОВ-НА-ДОНУ',
        6: 'САМАРА',
        #2: 'САНКТ-ПЕТЕРБУРГ',
        #10: 'САРАТОВ',
        #39: 'СТАВРОПОЛЬ',
        #3: 'ТВЕРЬ',
        #55: 'ТОМСК',
        #16: 'ТЮМЕНЬ',
        #20: 'УФА',
        #26: 'ХАБАРОВСК',
        #13: 'ЧЕЛЯБИНСК',
        #7: 'ЯРОСЛАВЛЬ'
    }

    tsks = []
    print("Отправляем задания на расчет")

    # Для каждого региона формируем задание и отправляем на расчет
    for reg_id, reg_name in regions_dict.items():

        #Передаем id региона в company_filter
        init_company_filter = company_filter

        if company_filter is not None:
            company_filter = company_filter + f' AND regionId IN ({reg_id})'

        else:
            company_filter = f'regionId IN ({reg_id})'

        # Формируем задание для API TV Index в формате JSON
        task_json = tasks.build_timeband_task(date_filter=date_filter,
                                              weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                                              company_filter=company_filter, time_filter=time_filter,
                                              basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                                              location_filter=location_filter, slices=slices, sortings=sortings,
                                              statistics=statistics, options=options,
                                              add_city_to_basedemo_from_region=True,
                                              add_city_to_targetdemo_from_region=True
                                              )

        # Для каждого этапа цикла формируем словарь с параметрами и отправленным заданием на расчет
        tsk = {}
        tsk['task'] = tasks.send_timeband_task(task_json)
        tsks.append(tsk)
        time.sleep(2)
        print('.', end = '')

        company_filter = init_company_filter

    print(f"\nid: {[i['task']['taskId'] for i in tsks]}")

    print('')
    # Ждем выполнения
    print('Ждем выполнения')
    tasks.wait_task(tsks)
    print('Расчет завершен, получаем результат')

    # Получаем результат
    results = []
    print('Собираем таблицу')
    for t in tsks:
        tsk = t['task']
        df_result = tasks.result2table(tasks.get_result(tsk))
        results.append(df_result)
        print('.', end = '')
    df = pd.concat(results)

    # Приводим порядок столбцов в соответствие с условиями расчета
    df = df[slices+statistics]

    assert not df.empty


def test_timeband_big_tv(tasks):
    # Задаем период
    date_filter = [('2023-07-04', '2023-07-04')]

    # Задаем дни недели
    weekday_filter = None

    # Задаем тип дня
    daytype_filter = None

    # Задаем ЦА
    basedemo_filter = None

    # Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index
    targetdemo_filter = None

    # Задаем место просмотра
    location_filter=None

    # Задаем каналы
    company_filter = 'tvCompanyId in (1870)'

    # Фильтр типа плейбека
    playbacktype_filter = "playBackTypeName in ('Live')"

    # Фильтр платформы
    platform_filter = "platformName in ('Desktop')"

    # Указываем список срезов
    slices = [
        "researchDate",
        "tvCompanyName",
        "playBackTypeName",
        "platformName"
    ]
    # Указываем список статистик для расчета
    statistics = ['Reach000']

    # Задаем условия сортировки
    sortings = {'tvCompanyName': 'ASC'}

    # Задаем опции расчета
    options = {
        "kitId": 7,
        "bigTv": True
    }

    # Формируем задание для API TV Index в формате JSON
    task_json = tasks.build_timeband_task(
        date_filter=date_filter, weekday_filter=weekday_filter,
        daytype_filter=daytype_filter, company_filter=company_filter,
        location_filter=location_filter, basedemo_filter=basedemo_filter,
        targetdemo_filter=targetdemo_filter,
        playbacktype_filter=playbacktype_filter, platform_filter=platform_filter,
        slices=slices, statistics=statistics, sortings=sortings, options=options)

    # Отправляем задание на расчет и ждем выполнения
    task = tasks.wait_task(tasks.send_timeband_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task))

    # Приводим порядок столбцов в соответствие с условиями расчета
    df = df[slices+statistics]

    assert not df.empty


def test_respondent_analisys(tasks):
    # задаем параметры отчета
    date_filter = [('2022-07-12', '2022-07-12')]

    company_filter = 'tvCompanyId = 1858'

    slices = []

    statistics = ["ChannelDur"]

    custom_respondent_variable_id=None#'f6d1471a-a8dd-4c10-b445-61ea059b886b'

    options = {
        "kitId": 1, #TV Index Russia all
    }

    # Формируем из указанных параметров задание для API MediaVortex в формате JSON
    task_json = tasks.build_respondent_analysis_task(date_filter=date_filter, company_filter=company_filter,
                                                     slices=slices, statistics=statistics,
                                                     options=options, custom_respondent_variable_id=custom_respondent_variable_id)

    # Отправляем задание на расчет и ждем выполнения
    task_resp = tasks.wait_task(tasks.send_respondent_analysis_task(task_json))

    # Получаем результат
    df = tasks.result2table(tasks.get_result(task_resp))

    assert not df.empty
