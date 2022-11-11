import json
import pandas as pd
import numpy as np
from pandas import DataFrame
from ..core import net
from ..core import tasks
from ..core import errors


class CounterTask:
    def __new__(cls, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.task_builder = tasks.TaskBuilder()

    def build_task(self, task_name: str = '', date_filter: list = None, area_type_filter: list = None,
                   partner_filter: list = None, tmsec_filter: list = None, geo_filter: str = None,
                   device_type_filter: list = None, slices: list = None, statistics: list = None,
                   sampling: int = 42) -> dict:
        """
        Формирует текст задания для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : list
            Список периодов, период задается списком пар - (начало, конец):
            Пример:
                date_filter = [
                               ('2021-07-05', '2021-07-18'),
                               ('2021-09-06', '2021-09-26'),
                               ('2021-10-18', '2021-10-31')
                              ]

        area_type_filter : list
            Тип размещения счетчика, определяет область обрабатываемых данных и позволяет сократить время расчета:
                audience - В отбор попадают данные по Интернет площадкам
                advertisingCampaign - тегированные рекламные кампании
                advertisingNetwork - рекламные сети

            Пример:
                area_type_filter = ["advertisingCampaign", "advertisingNetwork"]
                area_type_filter = "audience"

        partner_filter : str|None
            Фильтр по имени партнера (он же account_name)
            Пример:
                partner_filter = ["anysite_ru"]
                partner_filter = ["newsite_com", "oldsite_net"]


        tmsec_filter : str|None
            Фильтр по tmsec
            Пример:
                tmsec_filter = ["newsite_total"]
                tmsec_filter ["newsite_total", "oldsite_news"]


        geo_filter: str|None
            Условия фильтрации по географии
            Пример:
                geo_filter = "countryName ='RU'
                geo_filter = "provinceName = 'Москва'"
                geo_filter = "cityName IN ('Москва', 'Тверь', 'Санкт-Петербург')"

        device_type_filter: list|None
            Условия фильтрации по типам устройств
            Пример:
                device_type_filter = ['DESKTOP']
                device_type_filter = ['DESKTOP', 'SMARTPHONE']

        slices: list
            Порядок разбивки результата расчета, задается в виде списка,
            возможные значения:
            - "researchDate" - дата
            - "countryName" - страна
            - "provinceName" - регион
            - "cityName" - город
            - "partnerName" - партнер (account_name)
            - "tmsec" - tmsec (тематический раздел, идентификатор РК, ...)
            - "deviceTypeName" - тип устройства
            - "areaType" - тип размещени счетчика
            Пример:
                slices = ["researchDate", "tmsec"]

        statistics : list
            Список статистик, которые необходимо рассчитать,
            возможные значения:
            - hitsVisits - хиты
            - uniqsVisits - уникальные посетители
            Пример:
                statistics = ['hitsVisits', 'uniqsVisits']
                statistics = ['hitsVisits']

        sampling : int|None
            Сэмпл данных используемых для расчета.
            Задается в процентах от 1 до 100.
            Значение по умолчанию = 42 (Answer to the Ultimate Question of Life, the Universe, and Everything).
            Сэмплирование позволяет выполнять запросы приближённо на основе указанного процента данных.
            К примеру, если указать значение 10, то выполнится фильтрация, после чего возьмётся 10% отсортированных
            данных и на их основе посчитается статистика.
        Returns
        -------
        text : json
            Результат расчета
        """

        # if not self.checks_module.check_task(task_type, date_filter, usetype_filter, geo_filter,
        #                                      demo_filter, mart_filter, slices, statistics, scales):
        #     return

        # Собираем JSON
        tsk = {
            "statistics": statistics,
            "filter": {}
        }
        # Добавляем фильтры
        self.task_builder.add_range_filter(tsk, date_filter)
        self.task_builder.add_list_filter(tsk, 'areaTypeFilter', 'areaType', area_type_filter)
        self.task_builder.add_list_filter(tsk, 'partnerNameFilter', 'partnerName', partner_filter)
        self.task_builder.add_list_filter(tsk, 'tmsecFilter', 'tmsec', tmsec_filter)
        self.task_builder.add_filter(tsk, device_type_filter, 'deviceTypeFilter')
        self.task_builder.add_filter(tsk, geo_filter, 'geoFilter')

        self.task_builder.add_slices(tsk, slices)
        self.task_builder.add_sampling(tsk, sampling)

        # if not self.checks_module.check_units_in_task(task_type, tsk):
        #     return

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        tinfo = {
            'task_name': task_name,
            'date_filter': date_filter,
            'area_type_filter': area_type_filter,
            'partner_filter': partner_filter,
            'tmsec_filter': tmsec_filter,
            'geo_filter': geo_filter,
            'device_type_filter': device_type_filter,
            'slices': slices,
            'statistics': statistics,
            'sampling': sampling
        }
        self.task_builder.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)
        #return tsk

    def send_task(self, task: dict):
        """
        Отправить задание на расчет

        Parameters
        ----------

        task : str
            Текст задания в JSON формате

        Returns
        -------
        text : json
            Ответ сервера с результатом расчета

        """
        if task is None:
            print('Задание пустое')
            return None
        try:
            return self.msapi_network.send_request('post', '/daily-task', task)
        except errors.HTTP400Error as e:
            print(f"Ошибка: {e}")

    @staticmethod
    def result2table(data: dict, project_name: str = None):
        """
        Получить результат выполнения задания по его ID

        Parameters
        ----------

        data : dict
            Результат выполнения задания в JSON формате

        project_name : str
            Название проекта

        Returns
        -------
        result : DataFrame
            DataFrame с результатом выполнения задания
        """
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'taskId' not in data or 'resultBody' not in data:
            return None

        if type(data['resultBody']) == list and len(data['resultBody']) == 0:
            msg = data.get('message', None)
            if msg is not None:
                print(msg)

        slices = set()
        statistics = set()

        for item in data['resultBody']:
            stat = item['statistics']
            sls = item['slice']
            for k in sls.keys():
                if k not in slices:
                    slices.add(k)
                    res[k] = []

            for k in stat.keys():
                if k not in statistics:
                    statistics.add(k)
                    res['stat.' + k] = []

        for item in data['resultBody']:
            stat = item['statistics']
            sls = item['slice']
            for k in slices:
                if k in sls:
                    v = str(sls[k])
                else:
                    v = '-'
                res[k].append(v)

            for k in statistics:
                if k in stat:
                    v = stat[k]
                else:
                    v = None
                res['stat.' + k].append(v)

        df = pd.DataFrame(res)
        # self._get_text_names(df)
        df.replace(to_replace=[None], value=np.nan, inplace=True)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        return df
