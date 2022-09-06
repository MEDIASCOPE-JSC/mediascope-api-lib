import json
import pandas as pd
import numpy as np
import time
import datetime as dt
from pandas import DataFrame
from ..core import net
from ..core import sql
from . import catalogs
from ..core import errors
from ..core import tasks
from . import checks


class CrossWebTask:

    task_urls = {
        'media': '/task/media',
        'total': '/task/media-total',
        'ad': '/task/profile'
    }

    def __new__(cls, settings_filename=None, cache_path=None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CrossWebTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, settings_filename=None, cache_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.network_module = net.MediascopeApiNetwork(settings_filename, cache_path)
        self.task_builder = tasks.TaskBuilder()
        self.usetypes = self.get_usetype()
        self.cats = catalogs.CrossWebCats()
        self.units = self.cats.get_media_unit()

        self.media_attribs = self.cats.media_attribs[['sliceUnit', 'entityTitle', 'optionValue', 'optionName']].copy()
        self.media_attribs['optionValue'] = self.media_attribs['optionValue'].astype('int32')
        self.task_checker = checks.CrossWebTaskChecker(self.cats)

    def get_usetype(self):
        """
        Получить списки доступных для использования в заданиях:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        data = self.network_module.send_request_lo('get', '/dictionary/common/use-type', use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def build_task(self, task_type, task_name='', date_filter=None, usetype_filter=None, geo_filter=None,
                   demo_filter=None, mart_filter=None, slices=None, statistics=None, scales=None):
        """
        Формирует текст задания для расчета статистик

        Parameters
        ----------

        task_type : str
            Тип задания, возможные варианты:
            - media

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

        usetype_filter: list|None
            Список Типов пользования Интернетом
            Пример:
                usetype_filter = [1, 2, 3]

        geo_filter: list|None
            Условия фильтрации по географии
            Возможные варианты можно получить через метод `find_property` модуля catalogs:
            >>> cats.find_property('CityPop', expand=True)
            >>> cats.find_property('CityPop100', expand=True)
            >>> cats.find_property('FederalOkrug', expand=True)


        demo_filter: str|None
            Условия фильтрации по демографическим атрибутам
            Пример:
                demo_filter = "sex = 1 AND occupation = 1"

            Список допустимых атрибутов можно получить через метод `get_media_unit` модуля catalogs:
            >>> cats.get_media_unit()['filters']['demo']


        mart_filter: str|None
            Условия фильтрации по медиа-объектам
            Пример:
                mart_filter = "crossMediaResourceId = 1150 OR crossMediaResourceId = 1093"

            Список допустимых атрибутов можно получить через метод `get_media_unit` модуля catalogs:
            >>> cats.get_media_unit()['filters']['mart']


        slices: list
            Порядок разбивки результата расчета, задается в виде списка
            Пример:
                slices = ["useTypeName", "researchWeek", "crossMediaResourceId"]

            Список допустимых атрибутов можно получить через метод `get_media_unit` модуля catalogs:
            >>> cats.get_media_unit()['slices']

        statistics : list
            Список статистик, которые необходимо расчитать.
            Пример:
                statistics = ['reach', 'reachPer', 'dr']

            Список допустимых названий атрибутов можно получить через метод `get_media_unit` модуля catalogs:
            >>> cats.get_media_unit()['statistics']

        scales : dict|None
            Шкалы для статистик "drfd" и "reachN".
            Пример:
                scales = {
                            'drfd':[(1, 5), (10, 20)],
                            'reachN':[(2, 10), (20, 255)]
                        }

        Returns
        -------
        text : json
            Задание в формате CrossWeb API
        """

        if not self.task_checker.check_task(task_type, date_filter, usetype_filter, geo_filter,
                                            demo_filter, mart_filter, slices, statistics, scales):
            return

        # Собираем JSON
        tsk = {
            "task_type": task_type,
            "statistics": statistics,
            "filter": {}
        }
        # Добавляем фильтры
        self.task_builder.add_range_filter(tsk, date_filter)
        self.task_builder.add_usetype_filter(tsk, usetype_filter)
        self.task_builder.add_filter(tsk, geo_filter, 'geoFilter')
        self.task_builder.add_filter(tsk, demo_filter, 'demoFilter')
        self.task_builder.add_filter(tsk, mart_filter, 'martFilter')
        self.task_builder.add_slices(tsk, slices)
        self.task_builder.add_scales(tsk, scales)

        if not self.task_checker.check_units_in_task(task_type, tsk):
            return

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        tinfo = {
            'task_name': task_name,
            'task_type': task_type,
            'date_filter': date_filter,
            'usetype_filter': usetype_filter,
            'geo_filter': geo_filter,
            'demo_filter': demo_filter,
            'mart_filter': mart_filter,
            'slices': slices,
            'statistics': statistics,
            'scales': scales
        }
        self.task_builder.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)

    def _send_task(self, task_type, data):
        if data is None:
            return

        if task_type not in self.task_urls.keys():
            return

        try:
            return self.network_module.send_request('post', self.task_urls[task_type], data)
        except errors.HTTP400Error as e:
            print(f"Ошибка: {e}")

    def send_task(self, data):
        """
        Отправить задание на расчет

        Parameters
        ----------

        data : str
            Текст задания в JSON формате

        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        if data is None:
            print('Задание пустое')
            return
        task_type = json.loads(data)['task_type']
        if task_type not in self.task_urls.keys():
            print(f'Не верно указать тип задания, допустимые значения: {self.task_urls.keys().join(",")}')
            return
        return self._send_task(task_type, data)

    def send_audience_task(self, data):
        """
        Отправить задание на расчет аудиторных статистик по медиа

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('media', data)

    def send_total_audience_task(self, data):
        """
        Отправить задание на расчет аудиторных статистик по тотал-медиа

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('total', data)

    def send_advertisement_task(self, data):
        """
        Отправить задание на расчет аудиторных статистик по рекламе

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('ad', data)

    def wait_task(self, tsk):
        """
        Ожидает окончание расчета задания или заданий.

        Parameters
        ----------

        tsk : dict|list
            Задание в формате

                {
                    'taskId': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                    'userName': 'user.name',
                    'message': 'Задача поступила в обработку'
                }
            или список заданий
                 [
                    {
                        'taskId': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                        'userName': 'user.name',
                        'message': 'Задача поступила в обработку'
                    },
                    ...
                ]
        Returns
        -------
        tsk : dict|list
            Возвращает задание или список заданий
        """
        if type(tsk) == dict:
            if tsk.get('taskId') is not None:
                tid = tsk.get('taskId', None)
                task_state = ''
                task_state_obj = None
                cnt = 0
                while cnt < 5:
                    try:
                        time.sleep(3)
                        task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tid))
                    except errors.HTTP404Error:
                        cnt += 1
                        print(cnt)
                    except Exception:
                        raise Exception('Ошибка при получении статуса задания')
                    else:
                        break

                if task_state_obj is not None:
                    task_state = task_state_obj.get('taskStatus', '')

                print(f'Расчет задачи (id: {tsk["taskId"]}) [', end='')
                s = dt.datetime.now()
                # DONE, FAILED, IN_PROGRESS, CANCELLED, IN_QUEUE
                while task_state == 'IN_QUEUE' or task_state == 'IN_PROGRESS':
                    print('=', end=' ')
                    time.sleep(3)
                    task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tsk['taskId']))
                    if task_state_obj is not None:
                        task_state = task_state_obj.get('taskStatus', '')
                time.sleep(1)
                e = dt.datetime.now()
                print(f"] время расчета: {str(e - s)}")
                if task_state == 'DONE':
                    return tsk
        elif type(tsk) == list:
            task_list = list()
            # получим все идентификаторы заданий
            for t in tsk:
                cur_task = t.get('task')
                if cur_task is None or cur_task.get('taskId') is None:
                    continue
                task_list.append(t)
            # Проверим состояние заданий
            print(f'Расчет задач ({len(task_list)}) [ ', end='')
            s = dt.datetime.now()
            errs = dict()
            while True:
                time.sleep(3)
                # запросим состояние
                done_count = 0
                for t in task_list:
                    tid = t['task']['taskId']
                    task_state = ''
                    task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tid))
                    if task_state_obj is not None:
                        task_state = task_state_obj.get('taskStatus', '')

                    if task_state == 'IN_PROGRESS' or task_state == 'PENDING' or task_state == 'IN_QUEUE' or task_state == 'IDLE':
                        continue
                    elif task_state == 'DONE':
                        done_count += 1
                    else:
                        errs[tid] = t
                        errs[tid]['state'] = task_state
                        break
                print('=', end=' ')
                if done_count == len(tsk):
                    break

            if len(errs) > 0:
                print(f"Одна или несколько задач завершились с ошибкой")
                for tid, task_state in errs.items():
                    print(f"Задача: {tid} состояние: {task_state}")
                return None
            e = dt.datetime.now()
            print(f"] время расчета: {str(e - s)}")
            return tsk

    def get_status(self, tsk):
        """
        Получить статус расчета задания.

        Parameters
        ----------

        tsk : dict
            Задание в формате

                {
                    'taskId': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                    'userName': 'user.name',
                    'message': 'Задача поступила в обработку'
                }
        Returns
        -------
        tsk : dict
            Возвращает задание и его состояние:

                {
                    'taskId': 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
                    'userName': 'user.name',
                    'taskStatus': 'DONE',
                    'additionalParameters': {}
                }
        """
        if tsk.get('taskId') is not None:

            tid = tsk.get('taskId', None)
            task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tid))
            return task_state_obj

    def get_result(self, tsk):
        """
        Получить результат выполнения задания по его ID

        Parameters
        ----------

        tsk : dict
            Задание


        Returns
        -------
        text : json
            Результат выполнения задания в JSON формате

        """
        if tsk is None or tsk.get('taskId') is None:
            return None
        return self.network_module.send_request('get', '/task/result/{}'.format(tsk['taskId']))

    def result2table(self, data, project_name=None):
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
        self._get_text_names(df)
        df.replace(to_replace=[None], value=np.nan, inplace=True)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        # df['date'] = pd.to_datetime(df['date'])
        return df

    def _get_text_names(self, df, with_id=False):
        df = self._get_text_name_for(df, 'demo',  with_id)
        df = self._get_text_name_for(df, 'geo', with_id)
        df = self._get_text_name_for_mart(df)
        return df

    def _get_text_name_for(self, df: pd.DataFrame, entity_name: str, with_id=True):
        if type(df) != pd.DataFrame:
            return
        id_name = ''
        if with_id:
            id_name = 'Name'

        geo_attributes = self.cats.get_slices(entity_name)
        for col in df.columns:
            if col not in geo_attributes:
                continue
            # get cat
            _attrs = self.media_attribs[self.media_attribs['sliceUnit'] == col].copy()[['optionValue', 'optionName']]
            df[col] = df[col].astype('int32', errors='ignore')
            df[col + id_name] = df.merge(_attrs, how='left', left_on=col, right_on='optionValue')['optionName']
        return df

    def _get_text_name_for_mart(self, df: pd.DataFrame):
        if type(df) != pd.DataFrame:
            return
        matr_attributes = self.cats.get_slices('mart')
        pos = 0
        for col in df.columns:
            pos += 1
            if col not in matr_attributes:
                continue
            _attrs = pd.DataFrame()
            if col == 'crossMediaProductId':
                _attrs = self.cats.products
            elif col == 'crossMediaHoldingId':
                _attrs = self.cats.holdings
            elif col == 'crossMediaResourceId':
                _attrs = self.cats.resources
            elif col == 'crossMediaThemeId':
                _attrs = self.cats.themes
            else:
                break
            df[col] = df[col].astype('int64')
            df.insert(pos, col[:-2] + 'Name', df.merge(_attrs, how='left', left_on=col, right_on='id')['name'])
            pos += 1
        return df
