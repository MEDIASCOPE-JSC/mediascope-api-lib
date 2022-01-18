import json
import pandas as pd
import numpy as np
import time
import datetime as dt
from pandas import DataFrame
from ..core import net
from . import catalogs
from ..core import errors
from ..core import utils
from . import checks
from pyparsing import (
    Word,
    delimitedList,
    Group,
    alphas,
    alphanums,
    Forward,
    oneOf,
    quotedString,
    infixNotation,
    opAssoc,
    restOfLine,
    CaselessKeyword,
    ParserElement,
    pyparsing_common as ppc
)


class CrossWebTask:

    task_urls = {
        'audience': '/task/media',
        'total': '/task/media-total',
        'ad': '/task/advertisiment'
    }

    def __new__(cls, settings_filename=None, cache_path=None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CrossWebTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, settings_filename=None, cache_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rnet = net.MediascopeApiNetwork(settings_filename, cache_path)
        self.sql_parser = self._prepare_sql_parser()
        self.usetypes = self.get_usetype()
        self.cats = catalogs.CrossWebCats()
        self.units = self.cats.get_media_unit()
        self.task_info = dict()
        self.task_info['task'] = dict()
        self.media_attribs = self.cats.media_attribs[['sliceUnit', 'entityTitle', 'optionValue', 'optionName']].copy()
        self.media_attribs['optionValue'] = self.media_attribs['optionValue'].astype('int32')
        self.checks_module = checks.CrossWebChecker(self.cats)

    @staticmethod
    def _prepare_sql_parser():
        """
        Подготовка SQL-like парсера, для разбора условий в фильтрах

        Returns
        -------

        simple_sql : obj
            Объект класса отвечающего за парсинг
        """
        # define SQL tokens
        select_stmt = Forward()
        AND, OR, IN, NIN = map(
            CaselessKeyword, "and or in nin".split()
        )

        ident = Word(alphas, alphanums + "_$").setName("identifier")
        column_name = delimitedList(ident, ".", combine=True).setName("column name")
        column_name.addParseAction()

        binop = oneOf("= != > < <= >=", caseless=True)
        real_num = ppc.real()
        int_num = ppc.signed_integer()

        column_rval = (
                real_num | int_num | quotedString | column_name
        )  # need to add support for alg expressions
        where_condition = Group(
            (column_name + binop + column_rval)
            | (column_name + IN + Group("(" + delimitedList(column_rval) + ")"))
            | (column_name + IN + Group("(" + select_stmt + ")"))
        )

        where_expression = infixNotation(
            where_condition,
            [(AND, 2, opAssoc.LEFT), (OR, 2, opAssoc.LEFT), ],
        )

        # define the grammar
        select_stmt <<= where_expression
        simple_sql = select_stmt

        # define Oracle comment format, and ignore them
        oracle_sql_comment = "--" + restOfLine
        simple_sql.ignore(oracle_sql_comment)
        return simple_sql

    def _get_point(self, left_obj, logic_oper, right_obj):
        """
        Формирует объект - point понятный для API

        Parameters
        ----------

        left_obj : str
            Левая часть выражения
        logic_oper : str
            Логический оператор
        right_obj : obj
            Правая часть выражения


        Returns
        -------
        point : dict
            Объект - point понятный для API

        """
        # корректируем демо атрибуты: переводим названия в идентификаторы
        # if left_obj in self.demo_dict:
        #     left_obj = self.demo_dict[left_obj]['v']
        # проверяем логику
        point = {}
        if logic_oper == 'in':
            # ожидаем в правой части список атрибутов, бежим по нему
            if type(right_obj) == list:
                point = {"unit": left_obj, "relation": "IN", "value": []}
                for robj in right_obj:
                    if type(robj) == str and (robj == '(' or robj == ')'):
                        # пропускаем скобки, объекты и так лежат в отдельном списке
                        continue
                    # формируем условие в json формате
                    point['value'].append(robj)
        elif logic_oper == '!=':
            point = {"unit": left_obj, "relation": "NEQ", "value": right_obj}
        elif logic_oper == '>':
            point = {"unit": left_obj, "relation": "GT", "value": right_obj}
        elif logic_oper == '<':
            point = {"unit": left_obj, "relation": "LT", "value": right_obj}
        elif logic_oper == '>=':
            point = {"unit": left_obj, "relation": "GTE", "value": right_obj}
        elif logic_oper == '<=':
            point = {"unit": left_obj, "relation": "LTE", "value": right_obj}
        else:
            point = {"unit": left_obj, "relation": "EQ", "value": right_obj}

        return point

    def _find_points(self, obj):
        """
        Ищет в исходном объекте, объкты типа point и преобразует их в формат API
        """
        if type(obj) == list:
            if len(obj) == 3 and type(obj[0]) == str and obj[1] in ['=', '!=', 'in', 'nin', ">", "<", ">=", "<="]:
                return self._get_point(obj[0], obj[1], obj[2])
        i = 0
        while i < len(obj):
            obj_item = obj[i]
            if type(obj_item) == list:
                obj[i] = self._find_points(obj_item)
            i += 1
        return obj

    def _parse_expr(self, obj):
        """
        Преобразует выражение для фильтрации из набора вложенных списков в формат API

        Parameters
        ----------

        obj : dict | list
            Объект с условиями фильтрации в виде набора вложенных списков, полученный после работы SQL парсера


        Returns
        -------
        jdat : dict
            Условия фильтрации в формате API
        """
        if type(obj) == list:
            jdat = {}
            for obj_item in obj:
                if type(obj_item) == list:
                    ret_data = self._parse_expr(obj_item)
                    if jdat.get('children') is None:
                        jdat['children'] = [ret_data]
                    else:
                        jdat['children'].append(ret_data)
                elif type(obj_item) == dict:  # and 'point' in obj_item.keys():
                    if obj_item.get('elements') is None:
                        if jdat.get('elements') is None:
                            jdat['elements'] = []
                        jdat['elements'].append(obj_item)
                    else:
                        if jdat.get('children') is None:
                            jdat['children'] = []
                        jdat['children'].append(obj_item)
                elif type(obj_item) == str and obj_item in ['or', 'and']:
                    jdat["operand"] = obj_item.upper()
            return jdat
        elif type(obj) == dict:
            jdat = {'elements': []}
            jdat['elements'].append(obj)
            jdat["operand"] = 'OR'
            return jdat

    def _sql_to_json(self, sql_text):
        """
        Преобразует условие фильтрации записанное в SQL натации, в формат API

        Parameters
        ----------

        sql_text : str
            Текст условия в SQL формате


        Returns
        -------
        obj : dict
            Условия фильтрации в формате API

        """

        sql_obj = self.sql_parser.parseString(sql_text)

        #sql_obj.pprint()
        s = sql_obj.asList()[0]
        prep_points = self._find_points(s)
        return self._parse_expr(prep_points)

    @staticmethod
    def _get_sql_from_list(obj_name, obj_data, oper):
        result_text = ''
        if obj_data is not None:
            if type(obj_data) == list:
                if len(obj_data) > 1:
                    result_text = f"{obj_name} {oper} ({','.join(str(x) for x in obj_data)})"
                elif len(obj_data) == 1:
                    result_text = f"{obj_name} = { obj_data[0]}"
            elif type(obj_data) == str:
                result_text = f"{obj_name} = { obj_data}"
        return result_text

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
        data = self.rnet.send_request_lo('get', '/dictionary/common/use-type', use_cache=True)
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

    def _check_units_in_task(self, statistics, slices, filters):
        if type(statistics) == list:
            # self.units
            return None

    def _add_filter(self, tsk, filter_obj, filter_name):
        if filter_obj is not None:
            if type(filter_obj) == dict:
                tsk['filter'][filter_name] = filter
            elif type(filter_obj) == str:
                tsk['filter'][filter_name] = self._sql_to_json(filter_obj)
        return tsk

    def _add_range_filter(self, tsk, date_filter):
        # Добавляем фильтр по диапазонам
        if date_filter is not None and type(date_filter) == list and len(date_filter) > 0:
            date_ranges = {
                "operand": "OR",
                "children": []
            }
            for dr in date_filter:
                date_ranges['children'].append({
                    "operand": "AND",
                    "elements": [
                        {
                            "unit": "researchDate",
                            "relation": "GTE",
                            "value": dr[0]
                        },
                        {
                            "unit": "researchDate",
                            "relation": "LTE",
                            "value": dr[1]
                        }
                    ]
                })
            tsk['filter']['dateFilter'] = date_ranges

    @staticmethod
    def _add_usetype_filter(tsk, usetype_filter):
        # Добавляем фильтр по usetype
        if usetype_filter is not None and type(usetype_filter) == list and len(usetype_filter) > 0:
            usetype = {"operand": "OR", "elements": [{
                "unit": "useTypeId",
                "relation": "IN",
                "value": usetype_filter
            }]}
            tsk['filter']['useTypeFilter'] = usetype

    @staticmethod
    def _add_scales(tsk, scales):
        # Добавляем шкалы
        if scales is not None:
            scales_json = {}
            for scale, val in scales.items():
                scales_json[scale] = []
                for v in val:
                    scales_json[scale].append({"from": v[0], "to": v[1]})
            tsk['scales'] = scales_json

    @staticmethod
    def _add_slices(tsk, slices):
        # Добавляем срезы
        if slices is not None:
            tsk['slices'] = slices

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

        if not self.checks_module.check_task(task_type, date_filter, usetype_filter, geo_filter,
                                             demo_filter, mart_filter, slices, statistics, scales):
            return

        # Собираем JSON
        tsk = {
            "statistics": statistics,
            "filter": {}
        }
        # Добавляем фильтры
        self._add_range_filter(tsk, date_filter)
        self._add_usetype_filter(tsk, usetype_filter)
        self._add_filter(tsk, geo_filter, 'geoFilter')
        self._add_filter(tsk, demo_filter, 'demoFilter')
        self._add_filter(tsk, mart_filter, 'martFilter')
        self._add_slices(tsk, slices)
        self._add_scales(tsk, scales)

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        tinfo = {
            'task_name': task_name,
            'date_filter': date_filter,
            'usetype_filter': usetype_filter,
            'geo_filter': geo_filter,
            'demo_filter': demo_filter,
            'mart_filter': mart_filter,
            'slices': slices,
            'statistics': statistics,
            'scales': scales
        }
        self.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)

    def send_task(self, task_type, data):
        """
        Отправить задание на расчет

        Parameters
        ----------

        task_type: str
            Тип задания
                - audience - задание на расчет аудитории по media
                - total - задание на расчет аудитори по total-media
                - ad - задание на расчет аудитории по рекламе

        data : str
            Текст задания в JSON формате

        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        if data is None:
            return
        if task_type not in self.task_urls.keys():
            return

        try:
            return self.rnet.send_request('post', self.task_urls[task_type], data)
        except errors.HTTP400Error as e:
            print(f"Ошибка: {e}")

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
        return self.send_task('audience', data)

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
        return self.send_task('total', data)

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
        return self.send_task('ad', data)

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
                tstate = ''
                tstate_obj = None
                cnt = 0
                while cnt < 5:
                    try:
                        time.sleep(3)
                        tstate_obj = self.rnet.send_request('get', '/task/state/{}'.format(tid))
                    except errors.HTTP404Error:
                        cnt += 1
                        print(cnt)
                    except Exception:
                        raise Exception('Ошибка при получении статуса задания')
                    else:
                        break

                if tstate_obj is not None:
                    tstate = tstate_obj.get('taskStatus', '')

                print(f'Расчет задачи (id: {tsk["taskId"]}) [', end='')
                s = dt.datetime.now()
                # DONE, FAILED, IN_PROGRESS, CANCELLED, IN_QUEUE
                while tstate == 'IN_QUEUE' or tstate == 'IN_PROGRESS':
                    print('=', end=' ')
                    time.sleep(3)
                    tstate_obj = self.rnet.send_request('get', '/task/state/{}'.format(tsk['taskId']))
                    if tstate_obj is not None:
                        tstate = tstate_obj.get('taskStatus', '')
                time.sleep(1)
                e = dt.datetime.now()
                print(f"] время расчета: {str(e - s)}")
                if tstate == 'DONE':
                    return tsk
        elif type(tsk) == list:
            tasks = list()
            # получим все идентификаторы заданий
            for t in tsk:
                cur_task = t.get('task')
                if cur_task is None or cur_task.get('taskId') is None:
                    continue
                tasks.append(t)
            # Проверим состояние заданий
            print(f'Расчет задач ({len(tasks)}) [ ', end='')
            s = dt.datetime.now()
            errs = dict()
            while True:
                time.sleep(3)
                # запросим состояние
                done_count = 0
                for t in tasks:
                    tid = t['task']['taskId']
                    tstate = ''
                    tstate_obj = self.rnet.send_request('get', '/task/state/{}'.format(tid))
                    if tstate_obj is not None:
                        tstate = tstate_obj.get('taskStatus', '')

                    if tstate == 'IN_PROGRESS' or tstate == 'PENDING' or tstate == 'IN_QUEUE' or tstate == 'IDLE':
                        continue
                    elif tstate == 'DONE':
                        done_count += 1
                    else:
                        errs[tid] = t
                        errs[tid]['state'] = tstate
                        break
                print('=', end=' ')
                if done_count == len(tsk):
                    break

            if len(errs) > 0:
                print(f"Одна или несколько задач завершились с ошибкой")
                for tid, tstate in errs.items():
                    print(f"Задача: {tid} состояние: {tstate}")
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
            tstate_obj = self.rnet.send_request('get', '/task/state/{}'.format(tid))
            return tstate_obj

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
        return self.rnet.send_request('get', '/task/result/{}'.format(tsk['taskId']))

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
        #df['date'] = pd.to_datetime(df['date'])
        return df

    @staticmethod
    def get_excel_filename(task_name, export_path='../excel', add_dates=True):
        """
        Получить имя excel файла

        Parameters
        ----------

        task_name : str
            Название задания

        export_path : str
            Путь к папке с excel файлами

        add_dates : bool
            Флаг - добавлять в имя файла дату или нет, по умолчанию = True

        Returns
        -------
        filename : str
            Путь и имя excel файла
        """
        return utils.get_excel_filename(task_name, export_path, add_dates)

    def save_report_info(self, tinfo):
        """
        Сохраняет общую информацию о заданиях. Использует при сохранении отчета в Excel

        Parameters
        ----------
        tinfo : dict
            Параметры задания в виде словаря
        """
        self.task_info['task'] = tinfo

    def get_report_info(self):
        """
        Возвращает информацию о расчитываемом отчете в виде DataFrame, которая была предварительно сохранена
        с помощью метода save_audience_info

        Returns
        -------
        result: DataFrame
            Информация о расчитываемом отчете
        """
        data = list()
        for tk, tv in self.task_info['task'].items():
            data.append(f"{tk}: {tv}")
        return pd.DataFrame(data)

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
