import os
import json
import pandas as pd
from ..core import net
from . import catalogs
import time
import datetime as dt
import hashlib
import re

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


class ResponsumTask:
    STRUCT_NAMES = ["holding", "site", "section", "subsection", "network", "network_section", "network_subsection",
                    "ad_agency", "brand", "position", "subbrand"]
    AUDIENCE_STAT = ["ADF", "ADO", "ADR", "ADRPer", "Affinity", "AffinityIn", "AMF", "AMO", "AMR", "AMRPer", "AvAge",
                     "AWDR", "AWF", "AWO", "AWR", "AWRPer", "DR", "DRFD", "ExclUseOTSN", "ExclUseReachN",
                     "ExclusiveReach", "ExclusiveOts", "Frequency", "GRP", "OTS", "OTSN", "Reach", "ReachN",
                     "ReachPer", "Smp", "Uni", "UnwReach"]
    DUPLICATION_STAT = ["Reach", "ReachPer", "ADR", "ADRPer", "AWR", "AWRPer", "AMR", "AMRPer", "UnwReach", "OTS", "DR",
                        "Uni", "Smp"]
    DURATION_STAT = ["ATT", "ADDperU", "ADDperP", "ADDperUTotal", "ADDperPTotal", "DATT"]
    USETYPES_DICT = {1: "desktop", 2: "mobile-web", 3: "mobile-app-online", 4: "mobile-app-offline", 34: "mobile-app"}

    def __new__(cls, facility_id, settings_filename=None, cache_path=None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ResponsumTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, facility_id, settings_filename=None, cache_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ParserElement.enablePackrat()
        self.rnet = net.MediascopeApiNetwork(settings_filename, cache_path)
        self.rcats = catalogs.ResponsumCats(facility_id, settings_filename, cache_path)
        self.demo_attr = self.rcats.get_demo()
        self.demo_dict = self.rcats.get_demo_dict(self.demo_attr)
        self.sql_parser = self._prepare_sql_parser()
        self.task_info = dict()
        self.task_info['tasks'] = dict()

    @staticmethod
    def _prepare_sql_parser():
        """
        Подготовка SQL-like парсера для разбора условий в фильтрах.
        
        Returns
        -------
        
        simple_sql : obj
            Объект класса, отвечающего за парсинг.
        """
        # define SQL tokens
        select_stmt = Forward()
        AND, OR, IN, NOTIN = map(
            CaselessKeyword, "and or in notin".split()
        )

        ident = Word(alphas, alphanums + "_$").setName("identifier")
        column_name = delimitedList(ident, ".", combine=True).setName("column name")
        column_name.addParseAction(ppc.downcaseTokens)

        binop = oneOf("= != > <", caseless=True)
        real_num = ppc.real()
        int_num = ppc.signed_integer()

        column_rval = (
                real_num | int_num | quotedString | column_name
        )  # need to add support for alg expressions
        where_condition = Group(
            (column_name + binop + column_rval)
            | (column_name + IN + Group("(" + delimitedList(column_rval) + ")"))
            | (column_name + IN + Group("(" + select_stmt + ")"))
            | (column_name + NOTIN + Group("(" + delimitedList(column_rval) + ")"))
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
        Формирует объект - point, понятный для API Responsum.
        
        Parameters
        ----------
        
        left_obj : str
            Левая часть выражения.
        logic_oper : str
            Логический оператор.
        right_obj : obj
            Правая часть выражения.
        
        
        Returns
        -------
        point : dict
            Объект - point, понятный для API Responsum.
        
        """
        # корректируем демо атрибуты: переводим названия в идентификаторы
        if left_obj in self.demo_dict:
            left_obj = self.demo_dict[left_obj]['v']
        # проверяем логику
        point = {}
        if logic_oper == 'in' or logic_oper == 'notin':
            # ожидаем в правой части список атрибутов, бежим по нему
            if type(right_obj) == list:
                point = {"children": []}
                for robj in right_obj:
                    if type(robj) == str and (robj == '(' or robj == ')'):
                        # пропускаем скобки, объекты и так лежат в отдельном списке
                        continue
                    # формируем условие в json формате
                    p = {"point": {"type": left_obj, "val": robj}, "operator": "EQUAL", "isNot": False}
                    point['children'].append(p)
                point["logic"] = "OR"
                point["isNot"] = False

        elif logic_oper == '!=':
            point = {"point": {"type": left_obj, "val": right_obj}, "operator": "EQUAL", "isNot": True}
        elif logic_oper == '>':
            point = {"point": {"type": left_obj, "val": right_obj}, "operator": "ABOVE", "isNot": False}
        elif logic_oper == '<':
            point = {"point": {"type": left_obj, "val": right_obj}, "operator": "LESS", "isNot": False}
        else:
            point = {"point": {"type": left_obj, "val": right_obj}, "operator": "EQUAL", "isNot": False}

        return point

    def _find_points(self, obj):
        """
        Ищет в исходном объекте объекты типа point и преобразует их в формат Responsum API
        """
        if type(obj) == list:
            if len(obj) == 3 and type(obj[0]) == str and obj[1] in ['=', '!=', '>', '<', 'in', 'notin']:
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
        Преобразует выражение для фильтрации из набора вложенных списков в формат Responsum API.
        
        Parameters
        ----------
        
        obj : dict | list
            Объект с условиями фильтрации в виде набора вложенных списков, полученный после работы SQL парсера.
            
        
        Returns
        -------
        jdat : dict
            Условия фильтрации в формате Responsum API.
        """
        if type(obj) == list:
            jdat = {'children': []}
            for obj_item in obj:
                if type(obj_item) == list:
                    ret_data = self._parse_expr(obj_item)
                    jdat['children'].append(ret_data)
                elif type(obj_item) == dict:  # and 'point' in obj_item.keys():
                    jdat['children'].append(obj_item)
                elif type(obj_item) == str and obj_item in ['or', 'and']:
                    jdat["logic"] = obj_item.upper()
                    jdat["isNot"] = False
            return jdat
        elif type(obj) == dict:
            jdat = {'children': []}
            jdat['children'].append(obj)
            jdat["logic"] = 'OR'
            jdat["isNot"] = False
            return jdat

    def _sql_to_json(self, sql_text):
        """
        Преобразует условие фильтрации, записанное в SQL нотации, в формат Responsum API.
        
        Parameters
        ----------
        
        sql_text : str
            Текст условия в SQL формате.
            
        
        Returns
        -------
        obj : dict
            Условия фильтрации в формате Responsum API.
            
        """

        sql_obj = self.sql_parser.parseString(sql_text)
        # sql_obj.pprint()
        s = sql_obj.asList()[0]
        prep_points = self._find_points(s)
        return self._parse_expr(prep_points)

    @staticmethod
    def get_sql_from_list(obj_name, obj_data):
        result_text = ''
        if obj_data is not None:
            if type(obj_data) == list:
                if len(obj_data) > 1:
                    result_text = f"{obj_name} in ({','.join(str(x) for x in obj_data)})"
                elif len(obj_data) == 1:
                    result_text = f"{obj_name} = { obj_data[0]}"
            elif type(obj_data) == str:
                result_text = f"{obj_name} = { obj_data}"
        return result_text

    def _prepare_task(self, task_type, task_name='', facility=None, date_from=None, date_to=None, usetype_filter=None,
                      population_filter=None, ages_filter=None, media_filter=None, demo_filter=None,
                      statistics=None, structure=None, reach_n=None, excl_use=None, dup_media_filter=None,
                      is_duration=False):
        structure = self._convert_demo_structure(structure)
        # Собираем JSON
        tsk = {
            'header': {
                'name': task_name,
                'facility': facility
            },
            'filters': {
                'date': {
                    'from': date_from,
                    'to': date_to
                }
            },
            'statistics': {'names': statistics},
            'structure': structure
        }
        # формируем scale
        if reach_n is not None or excl_use is not None:
            scales = {}
            if reach_n is not None:
                scales['ReachN'] = reach_n
            if excl_use is not None:
                scales['ExclUse'] = excl_use
            tsk['statistics']['scales'] = scales

        #  Добавляем фильтр по usetype
        if usetype_filter is not None and type(usetype_filter) == list:
            if is_duration:
                if 3 in usetype_filter and 4 not in usetype_filter:
                    usetype_filter.append(4)
                elif 4 in usetype_filter and 3 not in usetype_filter:
                    usetype_filter.append(3)

            usetype_sql = 'usetype_id in (' + ','.join(str(x) for x in usetype_filter) + ')'
            if media_filter is not None:
                media_filter = '(' + media_filter + ') AND ' + usetype_sql
            else:
                media_filter = usetype_sql
            if task_type == 'duplication':
                if dup_media_filter is not None:
                    dup_media_filter = '(' + dup_media_filter + ') AND ' + usetype_sql
                else:
                    dup_media_filter = usetype_sql

        # Добавляем фильтр по населению
        if population_filter is not None:
            population_sql = 'CITY_TYPE2 in (' + ','.join(str(x) for x in population_filter) + ')'
            if demo_filter is not None:
                demo_filter = population_sql + ' AND ' + demo_filter
            else:
                demo_filter = population_sql

        # Добавляем фильтр по возрастным группам
        if ages_filter is not None:
            ages_sql = 'AGE_GROUPS in (' + ','.join(str(x) for x in ages_filter) + ')'
            if demo_filter is not None:
                demo_filter = ages_sql + ' AND ' + demo_filter
            else:
                demo_filter = ages_sql

        # Формируем фильтры
        if media_filter is not None:
            media_sql = self._sql_to_json(media_filter)
            tsk['filters']['media'] = media_sql

        if task_type == 'duplication':
            if dup_media_filter is not None:
                dup_media_sql = self._sql_to_json(dup_media_filter)
                tsk['filters']['duplicationMedia'] = dup_media_sql

        if demo_filter is not None:
            demo_sql = self._sql_to_json(demo_filter)
            tsk['filters']['demo'] = demo_sql

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        self._save_task_info(task_name, facility, date_from, date_to, usetype_filter,
                             population_filter, ages_filter, media_filter, demo_filter,
                             statistics, structure)
        return json.dumps(tsk)

    def build_audience_task(self, task_name='', facility=None, date_from=None, date_to=None, usetype_filter=None,
                            population_filter=None, ages_filter=None, media_filter=None, demo_filter=None,
                            statistics=None, structure=None, reach_n=None, excl_use=None):
        """
        Формирует текст задания для расчета аудиторных статистик.

        Parameters
        ----------

        task_name : str
            Название задания, если не задано, то формируется как "пользователь + тип задания + дата/время".

        facility : str
            Установка : "desktop", "mobile", "desktop_pre".

        date_from : str
            Начало периода для расчета, дата в формате YYYY-MM-DD.

        date_to : str
            Конец периода для расчета, дата в формате YYYY-MM-DD.

        usetype_filter: list|None
            Список типов пользования Интернетом.

        media_filter: str|None
            Условия фильтрации по медиа-объектам.

        demo_filter: str|None
            Условия фильтрации по демографическим атрибутам.

        statistics : list
            Список статистик, которые необходимо рассчитать.
            Например: ["UnwReach", "Reach", "OTS"]

        structure: dict
            Порядок группировки результата расчета, задается в виде словаря.
            Пример:
                {
                    "date": "day",
                    "media": ["site"],
                    "usetype": False
                }
            Варианты группировок для каждой из осей:
                * date - задается строкой, допустимые варианты:
                    "day",
                    "week",
                    "month",
                    "weekDay"

                * media - задается списком, допустимые варианты:
                    [
                        "holding",
                        "site",
                        "section",
                        "subsection",
                        "network",
                        "network_section",
                        "network_subsection",
                        "ad_agency",
                        "brand",
                        "position",
                        "subbrand"
                    ]

                * demo - задается в виде списка демографических переменных, например:
                    [ "AGE", "SEX", "CITY", ...]

                * usetype - задается булевым значением: True/False.

        reach_n: list
            Шкала для ReachN+Distribution.
            Для расчета необходимо прописать переменную reach_n с требуемыми параметрами в виде списка со словарем.
            Пример:
            reach_n = [ {"from": 0, "to": 2},
                        {"from": 3, "to": 4},
                        {"from": 5, "to": 255}
                      ]
            Также необходимо указать заданную переменную при отправке задания.
            Пример:
            build_audience_task(self, ..., reach_n=reach_n)
            
        excl_use: list
            Шкала для ExclUse, ее значения соответствуют значениям use type.
            Для расчета необходимо прописать переменную excl_use с требуемыми параметрами в виде списка со словарем.
            Пример:
            excl_use = [ {"from": 1, "to": 1},
                         {"from": 2, "to": 2},
                         {"from": 3, "to": 3},
                         {"from": 4, "to": 4}                  
                       ] 
            Также необходимо указать заданную переменную при отправке задания.
            Пример:
            build_audience_task(self, ..., excl_use=excl_use)
            
        Returns
        -------
        text : json
            Задание в формате Responsum API.
        """
        error_text = self._check_task_params(date_from, date_to, facility, statistics, structure, self.AUDIENCE_STAT)
        if 'ReachN' in statistics:
            if reach_n is None or type(reach_n) != list or len(reach_n) == 0:
                error_text += 'Для статистики ReachN не задана шкала (переменная reach_n) или она пустая.\n'
        if 'ExclUseOTSN' in statistics or 'ExclUseReachN' in statistics:
            if excl_use is None or type(excl_use) != list or len(excl_use) == 0:
                error_text += 'Для статистик ExclUseOTSN/ExclUseReachN не задана шкала (переменная excl_use) или ' \
                              'она пустая.\n'

        if len(error_text) > 0:
            print('Ошибка при формировании задания')
            print(error_text)
            return None

        return self._prepare_task(task_type='audience', task_name=task_name, facility=facility,
                                  date_from=date_from, date_to=date_to, usetype_filter=usetype_filter,
                                  population_filter=population_filter, ages_filter=ages_filter,
                                  media_filter=media_filter, demo_filter=demo_filter,
                                  statistics=statistics, structure=structure, reach_n=reach_n, excl_use=excl_use,
                                  dup_media_filter=None,  is_duration=False)

    def build_duplication_task(self, task_name='', facility=None, date_from=None, date_to=None, usetype_filter=None,
                               population_filter=None, ages_filter=None, media_filter=None, dup_media_filter=None,
                               demo_filter=None, statistics=None, structure=None):
        """
        Формирует текст задания типа duplication для расчета пересечения аудиторий.

        Parameters
        ----------

        task_name : str
            Название задания, если не задано, то формируется как "пользователь + тип задания + дата/время".

        facility : str
            Установка : "desktop", "mobile", "desktop_pre".

        date_from : str
            Начало периода для расчета, дата в формате YYYY-MM-DD.

        date_to : str
            Конец периода для расчета, дата в формате YYYY-MM-DD.

        usetype_filter: list|None
            Список типов пользования Интернетом.

        media_filter: str
            Условия фильтрации по медиа-объектам.

        dup_media_filter: str
            Условия фильтрации по медиа-объектам для оси duplication.

        demo_filter: str|None
            Условия фильтрации по демографическим атрибутам.

        statistics : list
            Список статистик, которые необходимо рассчитать.
            Например: ["UnwReach", "Reach", "OTS"]

        structure: dict
            Порядок группировки результата расчета, задается в виде словаря.
            Пример:
                {
                    "date": "day",
                    "media": ["site"],
                    "usetype": False
                }


        Returns
        -------
        text : json
            Задание в формате Responsum API.
        """
        if task_name is None or task_name == '':
            # make task name by user and datetime
            task_name = 'test'
        error_text = self._check_task_params(date_from, date_to, facility, statistics, structure, self.DUPLICATION_STAT)
        if dup_media_filter is None or media_filter is None:
            error_text += 'не заданы медиа-объекты для построения пересечения.\n'
        if len(error_text) > 0:
            print('Ошибка при формировании задания')
            print(error_text)
            return

        return self._prepare_task(task_type='duplication', task_name=task_name, facility=facility, date_from=date_from,
                                  date_to=date_to, usetype_filter=usetype_filter, population_filter=population_filter,
                                  ages_filter=ages_filter, media_filter=media_filter, demo_filter=demo_filter,
                                  statistics=statistics, structure=structure, reach_n=None, excl_use=None,
                                  dup_media_filter=dup_media_filter,  is_duration=False)

    def _build_duration_task(self, task_name='', facility=None, date_from=None, date_to=None, usetype_filter=None,
                            population_filter=None, ages_filter=None, media_filter=None, demo_filter=None,
                            statistics=None, structure=None):
        """
        Формирует текст задания для расчета длительностей.

        Parameters
        ----------

        task_name : str
            Название задания, если не задано, то формируется как "пользователь + тип задания + дата/время".

        facility : str
            Установка : "desktop", "mobile", "desktop_pre".

        date_from : str
            Начало периода для расчета, дата в формате YYYY-MM-DD.

        date_to : str
            Конец периода для расчета, дата в формате YYYY-MM-DD.

        usetype_filter: list|None
            Список типов пользования Интернетом.

        media_filter: str|None
            Условия фильтрации по медиа-объектам.

        demo_filter: str|None
            Условия фильтрации по демографическим атрибутам.

        statistics : list
            Список статистик, которые необходимо рассчитать.
            Например: ["ADDperU"]

        structure: dict
            Порядок группировки результата расчета, задается в виде словаря.
            Пример:
                {
                    "date": "month",
                    "media": ["site"],
                    "demo": [],
                    "usetype": False
                }

        Returns
        -------
        text : json
            Задание в формате Responsum API.
        """

        if task_name is None or task_name == '':
            # make task name by user and datetime
            task_name = 'test'
        error_text = self._check_task_params(date_from, date_to, facility, statistics, structure, self.DURATION_STAT)
        if len(error_text) > 0:
            print('Ошибка при формировании задания')
            print(error_text)
            return

        return self._prepare_task(task_type='duration', task_name=task_name, facility=facility, date_from=date_from,
                                  date_to=date_to, usetype_filter=usetype_filter, population_filter=population_filter,
                                  ages_filter=ages_filter, media_filter=media_filter, demo_filter=demo_filter,
                                  statistics=statistics, structure=structure, reach_n=None, excl_use=None,
                                  dup_media_filter=None,  is_duration=True)

    @staticmethod
    def _check_task_params(date_from, date_to, facility, statistics, structure, stat_list):
        error_text = ''
        if facility is None or facility not in ['desktop', 'mobile', 'desktop_pre']:
            error_text += 'facility не задано или не допустимо, допустимые значения: desktop, mobile, desktop_pre\n'
        if date_from is None:
            error_text += 'date_from должна быть задана, формат: YYYY-MM-DD\n'
        if date_to is None:
            error_text += 'date_to должна быть задана, формат: YYYY-MM-DD\n'
        if statistics is None or type(statistics) != list or len(statistics) == 0:
            error_text += 'не заданы статистики для задания.\n'
        if structure is None or type(structure) != dict or len(structure) == 0:
            error_text += 'не задана структура для результата.\n'
        strucat_date = structure.get('date', None)
        if strucat_date is not None and (strucat_date != 'day' and strucat_date != 'week' and strucat_date != 'month'
                                         and strucat_date != 'weekDay' and strucat_date != 'absence'):
            error_text += 'В структуре отчета в date допускаются значения: day, week, month, weekDay, absence .\n'
        struct_demo = structure.get('demo', None)
        if struct_demo is not None and (type(struct_demo) != list or len(struct_demo) == 0):
            error_text += 'Разбивка по DEMO должна быть задана в виде списка демо-переменных ["SEX", "AGE", ...].\n'
        strucat_media = structure.get('media', None)
        if strucat_media is not None and (type(strucat_media) != list or len(strucat_media) == 0):
            error_text += 'Разбивка по MEDIA должна быть задана в виде списка ["holding", "site", ...].\n'
        for stat in statistics:
            if stat not in stat_list:
                error_text += f'Статистика: {stat} не найдена.\n'
        if 'Uni' in statistics and 'media' in structure:
            error_text += f'Разбивка по MEDIA не применима для статистики Uni.\n'
        if 'Uni' in statistics and structure['usetype'] is True:
            error_text += f'Разбивка по USETYPE не применима для статистики Uni.\n'
        if 'Smp' in statistics and 'media' in structure:
            error_text += f'Разбивка по MEDIA не применима для статистики Smp.\n'
        if 'Smp' in statistics and structure['usetype'] is True:
            error_text += f'Разбивка по USETYPE не применима для статистики Smp.\n'
        return error_text

    def _convert_demo_structure(self, structure):
        # корректируем демо атрибуты: переводим названия в идентификаторы
        if structure.get('demo', None) is not None and type(structure['demo']) == list:
            demo_vals = []
            for v in structure['demo']:
                key = str(v).lower()
                if key in self.demo_dict:
                    demo_vals.append(self.demo_dict[key]['v'])
                else:
                    demo_vals.append(key)
            structure['demo'] = demo_vals
        return structure

    def send_audience_task(self, data):
        """
        Отправить задание на расчет аудиторных статистик.
        
        Parameters
        ----------
        
        data : str
            Текст задания в JSON формате.
            
        
        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который будет необходим для получения результата.
            
        """
        if data is not None:
            return self.rnet.send_request('post', '/task/audience', data)
        else:
            return None

    def send_duplication_task(self, data):
        """
        Отправить задание типа duplication, для расчета пересечения аудиторий.
        
        Parameters
        ----------
        
        data : str
            Текст задания в JSON формате.
            
        
        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который будет необходим для получения результата.
            
        """
        if data is not None:
            return self.rnet.send_request('post', '/task/duplication', data)
        else:
            return None

    def _send_duration_task(self, data):
        """
        Отправить задание типа duration, для расчета статистик по длительностям.
        
        Parameters
        ----------
        
        data : str
            Текст задания в JSON формате.
            
        
        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который будет необходим для получения результата.
            
        """
        if data is not None:
            return self.rnet.send_request('post', '/task/audience-duration', data)
        else:
            return None

    def wait_task(self, tsk):
        if tsk is None:
            return None
        if type(tsk) == dict:
            msgs = tsk.get('messages', None)
            for msg in msgs:
                print(msg)

            if tsk.get('taskId') is not None:
                tid = tsk.get('taskId', None)
                time.sleep(1)
                tstate = self.rnet.send_raw_request('get', '/task/state?task-id={}'.format(tid))
                print('Расчет задачи [ ', end='')
                s = dt.datetime.now()
                while tstate == 'IN_PROGRESS' or tstate == 'PENDING' or tstate == 'IN_QUEUE' or tstate == 'IDLE':
                    print('=', end=' ')
                    time.sleep(3)
                    tstate = self.rnet.send_raw_request('get', '/task/state?task-id={}'.format(tsk['taskId']))
                time.sleep(1)
                e = dt.datetime.now()
                print(f"] время расчета: {str(e - s)}")
                if tstate == 'DONE':
                    return tsk
                else:
                    print(f" Задача завершена со статутом: {tstate}")
        elif type(tsk) == list:
            tasks = list()
            # получим все идентификаторы заданий
            for t in tsk:
                cur_task = t.get('task')
                if cur_task is None:
                    continue
                tid = cur_task.get('taskId')
                if tid is None:
                    continue
                tasks.append(tid)
            # Проверим состояние заданий
            print(f'Расчет задач ({len(tasks)}) [ ', end='')
            s = dt.datetime.now()
            errors = dict()
            while True:
                time.sleep(3)
                # запросим состояние
                done_count = 0

                tstates = self.rnet.send_request('post', '/task/state/state-list', data=json.dumps(tasks))

                if tstates is None:
                    print("Ошибка при получении статусов заданий")
                    return None
                if type(tstates) == dict:
                    for tid, tstate in tstates.items():
                        if tstate == 'IN_PROGRESS' or tstate == 'PENDING' or tstate == 'IN_QUEUE' or tstate == 'IDLE':
                            continue
                        elif tstate == 'DONE':
                            done_count += 1
                        else:
                            errors[tid] = tstate
                            break
                print('=', end=' ')
                if done_count == len(tsk):
                    break
            # print("]")
            if len(errors) > 0:
                print(f"Одна или несколько задач завершились с ошибкой")
                for tid, tstate in errors.items():
                    print(f"Задача: {tid} состояние: {tstate}")
                return None
            e = dt.datetime.now()
            print(f"] время расчета: {str(e - s)}")
            return tsk

    def get_result(self, tsk):
        """
        Получить результат выполнения задания по его ID.
        
        Parameters
        ----------
        
        tsk : dict
            Задание
            
        
        Returns
        -------
        text : json
            Результат выполнения задания в JSON формате.
            
        """
        if tsk is None or tsk.get('taskId') is None:
            return None
        return self.rnet.send_request('get', '/task/result?task-id={}'.format(tsk['taskId']))

    @staticmethod
    def _result2table(data, axis_y=None):
        """
        Преобразует результат из JSON в DataFrame.

        Parameters
        ----------

        data : dict
            Результат расчета задачи в виде JSON объекта.

        axis_y : list
            Список осей, которые хотим поместить из столбцов в строки.
            
            Пример.
            В отчете присутствует разбивка по двум демографическим переменным:
                - пол
                - пол/возвраст
            Если не указать axis_y, то получим DataFrame, в котором каждая демографическая переменная будет в
            своем столбце:
                prj_name | Пол      | Пол Возвраст  | ...
                -----------------------------------
                total    | Женский | Женщины 18-24 | ...

            Такое представление не всегда удобно.
            Если укажем:
                axis_y = ['demo'],
            то получим другое представление DataFrame:

                prj_name | attrtitle_demo | attrval_demo  | ...
                ------------------------------------------------
                total    | Пол            | Женщины       | ...
                total    | Пол            | Мужчины       | ...
                total    | Пол / Возраст  | Женщины 18-24 | ...
                total    | Пол / Возраст  | Женщины 55-64 | ...

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        if data is None:
            return None

        axis_x = ['media', 'dt', 'usetype', 'demo', 'duplication', 'duplicationUsetype']
        if 'taskId' not in data:
            return None
        if axis_y is None:
            axis_y = []

        # распределяем оси, по умолчанию все в колонках, т.е. X
        # если передали что-то в осях Y, удаляем их из X
        for ay in axis_y:
            axis_x.remove(ay)

        cells = data['cells']
        # Строим DataFrame
        res = {}
        # get uniq keys
        ax_keys = {}
        ay_keys = {}
        val_keys = []
        for cell in cells:
            coord = cell['coord']
            # строим оси для Y
            for ay in axis_y:
                ay_name = '{}Point'.format(ay)
                if coord.get(ay_name) is None:
                    continue
                point = coord[ay_name]
                point_type = f"{ay}_{point['type']}"

                if ay not in ay_keys:
                    ay_keys[ay] = set()
                if point_type not in ay_keys[ay]:
                    ay_keys[ay].add(point_type)
            # строим оси для X
            for ax in axis_x:
                ax_name = '{}Point'.format(ax)
                if coord.get(ax_name) is None:
                    continue
                point = coord[ax_name]
                point_type = f"{ax}_{point['type']}"

                if ax not in ax_keys:
                    ax_keys[ax] = set()
                if point_type not in ax_keys[ax]:
                    ax_keys[ax].add(point_type)
            # строим список статистик, что реализовать LEFT OUTER
            # {'demo': ['demo_170', 'demo_350']}
            # {'dt': ['dt_month']}
            # {'usetype': ['usetype_usetype_id']}

            for k in cell['values'].keys():
                if k not in val_keys:
                    val_keys.append(k)

        for cell in cells:
            coord = cell['coord']
            for ay in axis_y:
                ay_name = '{}Point'.format(ay)
                if coord.get(ay_name) is None:
                    continue
                point = coord[ay_name]

                for point_type in ay_keys[ay]:
                    # media_holding
                    if f"{ay}_{point['type']}" != point_type:
                        continue
                    point_val = point['val']
                    # по типу точки и id нужно получить значение
                    # кладем название точки и id в столбцы
                    col_attr_name = f"attrtitle_{ay}"
                    col_attr_val = f"attrval_{ay}"

                    if col_attr_name not in res:
                        res[col_attr_name] = []
                    if col_attr_val not in res:
                        res[col_attr_val] = []
                    res[col_attr_name].append(point_type)
                    res[col_attr_val].append(point_val)

            for ax in axis_x:
                ax_name = '{}Point'.format(ax)
                if coord.get(ax_name) is None:
                    continue
                point = coord[ax_name]

                for point_type in ax_keys[ax]:
                    # demo_170
                    # media_holding
                    if f"{ax}_{point['type']}" != point_type:
                        point_val = '-'
                    else:
                        point_val = point['val']
                    # по типу точки и id нужно получить значение
                    if point_type not in res:
                        res[point_type] = []
                    res[point_type].append(point_val)

            for k in val_keys:
                kn = f"stat_{k}"
                if k in cell['values']:
                    v = cell['values'][k]
                else:
                    v = '-'
                if kn not in res:
                    res[kn] = []
                res[kn].append(v)
        return res, ay_keys

    def result2table(self, data, project_name=None, axis_y=None):
        """
        Преобразует результат из JSON в DataFrame.

        Parameters
        ----------

        data : dict
            Результат расчета задачи в виде JSON объекта.

        project_name : str
            Название проекта в итоговом DataFrame.
            Если указано, то в DataFrame добавляется в поле "prj_name" с названием project_name в данных.

        axis_y : list
            Список осей, которые хотим поместить из столбцов в строки.
            
            Пример.
            В отчете присутствует разбивка по двум демографическим переменным:
                - пол
                - пол/возвраст
            Если не указать axis_y, то получим DataFrame в котором каждая демографическая переменная будет в
            своем столбце:
                prj_name | Пол      | Пол Возвраст  | ...
                -----------------------------------
                total    | Женский | Женщины 18-24 | ...

            Такое представление не всегда удобно.
            Если укажем:
                axis_y = ['demo'],
            то получим другое представление DataFrame:

                prj_name | attrtitle_demo | attrval_demo  | ...
                ------------------------------------------------
                total    | Пол            | Женщины       | ...
                total    | Пол            | Мужчины       | ...
                total    | Пол / Возраст  | Женщины 18-24 | ...
                total    | Пол / Возраст  | Женщины 55-64 | ...

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        if data is None:
            return None

        axis_x = ['media', 'dt', 'usetype', 'demo', 'duplication', 'duplicationUsetype']
        if 'taskId' not in data:
            return None
        if axis_y is None:
            axis_y = []

        # распределяем оси, по умолчанию все в колонках, т.е. X
        # если передали что-то в осях Y, удаляем их из X
        for ay in axis_y:
            axis_x.remove(ay)
        if 'taskId' not in data:
            return None
        res, ay_keys = self._result2table(data, axis_y)
        res = self._get_text_names(res)
        self._get_ytext_names(ay_keys, res)
        # Корректируем название столбцов для ReachN

        df = pd.DataFrame(res)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        return df

    def result2hierarchy(self, data, project_name=None):
        """
        Преобразует результат из JSON в DataFrame.

        Parameters
        ----------

        data : dict
            Результат расчета задачи в виде JSON объекта.

        project_name : str
            Название проекта в итоговом DataFrame.
            Если указано, то в DataFrame добавляется в поле "prj_name" с названием project_name в данных.

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        if data is None:
            return None
        if 'taskId' not in data:
            return None

        # распределяем оси, по умолчанию все в колонках, т.е. X
        # если передали что-то в осях Y, удаляем их из X
        if 'taskId' not in data:
            return None
        res, ay_keys = self._result2table(data)
        # Корректируем название столбцов для ReachN

        res_size = 1000000000
        for col, vals in res.items():
            if not str(col).startswith('media_'):
                continue
            # получим размер самого маленького словаря
            if len(vals) < res_size:
                res_size = len(vals)
        # Перебираем и заполняем значения
        levels = {'holding': 1, 'site': 2, 'section': 3, 'subsection': 4,
                  "network": 1, "network_section": 2, "network_subsection": 3,
                  "ad_agency": 1, "brand": 2, "position": 3, "subbrand": 4}

        for i in range(0, res_size):
            lev = 1
            # Ищем не пустое значение
            necol_name = ''
            necol_val = None
            for col, vals in res.items():
                if not str(col).startswith('media_'):
                    continue
                cname = str(col)[6:]
                val = vals[i]
                if val == '-':
                    continue
                necol_name = cname
                necol_val = str(val)
                lev = levels[cname]
                break
            # Ищем для не пустого значения позицию в каталоге медиа-дерева
            cat_rows = self.rcats.holdings[self.rcats.holdings[f'{self._map_media_tree_id(necol_name)}_id'] == necol_val].iloc[0]
            # Заполняем пустые значения
            for col, vals in res.items():
                if not str(col).startswith('media_'):
                    continue
                necol_name = str(col)[6:]
                if levels[necol_name] <= lev:
                    vals[i] = cat_rows[f'{self._map_media_tree_id(necol_name)}_title']
        df = pd.DataFrame(res)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        return self.sort_df(df)

    @staticmethod
    def sort_df(df):
        sort_dict = {'prj_name': 0, 'media_holding': 1, 'media_site': 2, 'media_section': 3, 'media_subsection': 4,
                     'usetype': 5, 'date': 6, 'day': 7, 'month': 8, 'year': 9, 'quater': 10}
        sorted_cols = {}
        p = 100
        for col in df.columns:
            if col in sort_dict.keys():
                sorted_cols[sort_dict[col]] = col
            else:
                sorted_cols[p] = col
                p = p + 1
        sorted_cols = dict(sorted(sorted_cols.items())).values()
        dfs = df[sorted_cols]
        return dfs.sort_values(by=list(sorted_cols))

    @staticmethod
    def rename_reachn_columns(reach_n, df):
        """
        Переименовывает название столбцов для статистик ReachN+ в соответствии со списком диапазонов.

        При расчете статистик типа ReachN+, API возвращает результат в формате:
            reach_1, reach_2, ..., reach_n
        где 1-n - номер диапазона, заданного в задании на расчет. Данный метод преобразует названия статистик
        в соответствии со списком, заданным в задании.

        Parameters
        ----------

        reach_n : list
            Список диапазонов для ReachN.
            Например:
            reach_n = [{"from": 1, "to": 2},
                       {"from": 3, "to": 4},
                       {"from": 5, "to": 255}
                      ]

        df : DataFrame
            DataFrame с результами расчета.

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        if reach_n is None:
            return
        rename_dict = {}
        for i in range(0, len(reach_n)):
            f = reach_n[i]['from']
            t = reach_n[i]['to']
            n = f'stat_reach_{i}'
            if n in df.columns:
                rename_dict[n] = f'stat_reach_{f}_{t}'
        if len(rename_dict) > 0:
            df = df.rename(columns=rename_dict)
        return df

    def _get_names_for_usetype(self, key, vals):
        ax_title = key.replace('_usetype_id', '')
        data = []
        for v in vals:
            data.append(self.USETYPES_DICT[v])
        return ax_title, data

    def _get_names_for_demo(self, ax_dat, vals):
        rx_dig = re.compile(r'[0-9]+')
        da = self.rcats.demattr_exp
        cat_nums = da[da['varId'] == int(ax_dat)]
        var_title = cat_nums['varTitle'].tolist()[0]
        data = []
        for v in vals:
            if rx_dig.match(str(v)) is not None and int(ax_dat) != 148:
                cat_title = cat_nums[cat_nums['catNum'] == int(v)]['catTitle'].tolist()[0]
            else:
                cat_title = str(v)
            data.append(cat_title)
        return var_title, data

    def _get_names_for_media(self, ax_dat, vals):
        data = []
        hlds = self.rcats.holdings
        cat_id = f"{self._map_media_tree_id(ax_dat)}_id"
        cat_title = f"{self._map_media_tree_id(ax_dat)}_title"
        cache_dict = {}
        for v in vals:
            title = cache_dict.get(v)
            if v == '-':
                title = '-'
            if title is None:
                title = hlds[hlds[cat_id] == str(v)][cat_title].tolist()[0]
                cache_dict[v] = title
            data.append(title)
        return data

    @staticmethod
    def _map_media_tree_id(cat_id):
        mapper = {
            "holding": "holding", "site": "site", "section": "section", "subsection": "subsection",
            "network": "site", "network_section": "section", "network_subsection": "subsection",
            "ad_agency": "holding", "brand": "site", "subbrand": "section", "position": "subsection"
        }
        return mapper[cat_id]

    def _get_text_names(self, data):
        tdata = {}
        for key, vals in data.items():
            uindex = key.index('_')
            ax_name = key[:uindex]
            ax_dat = key[uindex+1:]
            if ax_name == 'usetype' or ax_name == 'duplicationUsetype':
                ax_title, v = self._get_names_for_usetype(key, vals)
                tdata[ax_title] = v
            elif ax_name == 'demo':
                ax_title, v = self._get_names_for_demo(ax_dat, vals)
                tdata[ax_title] = v
            elif ax_name == 'media' or ax_name == 'duplication':
                v = self._get_names_for_media(ax_dat, vals)
                tdata[key] = v
            # пока уберем, по имени stat_ ищем статистики
            # elif ax_name == 'stat':
            #     tdata[ax_dat] = vals
            else:
                tdata[key] = vals
        return tdata

    def _get_ytext_names(self, ay_keys, data):
        rx_dig = re.compile(r'[0-9]+')
        da = self.rcats.demattr_exp
        hlds = self.rcats.holdings
        cache_dict = {}
        for ay in ay_keys:
            col_attr_name = f"attrtitle_{ay}"
            col_attr_val = f"attrval_{ay}"
            for i in range(0, len(data[col_attr_val])):
                attr_name = data[col_attr_name][i]
                atr_val = data[col_attr_val][i]

                uindex = attr_name.index('_')
                ax_name = attr_name[:uindex]
                ax_dat = attr_name[uindex+1:]

                if ax_name == 'demo':
                    cat_nums = da[da['varId'] == int(ax_dat)]
                    var_title = cat_nums['varTitle'].tolist()[0]
                    if rx_dig.match(str(atr_val)) is not None:
                        cat_title = cat_nums[cat_nums['catNum'] == int(atr_val)]['catTitle'].tolist()[0]
                    else:
                        cat_title = str(atr_val)
                    data[col_attr_name][i] = var_title
                    data[col_attr_val][i] = cat_title
                elif ax_name == 'media' or ax_name == 'duplication':
                    cat_id = f"{ax_dat}_id"
                    cat_title = f"{ax_dat}_title"
                    title = cache_dict.get(atr_val)
                    if title is None:
                        title = hlds[hlds[cat_id] == str(atr_val)][cat_title].tolist()[0]
                        cache_dict[atr_val] = title
                    data[col_attr_val][i] = title
                elif ax_name == 'usetype' or ax_name == 'duplicationUsetype':
                    data[col_attr_name][i] = str(data[col_attr_name][i]).replace('_usetype_id', '')
                    data[col_attr_val][i] = self.USETYPES_DICT[atr_val]

    def calc_row_col(self, df_project, df_total_project, df_total_demo):
        """
        Вычисляет значения Row% и Col% для отчетов с разбивкой по демографии и проектам.

          Parameters
        ----------

        df_project : DataFrame
            Данные по проектам с разбивкой по демографии.

        df_total_project : DataFrame
            Данные по проектам без разбивки по демографии.

        df_total_demo : DataFrame
            Данные с разбивкой по демографии без фильтрации по проектам (Total Internet с разбивкой по демо).


        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        df_prj = pd.concat([df_project, df_total_project]).fillna('total')
        #df_total_uni = pd.concat([df_total_demo, df_universe]).fillna('total')
        df_prj_col_percent = self.calc_percents(df_prj, df_total_project, 'stat_col_prc_')
        df_prj_row_percent = self.calc_percents(df_prj, df_total_demo, 'stat_row_prc_')
        # Объединяем результат в один DataFrame
        df_tmp = df_prj.merge(df_prj_col_percent)
        df_tmp = df_tmp.merge(df_prj_row_percent).merge(df_prj_row_percent)
        # Переименуем колонки
        rename_dict = {}
        for col in df_tmp.columns:
            if str(col).startswith('stat_col_prc_'):
                rename_dict[col] = str(col).replace('stat_col_prc_', '') + '_col_prc'
            elif str(col).startswith('stat_row_prc_'):
                rename_dict[col] = str(col).replace('stat_row_prc_', '') + '_row_prc'
            elif str(col).startswith('stat_'):
                rename_dict[col] = str(col).replace('stat_', '')
        return df_tmp.rename(columns=rename_dict)

    def calc_duplication_row_col(self, df_project, df_total_project, df_total):
        """
        Вычисляет значения Row% и Col% для отчетов типа Duplication.

          Parameters
        ----------

        df_project : DataFrame
            Данные по пересечению аудитории проектов. Т.е. в DataFrame ожидается разбивка (поля)
            по media_объектам и duplication_объектам, например:
                - media_site,
                - duplication_site

        df_total_project : DataFrame
            Данные по аудитории проектов без учета пересечений.

        df_total : DataFrame
            Аудитория Total Internet для рассчитываемого периода/географии.

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.

        Examples
        -------
            Рассчитываем Reach для пересекающейся аудитории проектов Facebook.com и Vk.com. 
            Тогда в df_project ожидаем данные:

                media_site     | duplication_site | reach
                -------------------------------------
                Facebook.com   | Vk.com           | 1234.56

            В df_total_project ожидаем данные:

                media_site     | reach
                -------------------------------------
                Facebook.com   | 5678.90
                Vk.com         | 12340.56

            В df_universe ожидаем данные:

                reach
                ---------
                123400.00
        """
        # Ищем поля media и duplication
        rename_cols = {}
        media_cols = []
        drop_cols = []
        stat_cols = []

        for col in df_project.columns:
            if str(col).startswith('duplication_'):
                rename_cols[col] = 'media_' + col[12:]
            elif str(col).startswith('media_'):
                rename_cols[col] = 'duplication_' + col[6:]
                media_cols.append(col)
            elif str(col).startswith('prj_name'):
                drop_cols.append(col)
            elif str(col).startswith('stat_'):
                stat_cols.append(col)
        # формируем табличку для total
        tmp_total_project = df_total_project.drop(columns=drop_cols)
        tmp_total = df_total.drop(columns=drop_cols)
        lnk_field = 'lnk_hgeasjkfdsfhjk'
        tmp_total_project['lnk_field'] = lnk_field
        tmp_total['lnk_field'] = lnk_field
        tmp_total_all = tmp_total_project.merge(tmp_total, on=['lnk_field']).drop(columns=['lnk_field'])

        tmp_drop_cols = []
        tmp_rename_cols = {}
        for col in tmp_total_all.columns:
            if not str(col).startswith('stat_'):
                continue
            c = str(col)
            if c[-2:] == '_x':
                tmp_rename_cols[c] = c[:-2]
            if c[-2:] == '_y':
                tmp_drop_cols.append(c)

        # формируем перевернутую таблицу media_site <-> duplication_site
        df_cross_prj = pd.concat([
            df_project.drop(columns=drop_cols),
            df_project.drop(columns=drop_cols).rename(columns=rename_cols)]
        )
        # calc prc for prj
        df_tmp = df_cross_prj.merge(df_total_project.drop(columns=drop_cols), left_on=media_cols, right_on=media_cols)
        df_dup = pd.concat([df_tmp, tmp_total_all])

        cols = []
        cols4rename = {}
        cols4drop = []
        for col in df_dup.columns:
            if str(col).startswith('stat_'):
                continue
            c = str(col)
            if c[-2:] == '_x' or c[-2:] == '_y':
                c = c[:-2]
            if c not in cols:
                cols.append(c)
                cols4rename[col] = c
            else:
                cols4drop.append(col)

        df_tmp = df_dup.drop(columns=cols4drop).rename(columns=cols4rename)
        df_tmp = self._calc_prc(df_tmp, stat_cols, 'stat_prc_')
        df_tmpc = df_dup.merge(df_tmp)

        cols4rename = {}
        for col in df_tmpc.columns:
            if not str(col).startswith('stat_'):
                continue
            c = str(col)
            if c[-2:] == '_x':
                cols4rename[col] = f'duplication_{c[:-2]}'
            elif c[-2:] == '_y':
                cols4rename[col] = f'media_{c[:-2]}'
        for col in df_tmpc.columns:
            c = str(col)
            if c.startswith('media_') or c.startswith('duplication_'):
                df_tmpc[c] = df_tmpc[[c]].fillna('total')
            else:
                df_tmpc[c] = df_tmpc[[c]].fillna('-')
        df_tmpc = df_tmpc.rename(columns=cols4rename)
        return df_tmpc

    def calc_percents(self, df_project, df_total, row_prefix):
        """
        Вычисляет значения Row% и Col%.

          Parameters
        ----------

        df_project : DataFrame
            Данные по проектам с разбивкой по media и демографии.

        df_total_project : DataFrame
            Данные по аудитории проектов без учета демографии.

        Returns
        -------
        data : DataFrame
            DataFrame с результатом.
        """
        # получим список полей для связок и полей статистик
        link_cols = []
        stat_cols = []
        for col in df_project.columns:
            if col == 'prj_name':
                # пропускаем искуственное поле отвечающее за проект
                continue
            if str(col).startswith('stat_') and col in df_total:
                # stat column
                stat_cols.append(col)
            elif col in df_total:
                link_cols.append(col)

        # клеим DataFrame'ы - проекты и тоталы по проектам
        df_tmp = df_project.merge(df_total.drop(columns=['prj_name']), on=link_cols)
        df_tmp = self._calc_prc(df_tmp, stat_cols, row_prefix)
        return df_tmp

    @staticmethod
    def _calc_prc(df, stat_cols, row_prefix):
        # бежим по статистикам, считаем проценты
        for col in stat_cols:
            if str(col)[-3:] == 'per':
                continue
            colname = row_prefix + col.replace('stat_', '')
            df[colname] = df[f'{col}_x'] / df[f'{col}_y'] * 100.0
        # удаляем лишние столбцы
        cols4drop = []
        for col in df.columns:
            if str(col)[-2:] == '_x' or str(col)[-2:] == '_y':
                cols4drop.append(col)
        return df.drop(columns=cols4drop)

    @staticmethod
    def round_prc(df):
        return df.round(2)

    def save_report_info(self, facility=None, date_from=None, date_to=None, usetype_filter=None,
                         population_filter=None, ages_filter=None):
        """
        Сохраняет общую информацию о заданиях. Используется при сохранении отчета в Excel.

        Parameters
        ----------

        facility : str
            Установка : "desktop", "mobile", "desktop_pre".

        date_from : str
            Начало периода для расчета, дата в формате YYYY-MM-DD.

        date_to : str
            Конец периода для расчета, дата в формате YYYY-MM-DD.

        usetype_filter: list|None
            Список типов пользования Интернетом.

        population_filter: str|None
            Фильтр по численности населения (не используется).

        ages_filter: str|None
            Фильтр по возрастным группам (не используется).

            Задание в формате Responsum API.


        """
        self.task_info['calculated'] = {'val': dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'name': 'Дата/время расчета'}
        self.task_info['facility'] = {'val': facility, 'name': 'Тип установки'}
        self.task_info['date_from'] = {'val': date_from, 'name': 'Начало периода'}
        self.task_info['date_to'] = {'val': date_to, 'name': 'Конец периода'}
        self.task_info['usetype'] = {'val': usetype_filter, 'name': 'Типы пользования Интернетом'}
        self.task_info['population'] = {'val': population_filter, 'name': 'Население'}
        self.task_info['ages'] = {'val': ages_filter, 'name': 'Возрастные группы'}

    def _save_task_info(self, task_name='', facility=None, date_from=None, date_to=None, usetype_filter=None,
                        population_filter=None, ages_filter=None, media_filter=None, demo_filter=None,
                        statistics=None, structure=None):
        t = dict()
        t['task_name'] = task_name
        t['facility'] = facility
        t['date_from'] = date_from
        t['date_to'] = date_to
        t['usetype_filter'] = usetype_filter
        t['population_filter'] = population_filter
        t['ages_filter'] = ages_filter
        t['media_filter'] = media_filter
        t['demo_filter'] = demo_filter
        t['statistics'] = statistics
        t['structure'] = structure

        h = hashlib.md5(json.dumps(t, sort_keys=True).encode('utf-8')).hexdigest()
        self.task_info['tasks'][h] = t

    def get_report_info(self):
        """
        Возвращает информацию о рассчитываемом отчете в виде DataFrame, которая была предварительно сохранена
        с помощью метода save_report_info.

        Returns
        -------
        DataFrame
            Информация о рассчитываемом отчете.
        """
        data = list()
        for k, v in self.task_info.items():
            if k == 'tasks':
                continue
            data.append(f"{v['name']}: {v['val']}")

        for _, tsk in self.task_info['tasks'].items():
            data.append("")
            for tk, tv in tsk.items():
                data.append(f"{tk}: {tv}")
        return pd.DataFrame(data)

    def show_report_info(self):
        for k, v in self.task_info.items():
            if k == 'tasks':
                continue
            print(f"{v['name']}: {v['val']}")

    @staticmethod
    def get_excel_filename(task_name, export_path='../excel', add_date=True):
        """
        Формирует назание Excel-файла для отчета.

        Parameters
        ----------
        task_name : str
            Название отчета/задания.
        export_path: : str
            Путь к файл.
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

    @staticmethod
    def get_sql_from_list(media_object, items):
        sql_text = '('
        for item in items:
            sql_text += f"{media_object} = {item} OR "
        if len(sql_text) >= 3:
            sql_text = sql_text[:-3] + ')'
        return sql_text
