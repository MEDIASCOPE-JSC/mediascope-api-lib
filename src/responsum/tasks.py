import json
import pandas as pd
from ..core import net
from . import catalogs

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
    def __new__(cls, facility_id, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ResponsumTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, facility_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ParserElement.enablePackrat()
        # TODO Add link to root_url
        self.rnet = net.MediascopeApiNetwork()

        self.rcats = catalogs.ResponsumCats(facility_id)
        self.demo_attr = self.rcats.get_demo()
        self.demo_dict = self.rcats.get_demo_dict(self.demo_attr)
        self.sql_parser = self.prepare_sql_parser()
        


    def prepare_sql_parser(self):
        """
        Подготовка SQL-like парсера, для разбора условий в фильтрах
        
        Returns
        -------
        
        simple_sql : obj
            Объект класса отвечающего за парсинг
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
        intNum = ppc.signed_integer()

        column_rval = (
            real_num | intNum | quotedString | column_name
        )  # need to add support for alg expressions
        where_condition = Group(
            (column_name + binop + column_rval)
            | (column_name + IN + Group("(" + delimitedList(column_rval) + ")"))
            | (column_name + IN + Group("(" + select_stmt + ")"))
            | (column_name + NOTIN + Group("(" + delimitedList(column_rval) + ")"))
        )

        where_expression = infixNotation(
            where_condition,
            [(AND, 2, opAssoc.LEFT), (OR, 2, opAssoc.LEFT),],
        )

        # define the grammar
        select_stmt <<= where_expression
        simple_sql = select_stmt

        # define Oracle comment format, and ignore them
        oracle_sql_comment = "--" + restOfLine
        simple_sql.ignore(oracle_sql_comment)
        return simple_sql


def get_point(self, left_obj, logic_oper, right_obj):
    """
    Формирует объект - point понятный для API Responsum
    
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
        Объект - point понятный для API Responsum
    
    """
    # TODO проверяем попадание левой части в список доступных
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
                # p = {"point": {"type": left_obj, "val": robj}, "operator": "EQUAL", "isNot": False}
                point['children'].append(point)
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


def find_points(self, obj):
    """
    Ищет в исходном объекте, объкты типа point и преобразует их в формат Responsum API
    """
    if type(obj) == list:
        if len(obj) == 3 and type(obj[0]) == str and obj[1] in ['=', '!=', 'in', 'notin']:
            return self.get_point(obj[0], obj[1], obj[2])

    i = 0
    while i < len(obj):
        obj_item = obj[i]
        if type(obj_item) == list:
            obj[i] = self.find_points(obj_item)
        i += 1
    return obj


def parse_expr(self, obj):
    """
    Преобразует выражение для фильтрации из набора вложенных списков в формат Responsum API
    
    Parameters
    ----------
    
    obj : dict | list
        Объект с условиями фильтрации в виде набора вложенных списков, полученный после работы SQL парсера
        
    
     Returns
    -------
    jdat : dict
        Условия фильтрации в формате Responsum API
    """
    if type(obj) == list:
        jdat = {'children': []}
        for obj_item in obj:
            if type(obj_item) == list:
                ret_data = self.parse_expr(obj_item)
                jdat['children'].append(ret_data)
            elif type(obj_item) == dict: # and 'point' in obj_item.keys():
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


def sql_to_json(self, sql_text):
    """
    Преобразует условие фильтрации записанное в SQL натации, в формат Responsum API
    
    Parameters
    ----------
    
    sql_text : str
        Текст условия в SQL формате
        
    
     Returns
    -------
    obj : dict
        Условия фильтрации в формате Responsum API
        
    """
    
    sql_obj = self.sql_parser.parseString(sql_text)
    # sql_obj.pprint()
    s = sql_obj.asList()[0]
    prep_points = self.find_points(s)
    return self.parse_expr(prep_points)



def send_audience_task(self, data):
    """
    Отправить задание на расчет аудиторных статистик
    
    Parameters
    ----------
    
    data : str
        Текст задания в JSON формате
        
    
     Returns
    -------
    text : json
        Ответ сервера, содержит taskid, который будет негоходим для получения результата
        
    """
    return self.rnet.send_request('post', '/task/audience', data)

def send_duplication_task(self, data):
    """
    Отправить задание типа duplication, для расчета пересечения аудиторий
    
    Parameters
    ----------
    
    data : str
        Текст задания в JSON формате
        
    
     Returns
    -------
    text : json
        Ответ сервера, содержит taskid, который будет негоходим для получения результата
        
    """
    return self.rnet.send_request('post', '/task/duplication', data)



def send_duration_task(self, data):
    """
    Отправить задание типа duration, для расчета расчета статистик по длительностям
    
    Parameters
    ----------
    
    data : str
        Текст задания в JSON формате
        
    
     Returns
    -------
    text : json
        Ответ сервера, содержит taskid, который будет негоходим для получения результата
        
    """
    return self.rnet.send_request('post', '/task/audience-duration', data)


def get_result(self, taskid):
    """
    Получить результат выполнения задания по его ID
    
    Parameters
    ----------
    
    taskid : str
        Идентификатор задания
        
    
     Returns
    -------
    text : json
        Результат выполнения задания в JSON формате
        
    """
    return self.rnet.send_request('get', '/task/result?task-id={}'.format(taskid))


def result2table(self, data, axis_x=None, axis_y=None):
    if axis_y is None:
        axis_y = ['media']
    if axis_x is None:
        axis_x = ['dt', 'usetype', 'demo', 'values']
    if 'taskId' not in data:
        return None
    cells = data['cells']
    # всего четыре оси: dtPoint, demoPoint, mediaPoint, usetypePoint
    # оси могут быть None
    # - определить количество осей
    # - распределить оси, нужно заранее подумать какие с какими могут
    # - получить список статистик
    # - отобразить
    axis_list = ['dt', 'usetype', 'demo', 'media']
    res = {}
    # get uniq keys
    ax_keys = {}
    for cell in cells:
        coord = cell['coord']
        for ax in axis_list:
            ax_name = '{}Point'.format(ax)
            if coord[ax_name] is None:
                continue
            point = coord[ax_name]
            point_type = point['type']
            
            if ax not in ax_keys:
                ax_keys[ax] = set()
            if point_type not in ax_keys[ax]:
                ax_keys[ax].add(point_type)
                
    for cell in cells:
        coord = cell['coord']
        for ax in axis_list:
            ax_name = '{}Point'.format(ax)
            if coord[ax_name] is None:
                continue
            point = coord[ax_name]
            
            for point_type in ax_keys[ax]:
                if point['type'] != point_type:
                    point_val = '-'
                else:
                    point_val = point['val']
            
                # по типу точки и id нужно получить значение
                if point_type not in res:
                    res[point_type] = []
                res[point_type].append(point_val)
            
        for k in  cell['values'].keys():
            v = cell['values'][k]
            if k not in res:
                res[k] = []
            res[k].append(v)
    return pd.DataFrame(res)


def build_audience_task(self, task_name='', facility=None, date_from=None, date_to=None, media_filter=None, demo_filter=None, statistics=None, structure=None):
    """
    Формирует текст задания для расчета аудиторных статистик
    
    Parameters
    ----------
    
    task_name : str
        Название задания, если не задано - формируется как: пользователь + типа задания + дата/время
    
    facility : str
        Установка : "desktop", "mobile", "desktop-pre".
    
    date_from : str
        Начало периода для расчета, дата в формате YYYY-MM-DD
        
    date_to : str
        Конец периода для расчета, дата в формате YYYY-MM-DD
    
    media_filter: str|None
        Условия фильтрации по медиа-объектам

    demo_filter: str|None
        Условия фильтрации по демографическим атрибутам
        
    statistics : list
        Список статистик, которые необходимо расчитать. 
        Например: ["UnwReach", "Reach", "OTS"]
    
    structure: dict
        Порядок группировки результата расчета, задается в виде словаря
        Пример:  
             {
                "date": "day",
                "media": ["site"],
                "usetype": False
              }
        
    
     Returns
    -------
    text : json
        Задание в формате Responsum API
        
        
    """
    error_text = ''
    if task_name is None or task_name == '':
        # make task name by user and datetime
        task_name = 'test'
    if facility is None or facility not in ['desktop', 'mobile', 'desktop-pre']:
        error_text += 'facility не задано или не допустимо, допустимые значения: desktop, mobile, desktop-pre\n'        
    if date_from is None:
        error_text += 'date_from должна быть задана, формат: YYYY-MM-DD\n'
    if date_to is None:
        error_text += 'date_to должна быть задана, формат: YYYY-MM-DD\n'
    if statistics is None or (type(statistics) == list and len(statistics) == 0):
        error_text += 'не заданы статистики для задания.\n'
    if structure is None or (type(structure) == list and len(structure) == 0):
        error_text += 'не задана структура для результат.\n'
    if len(error_text) > 0:
        print('Ошибка при формировании задания')
        print(error_text)
        return
    
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
    if media_filter is not None:
        media_sql = self.sql_to_json(media_filter)
        tsk['filters']['media'] = media_sql

    if demo_filter is not None:
        demo_sql = self.sql_to_json(demo_filter)
        tsk['filters']['demo'] = demo_sql
    return json.dumps(tsk)


def build_duplication_task(self, task_name='', facility=None, date_from=None, date_to=None, media_filter=None, dup_media_filter=None, demo_filter=None, statistics=None, structure=None):
    """
    Формирует текст задания типа duplication, для расчета пересечения аудиторий
    
    Parameters
    ----------
    
    task_name : str
        Название задания, если не задано - формируется как: пользователь + типа задания + дата/время
    
    facility : str
        Установка : "desktop", "mobile", "desktop-pre".
    
    date_from : str
        Начало периода для расчета, дата в формате YYYY-MM-DD
        
    date_to : str
        Конец периода для расчета, дата в формате YYYY-MM-DD
    
    media_filter: str
        Условия фильтрации по медиа-объектам
    
    dup_media_filter: str
        Условия фильтрации по медиа-объектам для оси duplicatiob

    demo_filter: str|None
        Условия фильтрации по демографическим атрибутам
        
    statistics : list
        Список статистик, которые необходимо расчитать. 
        Например: ["UnwReach", "Reach", "OTS"]
    
    structure: dict
        Порядок группировки результата расчета, задается в виде словаря
        Пример:  
             {
                "date": "day",
                "media": ["site"],
                "usetype": False
              }
        
    
     Returns
    -------
    text : json
        Задание в формате Responsum API
        
        
    """
    error_text = ''
    if task_name is None or task_name == '':
        # make task name by user and datetime
        task_name = 'test'
    if facility is None or facility not in ['desktop', 'mobile', 'desktop-pre']:
        error_text += 'facility не задано или не допустимо, допустимые значения: desktop, mobile, desktop-pre\n'        
    if date_from is None:
        error_text += 'date_from должна быть задана, формат: YYYY-MM-DD\n'
    if date_to is None:
        error_text += 'date_to должна быть задана, формат: YYYY-MM-DD\n'
    if dup_media_filter is None or media_filter is None:
        error_text += 'не заданы медиа-объекты для построения пересечения.\n'
    if statistics is None or (type(statistics) == list and len(statistics) == 0):
        error_text += 'не заданы статистики для задания.\n'
    if structure is None or (type(structure) == list and len(structure) == 0):
        error_text += 'не задана структура для результат.\n'
    if len(error_text) > 0:
        print('Ошибка при формировании задания')
        print(error_text)
        return
    
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
    if media_filter is not None:
        media_sql = self.sql_to_json(media_filter)
        tsk['filters']['media'] = media_sql
    
    if dup_media_filter is not None:
        dup_media_sql = self.ql_to_json(dup_media_filter)
        tsk['filters']['duplicationMedia'] = dup_media_sql

    if demo_filter is not None:
        demo_sql = self.sql_to_json(demo_filter)
        tsk['filters']['demo'] = demo_sql
    return json.dumps(tsk)


def build_duration_task(self, task_name='', facility=None, date_from=None, date_to=None, media_filter=None, dup_media_filter=None, demo_filter=None, statistics=None, structure=None):
    """
    Формирует текст задания типа duplication, для расчета пересечения аудиторий
    
    Parameters
    ----------
    
    task_name : str
        Название задания, если не задано - формируется как: пользователь + типа задания + дата/время
    
    facility : str
        Установка : "desktop", "mobile", "desktop-pre".
    
    date_from : str
        Начало периода для расчета, дата в формате YYYY-MM-DD
        
    date_to : str
        Конец периода для расчета, дата в формате YYYY-MM-DD
    
    media_filter: str
        Условия фильтрации по медиа-объектам
    
    dup_media_filter: str
        Условия фильтрации по медиа-объектам для оси duplicatiob

    demo_filter: str|None
        Условия фильтрации по демографическим атрибутам
        
    statistics : list
        Список статистик, которые необходимо расчитать. 
        Например: ["UnwReach", "Reach", "OTS"]
    
    structure: dict
        Порядок группировки результата расчета, задается в виде словаря
        Пример:  
             {
                "date": "day",
                "media": ["site"],
                "usetype": False
              }
        
    
     Returns
    -------
    text : json
        Задание в формате Responsum API
        
        
    """
    error_text = ''
    if task_name is None or task_name == '':
        # make task name by user and datetime
        task_name = 'test'
    if facility is None or facility not in ['desktop', 'mobile', 'desktop-pre']:
        error_text += 'facility не задано или не допустимо, допустимые значения: desktop, mobile, desktop-pre\n'        
    if date_from is None:
        error_text += 'date_from должна быть задана, формат: YYYY-MM-DD\n'
    if date_to is None:
        error_text += 'date_to должна быть задана, формат: YYYY-MM-DD\n'
    if statistics is None or (type(statistics) == list and len(statistics) == 0):
        error_text += 'не заданы статистики для задания.\n'
    if structure is None or (type(structure) == list and len(structure) == 0):
        error_text += 'не задана структура для результат.\n'
    if len(error_text) > 0:
        print('Ошибка при формировании задания')
        print(error_text)
        return
    
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
    if media_filter is not None:
        media_sql = self.sql_to_json(media_filter)
        tsk['filters']['media'] = media_sql
    
    if dup_media_filter is not None:
        dup_media_sql = self.sql_to_json(dup_media_filter)
        tsk['filters']['duplicationMedia'] = dup_media_sql

    if demo_filter is not None:
        demo_sql = self.selfsql_to_json(demo_filter)
        tsk['filters']['demo'] = demo_sql
    return json.dumps(tsk)



