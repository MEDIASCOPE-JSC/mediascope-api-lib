import pyparsing
from pyparsing import (
    Word,
    delimitedList,
    Group,
    alphas,
    alphanums,
    Forward,
    oneOf,
    sglQuotedString,
    quotedString,
    infixNotation,
    opAssoc,
    restOfLine,
    CaselessKeyword,
    ParserElement,
    pyparsing_common as ppc
)


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
    AND, OR, IN, NOTIN, NIN = map(
        CaselessKeyword, "and or in notin nin".split()
    )

    ident = Word(alphas, alphanums + "_$").setName("identifier")
    column_name = delimitedList(ident, ".", combine=True).setName("column name")
    # column_name.addParseAction(ppc.downcaseTokens)

    binop = oneOf("= != > < <= >=", caseless=True)
    real_num = ppc.real()
    int_num = ppc.signed_integer()

    column_rval = (
            real_num | int_num | quotedString | column_name
    )  # need to add support for alg expressions
    quotedString.setParseAction(pyparsing.removeQuotes)
    where_condition = Group(
        (column_name + binop + column_rval)
        | (column_name + IN + Group("(" + delimitedList(column_rval) + ")"))
        | (column_name + IN + Group("(" + select_stmt + ")"))
        | (column_name + NOTIN + Group("(" + delimitedList(column_rval) + ")"))
        | (column_name + NIN + Group("(" + delimitedList(column_rval) + ")"))
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


def _get_point(left_obj, logic_operand, right_obj):
    """
    Формирует объект - point понятный для API

    Parameters
    ----------

    left_obj : str
        Левая часть выражения
    logic_operand : str
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
    if logic_operand in ['in', 'notin', 'nin']:
        # ожидаем в правой части список атрибутов, бежим по нему
        if type(right_obj) == list:
            point = {"unit": left_obj, "relation": str(logic_operand).upper(), "value": []}
            for robj in right_obj:
                if type(robj) == str and (robj == '(' or robj == ')'):
                    # пропускаем скобки, объекты и так лежат в отдельном списке
                    continue
                # формируем условие в json формате
                point['value'].append(robj)
    elif logic_operand == '!=':
        point = {"unit": left_obj, "relation": "NEQ", "value": right_obj}
    elif logic_operand == '>':
        point = {"unit": left_obj, "relation": "GT", "value": right_obj}
    elif logic_operand == '<':
        point = {"unit": left_obj, "relation": "LT", "value": right_obj}
    elif logic_operand == '>=':
        point = {"unit": left_obj, "relation": "GTE", "value": right_obj}
    elif logic_operand == '<=':
        point = {"unit": left_obj, "relation": "LTE", "value": right_obj}
    else:
        point = {"unit": left_obj, "relation": "EQ", "value": right_obj}

    return point


def _find_points(obj):
    """
    Ищет в исходном объекте, объекты типа point и преобразует их в формат API
    """
    if type(obj) == list:
        if len(obj) == 3 and type(obj[0]) == str and obj[1] in ['=', '!=', 'in', 'nin', ">", "<", ">=", "<="]:
            return _get_point(obj[0], obj[1], obj[2])
    i = 0
    while i < len(obj):
        obj_item = obj[i]
        if type(obj_item) == list:
            obj[i] = _find_points(obj_item)
        i += 1
    return obj


def _parse_expr(obj):
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
        jdata = {}
        for obj_item in obj:
            if type(obj_item) == list:
                ret_data = _parse_expr(obj_item)
                if jdata.get('children') is None:
                    jdata['children'] = [ret_data]
                else:
                    jdata['children'].append(ret_data)
            elif type(obj_item) == dict:  # and 'point' in obj_item.keys():
                if obj_item.get('elements') is None:
                    if jdata.get('elements') is None:
                        jdata['elements'] = []
                    jdata['elements'].append(obj_item)
                else:
                    if jdata.get('children') is None:
                        jdata['children'] = []
                    jdata['children'].append(obj_item)
            elif type(obj_item) == str and obj_item in ['or', 'and']:
                jdata["operand"] = obj_item.upper()
        return jdata
    elif type(obj) == dict:
        jdata = {'elements': []}
        jdata['elements'].append(obj)
        jdata["operand"] = 'OR'
        return jdata


def sql_to_json(sql_text):
    """
    Преобразует условие фильтрации записанное в SQL нотации, в формат API

    Parameters
    ----------

    sql_text : str
        Текст условия в SQL формате


    Returns
    -------
    obj : dict
        Условия фильтрации в формате API

    """
    sql_parser = _prepare_sql_parser()
    sql_obj = sql_parser.parseString(sql_text)

    s = sql_obj.asList()[0]
    prep_points = _find_points(s)
    
    return _parse_expr(prep_points)

def sql_to_units(sql_text):
    """
    Преобразует условие фильтрации записанное в SQL нотации, в список элементов фильтров для проверки правильности

    Parameters
    ----------

    sql_text : str
        Текст условия в SQL формате


    Returns
    -------
    obj : list
        Список элементов фильтров

    """
    sql_parser = _prepare_sql_parser()
    sql_obj = sql_parser.parseString(sql_text)

    s = sql_obj.asList()[0]
    prep_points = _find_points(s)
    
    result = []
    
    if type(prep_points) == list:
        result = [i['unit'] for i in prep_points if type(i) == dict]
    else:
        result = [prep_points['unit']]
    return result


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
