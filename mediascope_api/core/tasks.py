"""
Tasks module for Mediascope API
"""
import pandas as pd
from ..core import utils
from ..core import sql


class TaskBuilder:
    """
    TasjkBuilder class for Mediascope API
    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(TaskBuilder, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_info = dict()
        self.task_info['task'] = dict()

    @staticmethod
    def get_excel_filename(task_name: str, export_path: str = '../excel', add_dates: bool = True) -> str:
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

    @staticmethod
    def get_csv_filename(task_name: str, export_path: str = '../csv', add_dates: bool = True) -> str:
        """
        Получить имя csv файла

        Parameters
        ----------

        task_name : str
            Название задания

        export_path : str
            Путь к папке с csv файлами

        add_dates : bool
            Флаг - добавлять в имя файла дату или нет, по умолчанию = True

        Returns
        -------
        filename : str
            Путь и имя excel файла
        """
        return utils.get_csv_filename(task_name, export_path, add_dates)

    def save_report_info(self, tinfo: dict):
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
        с помощью метода save_report_info

        Returns
        -------
        result: DataFrame
            Информация о расчитываемом отчете
        """
        data = list()
        for tk, tv in self.task_info['task'].items():
            data.append(f"{tk}: {tv}")
        return pd.DataFrame(data)

    @staticmethod
    def add_range_filter(tsk: dict, date_filter):
        """
        Добавляет фильтр по диапазонам
        """
        if date_filter is not None and isinstance(date_filter, list) and len(date_filter) > 0:
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
    def add_filter(tsk: dict, filter_obj, filter_name):
        """
        Добавляет фильтр
        """
        if filter_obj is not None:
            if isinstance(filter_obj, dict):
                tsk['filter'][filter_name] = filter
            elif isinstance(filter_obj, str):
                tsk['filter'][filter_name] = sql.sql_to_json(filter_obj)
            elif filter_name == 'respondentFilter' and isinstance(filter_obj, pd.DataFrame):
                tsk['filter'][filter_name] = utils.get_dict_from_dataframe(filter_obj)

    @staticmethod
    def add_list_filter(tsk, filter_name, filter_obj_name, filter_obj):
        """
        Добавляет IN фильтр
        """
        if filter_obj is not None:
            if isinstance(filter_obj, list):
                flt = {
                    "operand": "AND",
                    "elements": [
                        {
                            "unit": filter_obj_name,
                            "relation": "IN",
                            "value": filter_obj
                        }
                    ]
                }
                tsk['filter'][filter_name] = flt

    @staticmethod
    def add_usetype_filter(tsk, usetype_filter, unit_name="useTypeId", filter_name="useTypeFilter"):
        """
        Добавляет фильтр по usetype
        """
        if usetype_filter is not None and isinstance(usetype_filter, list) and len(usetype_filter) > 0:
            usetype = {"operand": "OR", "elements": [{
                "unit": unit_name,
                "relation": "IN",
                "value": usetype_filter
            }]}
            tsk['filter'][filter_name] = usetype

    @staticmethod
    def add_slices(tsk, slices):
        """
        Добавляет срезы
        """
        if slices is not None:
            tsk['slices'] = slices

    @staticmethod
    def add_scales(tsk, scales):
        """
        Добавляет шкалы
        """
        if scales is not None:
            scales_json = {}
            for scale, val in scales.items():
                scales_json[scale] = []
                for v in val:
                    scales_json[scale].append({"from": v[0], "to": v[1]})
            tsk['scales'] = scales_json

    @staticmethod
    def add_sampling(tsk, sampling=42):
        """
        Добавляет сэмплирование
        """
        tsk['sampling'] = {'percent': str(sampling)}

    @staticmethod
    def add_sortings(tsk, sortings):
        """
        Добавляет сортировки
        """
        if sortings is not None:
            sort_json = {}
            sort_list = []

            for k,v in sortings.items():
                unit = {}
                unit["unit"] = k
                unit["direction"] = v
                sort_list.append(unit)
            sort_json["sortingUnits"] = sort_list
            tsk["sorting"] = sort_json

class TaskWithSubfiltersBuilder:
    """
    TaskBuilder class with subfilter for Mediascope API
    """
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(TaskWithSubfiltersBuilder, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task_info = dict()
        self.task_info['task'] = dict()

    @staticmethod
    def get_excel_filename(task_name: str, export_path: str = '../excel', add_dates: bool = True) -> str:
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

    @staticmethod
    def get_csv_filename(task_name: str, export_path: str = '../csv', add_dates: bool = True) -> str:
        """
        Получить имя csv файла

        Parameters
        ----------

        task_name : str
            Название задания

        export_path : str
            Путь к папке с csv файлами

        add_dates : bool
            Флаг - добавлять в имя файла дату или нет, по умолчанию = True

        Returns
        -------
        filename : str
            Путь и имя excel файла
        """
        return utils.get_csv_filename(task_name, export_path, add_dates)

    def save_report_info(self, tinfo: dict):
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
        с помощью метода save_report_info

        Returns
        -------
        result: DataFrame
            Информация о расчитываемом отчете
        """
        data = list()
        for tk, tv in self.task_info['task'].items():
            data.append(f"{tk}: {tv}")
        return pd.DataFrame(data)

    @staticmethod
    def add_range_filter(tsk: dict, date_filter):
        """
        Добавляет фильтр по диапазонам
        """
        if date_filter is None:
            return
        
        # Если это список диапазонов
        if isinstance(date_filter, list) and len(date_filter) > 0 and isinstance(date_filter[0], list):
            # Если subFilters еще нет, создаем его
            if 'subFilters' not in tsk['filter']:
                # Переносим существующие одиночные фильтры в subFilters
                existing_filters = {k: v for k, v in tsk['filter'].items() if k != 'subFilters'}
                tsk['filter'] = {'subFilters': []}
                
                if existing_filters:
                    for _ in range(len(date_filter)):
                        tsk['filter']['subFilters'].append(existing_filters.copy())
                else:
                    for _ in range(len(date_filter)):
                        tsk['filter']['subFilters'].append({})
            
            # Расширяем существующий subFilters до нужной длины
            while len(tsk['filter']['subFilters']) < len(date_filter):
                new_dict = {}
                if tsk['filter']['subFilters']:
                    common_filters = {k: v for k, v in tsk['filter']['subFilters'][0].items() 
                                    if k not in ['subFilters']}
                    new_dict.update(common_filters)
                tsk['filter']['subFilters'].append(new_dict)
            
            # Для каждого диапазона добавляем в соответствующий элемент subFilters
            for i, dr in enumerate(date_filter):
                TaskWithSubfiltersBuilder._add_range_to_dict(tsk['filter']['subFilters'][i], dr)
            return
        
        # Если есть subFilters и это единичный диапазон, добавляем во все элементы
        if 'subFilters' in tsk['filter']:
            for sub_filter in tsk['filter']['subFilters']:
                TaskWithSubfiltersBuilder._add_range_to_dict(sub_filter, date_filter)
            return
        
        # Обратная совместимость: единичный диапазон без subFilters
        TaskWithSubfiltersBuilder._add_range_to_dict(tsk['filter'], date_filter)

    @staticmethod
    def _add_range_to_dict(target_dict: dict, date_range):
        """Вспомогательная функция для добавления одного диапазона в словарь"""
        if isinstance(date_range, list) and len(date_range) > 0:
            date_ranges = {
                "operand": "OR",
                "children": []
            }
            for dr in date_range:
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
            target_dict['dateFilter'] = date_ranges

    @staticmethod
    def add_filter(tsk: dict, filter_obj, filter_name):
        """
        Добавляет фильтр
        
        Поддерживает:
            - Единичные значения: 
                * если subFilters нет - добавляется как filter[filter_name]
                * если subFilters есть - добавляется во все элементы subFilters
            - Список значений: 
                * создает subFilters если его нет
                * при создании subFilters переносит существующие одиночные фильтры во все элементы
                * добавляет новые фильтры по позициям
        """
        if filter_obj is None:
            return
        
        # Обработка списка фильтров
        if isinstance(filter_obj, list):
            # Если subFilters еще нет, создаем его
            if 'subFilters' not in tsk['filter']:
                # Переносим существующие одиночные фильтры в subFilters
                existing_filters = {k: v for k, v in tsk['filter'].items() if k != 'subFilters'}
                tsk['filter'] = {'subFilters': []}
                
                # Если были существующие фильтры, добавляем их в каждый элемент
                if existing_filters:
                    # Определяем длину: максимальная из длины нового списка или 1
                    target_length = len(filter_obj)
                    for _ in range(target_length):
                        tsk['filter']['subFilters'].append(existing_filters.copy())
                else:
                    # Создаем пустые словари для subFilters
                    for _ in range(len(filter_obj)):
                        tsk['filter']['subFilters'].append({})
            
            # Расширяем существующий subFilters до нужной длины
            while len(tsk['filter']['subFilters']) < len(filter_obj):
                # Новые элементы наследуют все существующие одиночные фильтры
                # (которые уже есть в первом элементе, так как они общие для всех)
                new_dict = {}
                if tsk['filter']['subFilters']:
                    # Копируем фильтры, которые есть в первом элементе (общие для всех)
                    common_filters = {k: v for k, v in tsk['filter']['subFilters'][0].items() 
                                    if k not in ['subFilters']}
                    new_dict.update(common_filters)
                tsk['filter']['subFilters'].append(new_dict)
            
            # Для каждого элемента списка добавляем фильтр в соответствующий словарь
            for i, single_filter in enumerate(filter_obj):
                TaskWithSubfiltersBuilder._add_single_filter_to_dict(tsk['filter']['subFilters'][i], single_filter, filter_name)
            return
        
        # Если есть subFilters, то одиночные фильтры добавляются во все элементы
        if ('subFilters' in tsk['filter']) and (filter_name not in ('baseGeoFilter', 'baseDemoFilter')):
            for sub_filter in tsk['filter']['subFilters']:
                TaskWithSubfiltersBuilder._add_single_filter_to_dict(sub_filter, filter_obj, filter_name)
            return
        
        # Обратная совместимость: единичное значение без subFilters
        TaskWithSubfiltersBuilder._add_single_filter_to_dict(tsk['filter'], filter_obj, filter_name)

    @staticmethod
    def _add_single_filter_to_dict(target_dict: dict, filter_obj, filter_name):
        """
        Вспомогательная функция для добавления одного фильтра в словарь
        """
        if isinstance(filter_obj, dict):
            target_dict[filter_name] = filter_obj
        elif isinstance(filter_obj, str):
            target_dict[filter_name] = sql.sql_to_json(filter_obj)
        elif filter_name == 'respondentFilter' and isinstance(filter_obj, pd.DataFrame):
            target_dict[filter_name] = utils.get_dict_from_dataframe(filter_obj)

    @staticmethod
    def add_list_filter(tsk, filter_name, filter_obj_name, filter_obj):
        """
        Добавляет IN фильтр
        """
        if filter_obj is not None:
            if isinstance(filter_obj, list):
                flt = {
                    "operand": "AND",
                    "elements": [
                        {
                            "unit": filter_obj_name,
                            "relation": "IN",
                            "value": filter_obj
                        }
                    ]
                }
                tsk['filter'][filter_name] = flt

    @staticmethod
    def add_usetype_filter(tsk, usetype_filter, unit_name="useTypeId", filter_name="useTypeFilter"):
        """
        Добавляет фильтр по usetype
        """
        if usetype_filter is None:
            return
        
        # Если это список списков
        if isinstance(usetype_filter, list) and len(usetype_filter) > 0 and isinstance(usetype_filter[0], list):
            # Если subFilters еще нет, создаем его
            if 'subFilters' not in tsk['filter']:
                # Переносим существующие одиночные фильтры в subFilters
                existing_filters = {k: v for k, v in tsk['filter'].items() if k != 'subFilters'}
                tsk['filter'] = {'subFilters': []}
                
                if existing_filters:
                    for _ in range(len(usetype_filter)):
                        tsk['filter']['subFilters'].append(existing_filters.copy())
                else:
                    for _ in range(len(usetype_filter)):
                        tsk['filter']['subFilters'].append({})
            
            # Расширяем существующий subFilters до нужной длины
            while len(tsk['filter']['subFilters']) < len(usetype_filter):
                new_dict = {}
                if tsk['filter']['subFilters']:
                    common_filters = {k: v for k, v in tsk['filter']['subFilters'][0].items() 
                                    if k not in ['subFilters']}
                    new_dict.update(common_filters)
                tsk['filter']['subFilters'].append(new_dict)
            
            # Для каждого списка usetype добавляем в соответствующий элемент subFilters
            for i, ut_filter in enumerate(usetype_filter):
                TaskWithSubfiltersBuilder._add_usetype_to_dict(tsk['filter']['subFilters'][i], ut_filter, unit_name, filter_name)
            return
        
        # Если есть subFilters и это единичный список, добавляем во все элементы
        if 'subFilters' in tsk['filter']:
            for sub_filter in tsk['filter']['subFilters']:
                TaskWithSubfiltersBuilder._add_usetype_to_dict(sub_filter, usetype_filter, unit_name, filter_name)
            return
        
        # Обратная совместимость: единичный список без subFilters
        TaskWithSubfiltersBuilder._add_usetype_to_dict(tsk['filter'], usetype_filter, unit_name, filter_name)

    @staticmethod
    def _add_usetype_to_dict(target_dict: dict, usetype_filter, unit_name, filter_name):
        """Вспомогательная функция для добавления одного usetype фильтра в словарь"""
        if isinstance(usetype_filter, list) and len(usetype_filter) > 0:
            usetype = {
                "operand": "OR", 
                "elements": [{
                    "unit": unit_name,
                    "relation": "IN",
                    "value": usetype_filter
                }]
            }
            target_dict[filter_name] = usetype

    @staticmethod
    def add_slices(tsk, slices):
        """
        Добавляет срезы
        """
        if slices is not None:
            tsk['slices'] = slices

    @staticmethod
    def add_scales(tsk, scales):
        """
        Добавляет шкалы
        """
        if scales is not None:
            scales_json = {}
            for scale, val in scales.items():
                scales_json[scale] = []
                for v in val:
                    scales_json[scale].append({"from": v[0], "to": v[1]})
            tsk['scales'] = scales_json

    @staticmethod
    def add_sampling(tsk, sampling=42):
        """
        Добавляет сэмплирование
        """
        tsk['sampling'] = {'percent': str(sampling)}

    @staticmethod
    def add_sortings(tsk, sortings):
        """
        Добавляет сортировки
        """
        if sortings is not None:
            sort_json = {}
            sort_list = []

            for k,v in sortings.items():
                unit = {}
                unit["unit"] = k
                unit["direction"] = v
                sort_list.append(unit)
            sort_json["sortingUnits"] = sort_list
            tsk["sorting"] = sort_json
