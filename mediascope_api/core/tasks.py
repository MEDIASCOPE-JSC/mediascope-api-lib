import pandas as pd
import json
from ..core import utils
from ..core import sql


class TaskBuilder:
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
    def add_filter(tsk: dict, filter_obj, filter_name):
        if filter_obj is not None:
            if type(filter_obj) == dict:
                tsk['filter'][filter_name] = filter
            elif type(filter_obj) == str:
                tsk['filter'][filter_name] = sql.sql_to_json(filter_obj)
            elif filter_name == 'respondentFilter' and isinstance(filter_obj, pd.DataFrame):
                tsk['filter'][filter_name] = utils.get_dict_from_dataframe(filter_obj)

    @staticmethod
    def add_list_filter(tsk, filter_name, filter_obj_name, filter_obj):
        if filter_obj is not None:
            if type(filter_obj) == list:
                filter = {
                    "operand": "AND",
                    "elements": [
                        {
                            "unit": filter_obj_name,
                            "relation": "IN",
                            "value": filter_obj
                        }
                    ]
                }
                tsk['filter'][filter_name] = filter

    @staticmethod
    def add_usetype_filter(tsk, usetype_filter, unit_name="useTypeId", filter_name="useTypeFilter"):
        # Добавляем фильтр по usetype
        if usetype_filter is not None and type(usetype_filter) == list and len(usetype_filter) > 0:
            usetype = {"operand": "OR", "elements": [{
                "unit": unit_name,
                "relation": "IN",
                "value": usetype_filter
            }]}
            tsk['filter'][filter_name] = usetype

    @staticmethod
    def add_slices(tsk, slices):
        # Добавляем срезы
        if slices is not None:
            tsk['slices'] = slices

    @staticmethod
    def add_scales(tsk, scales):
        # Добавляем шкалы
        if scales is not None:
            scales_json = {}
            for scale, val in scales.items():
                scales_json[scale] = []
                for v in val:
                    scales_json[scale].append({"from": v[0], "to": v[1]})
            tsk['scales'] = scales_json

    @staticmethod
    def add_sampling(tsk, sampling=42):
        tsk['sampling'] = {'percent': str(sampling)}

    @staticmethod
    def add_sortings(tsk, sortings):
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
            
