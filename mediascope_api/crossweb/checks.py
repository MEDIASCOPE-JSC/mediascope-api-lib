import json
from . import catalogs


class CrossWebTaskChecker:

    def __new__(cls, cats: catalogs.CrossWebCats, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CrossWebTaskChecker, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, cats: catalogs.CrossWebCats, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cats = cats
        self.task_types = {'media': self.cats.get_media_unit(),
                           'total': self.cats.get_media_total_unit(),
                           'ad': self.cats.get_ad_unit(),
                           'monitoring': self.cats.get_monitoring_unit(),
                           'media-duplication': self.cats.get_media_duplication_unit()}
        self.check_list = {
            'task_type': {'types': [list], 'msg': 'Неверно задан тип задачи\n' +
                                                  f'Допустимые варианты: "{", ".join(self.task_types.keys())}"'
                          },
            'date_filter': {'types': [list], 'msg': 'Период должен быть задан, формат: ' +
                                                    '[("YYYY-MM-DD", "YYYY-MM-DD")]\n'},
            'date_filter_item': {'types': [tuple], 'msg': 'Диапазон дат внутри периода должен быть задан как tuple,' +
                                                          'формат: [("YYYY-MM-DD", "YYYY-MM-DD")] \n'},
            'mart_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по медиа.\n'},
            'demo_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по демографии.\n'},
            'geo_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по географии.\n'},
            'ad_description_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по рекламе.\n'},
            'event_description_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по событиям.\n'},
            'usetype_filter': {'types': [list], 'msg': 'неверно задан usetype_filter,\n' +
                                                       'формат: usetype_filter = [1,2,3].\n'},
            'statistics': {'types': [list], 'msg': 'Не заданы статистики для задания.\n'},
        }

    def _check_filter(self, name, obj, msg):
        if name not in self.check_list:
            return False
        if obj is not None:
            if type(obj) not in self.check_list[name]['types']:
                msg += self.check_list[name]['msg']
                return False
            elif type(obj) in [dict, list] and len(obj):
                msg += self.check_list[name]['msg']
                return False
        return True

    def check_task(self, task_type, date_filter, usetype_filter, geo_filter,
                   demo_filter, mart_filter, duplication_mart_filter, 
                   ad_description_filter, event_description_filter, slices, statistics, scales):
        error_text = ''
        if self._check_filter('task_type', task_type, error_text):
            if task_type not in self.task_types.keys():
                error_text += self.check_list['task_type']['msg']

        if self._check_filter('date_filter', date_filter, error_text):
            for r in date_filter:
                self._check_filter('date_filter_item', r, error_text)

        error_text = self._check_scales(statistics, scales, error_text)
        self._check_filter('mart_filter', mart_filter, error_text)
        self._check_filter('demo_filter', demo_filter, error_text)
        self._check_filter('geo_filter', geo_filter, error_text)
        self._check_filter('mart_filter', duplication_mart_filter, error_text)
        self._check_filter('ad_description_filter', ad_description_filter, error_text)
        self._check_filter('event_description_filter', event_description_filter, error_text)

        if self._check_filter('usetype_filter', mart_filter, error_text):
            ut_err = False
            uts = self.cats.usetypes['id'].to_list()
            for utype in usetype_filter:
                if type(utype) == int:
                    if utype not in uts:
                        ut_err = True
                        error_text += f'Usetype: {utype} не найден.\n'
            if ut_err:
                error_text += f'Доступные варианты: {self.cats.usetypes}\n'

        if slices is not None:
            if type(slices) is not list:
                error_text += f'Неверно заданы срезы (slices).\n'
            else:
                for s in slices:
                    if type(s) is not str:
                        error_text += f'Неверно задан срез (slices): {s}.\n'

        if len(error_text) > 0:
            print('Ошибка при формировании задания')
            print(error_text)
            return False
        else:
            return True

    @staticmethod
    def _check_scales(statistics, scales, error_text):
        for scale_stat in ['drfd', 'reachN']:
            if scale_stat in statistics:
                if scales is None or type(scales) != dict or len(scales) == 0:
                    error_text += f'1 нe задана шкала для статистики "{scale_stat}".\n'
                elif scales.get(scale_stat) is None:
                    error_text += f'2 нe задана шкала для статистики "{scale_stat}".\n'
                else:
                    scale_val = scales.get(scale_stat)
                    if type(scale_val) != list or len(scale_val) == 0:
                        error_text += f'3 нe задана шкала для статистики "{scale_stat}".\n'
                        error_text += f'формат: "{scale_stat}":[(F, T), ...].\n'
                    else:
                        for val_ft in scale_val:
                            if type(val_ft) != tuple or type(val_ft[0]) != int or type(val_ft[1]) != int:
                                error_text += f'4 шкала для статистики "{scale_stat} задана не верно,".\n'
                                error_text += f'формат: "{scale_stat}":[(F, T), ...].\n'
        return error_text

    def check_units_in_task(self, task_type, tsk):
        error_text = ''

        if type(tsk['statistics']) == list:

            for s in tsk['statistics']:
                if s not in self.task_types[task_type]['statistics']:
                    error_text += f'Неизвестная статистика "{s}".\n'
        if type(tsk['filter']) == dict:
            for filter_name, filter_val in tsk['filter'].items():
                filter_name = filter_name.replace('Filter', '')
                units = []                
                if filter_name == "duplicationMart":
                    filter_name = "mart"
                self.get_units(units, filter_val)
                self.check_units(f'фильтрах {filter_name}', units,
                                 self.task_types[task_type]['filters'][filter_name],
                                 error_text)
        if type(tsk['slices']) == list:
            avl_slices = self.get_avl_slices(task_type)
            for slice_name in tsk['slices']:
                if slice_name not in avl_slices:
                    error_text += f'Недопустимое название среза: "{slice_name}"'
        if len(error_text) > 0:
            print('Ошибка при формировании задания')
            print(error_text)
            return False
        else:
            return True

    def get_units(self, units, obj):
        if type(obj) == dict:
            for k, v in obj.items():
                if type(v) in [dict, list]:
                    self.get_units(units, v)
                elif type(v) == str:
                    if str(k) == 'unit':
                        units.append(v)
        elif type(obj) == list:
            for v in obj:
                self.get_units(units, v)

    @staticmethod
    def check_units(task_item_name, task_units, avl_units, error_text):
        for unit in task_units:
            if unit not in avl_units:
                error_text += f'Не допустимое название атрибута: "{unit}" в {task_item_name}'

    def get_avl_slices(self, task_type):
        slices = []
        for slice, vals in self.task_types[task_type]['slices'].items():
            for v in vals:
                slices.append(v)
        return slices







