import json

class CrossWebChecker:

    def __new__(cls, cats, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(CrossWebChecker, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, cats, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cats = cats
        self.task_types = ['media', 'total', 'ad']
        self.check_list = {
            'task_type': {'types': [list], 'msg': 'Не верно задан тип задачи\n' +
                                                  f'Допустимые варианты: "{", ".join(self.task_types)}"'
                          },
            'date_filter': {'types': [list], 'msg': 'Период должен быть задан, формат: ' +
                                                    '[("YYYY-MM-DD", "YYYY-MM-DD")]\n'},
            'date_filter_item': {'types': [tuple], 'msg': 'Диапазон дат внутри периода должен быть задан как tuple,' +
                                                          'формат: [("YYYY-MM-DD", "YYYY-MM-DD")] \n'},
            'mart_filter': {'types': [str, dict], 'msg': 'Не верно задан фильтр по медиа.\n'},
            'demo_filter': {'types': [str, dict], 'msg': 'Не верно задан фильтр по демографии.\n'},
            'geo_filter': {'types': [str, dict], 'msg': 'Не верно задан фильтр по географии.\n'},
            'usetype_filter': {'types': [list], 'msg': 'не верно задан usetype_filter,\n' +
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
                   demo_filter, mart_filter, slices, statistics, scales):
        error_text = ''
        if self._check_filter('task_type', task_type, error_text):
            if task_type not in self.task_types:
                error_text += self.check_list['task_type']['msg']

        if self._check_filter('date_filter', date_filter, error_text):
            for r in date_filter:
                self._check_filter('date_filter_item', r, error_text)

        error_text = self._check_scales(statistics, scales, error_text)
        self._check_filter('mart_filter', mart_filter, error_text)
        self._check_filter('demo_filter', demo_filter, error_text)
        self._check_filter('geo_filter', geo_filter, error_text)

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
                error_text += f'Не верно заданы срезы (slices).\n'
            else:
                for s in slices:
                    if type(s) is not str:
                        error_text += f'Не верно задан срез (slices): {s}.\n'

        # Проверяем по доступным медиа-юнитами
        # self._check_units_in_task(statistics, slices, fil)

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
