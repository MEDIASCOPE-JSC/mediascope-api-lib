from . import catalogs
from ..core import sql
import difflib as dl


class MediaVortexTaskChecker:
    def __new__(cls, cats: catalogs.MediaVortexCats, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MediaVortexTaskChecker, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, cats: catalogs.MediaVortexCats, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cats = cats
        self.task_types = {}
        self.check_list = {
            'task_type': {
                'types': [str], 'msg': 'Неверно задан тип задачи\n' +
                f'Допустимые варианты: "{", ".join(self.task_types.keys())}\n"'
                },
            'date_filter': {'types': [list], 'msg': 'Период должен быть задан, формат: ' +
                                                    '[("YYYY-MM-DD", "YYYY-MM-DD")]\n'},
            'date_filter_item': {'types': [tuple], 'msg': 'Диапазон дат внутри периода должен быть задан как tuple,' +
                                                          'формат: [("YYYY-MM-DD", "YYYY-MM-DD")] \n'},
            'weekdate_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по дням недели.\n'},
            'daytype_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по типам дней.\n'},
            'company_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по телекомпаниям.\n'},
            'region_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по регионам.\n'},
            'time_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по времени.\n'},
            'location_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по местоположению.\n'},
            'basedemo_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по базовому демо.\n'},
            'targetdemo_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по целевому демо.\n'},
            'program_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по программам.\n'},
            'break_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по перерывам.\n'},
            'ad_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по рекламе.\n'},
            'subject_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по теме.\n'},
            'duration_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр по длительности.\n'},
            'duplication_company_filter': {'types': [str, dict], 'msg': 'Неверно задан duplication company фильтр.\n'},
            'duplication_time_filter': {'types': [str, dict], 'msg': 'Неверно задан duplication time фильтр.\n'},
            'platform_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр платформы (биг тв).\n'},
            'playbacktype_filter': {'types': [str, dict], 'msg': 'Неверно задан фильтр типа плейбека (биг тв).\n'},
            'bigtv_filter': {'types': [str, dict], 'msg': 'Неверно задан биг тв фильтр.\n'},
            'statistics': {'types': [list], 'msg': 'Не заданы статистики для задания.\n'},
        }        
        self.error_text = ''

    def _check_filter(self, name, obj):
        if name not in self.check_list:
            return False
        if obj is not None:
            if type(obj) not in self.check_list[name]['types']:                
                self.error_text += self.check_list[name]['msg']
                return False
            elif not len(obj):                
                self.error_text += self.check_list[name]['msg']
                return False
        return True
    
    def _check_filter_units(self, task_type, name, obj):
        result = True
        if obj:
            units = sql.sql_to_units(obj)                        
            for u in units:
                if u not in self.task_types[task_type]['filters']:
                    result = False
                    self.error_text += f'Неизвестная переменная "{u}" в фильтре "{name}". '
                    probably_matches = dl.get_close_matches(u, self.task_types[task_type]['filters'], n=3)
                    if len(probably_matches) > 0:
                        matches = '" или "'.join(probably_matches)
                        self.error_text += f'Возможно соответствует "{matches}".\n'
        return result

    def check_task(self, task_type, date_filter, weekdate_filter, daytype_filter,
                   company_filter, region_filter, time_filter, location_filter,
                   basedemo_filter, targetdemo_filter, program_filter, break_filter,
                   ad_filter, subject_filter, duration_filter, duplication_company_filter,
                   duplication_time_filter, platform_filter, playbacktype_filter,
                   bigtv_filter, slices, statistics, scales, sortings, kit_id):

        self.task_types = {
            'timeband': self.cats.get_timeband_unit(kit_id),
            'simple': self.cats.get_simple_unit(kit_id),
            'crosstab': self.cats.get_crosstab_unit(kit_id),
            'consumption-target': self.cats.get_consumption_target_unit(kit_id),
            'duplication-timeband': self.cats.get_duplication_timeband_unit(kit_id),
            'respondent-analysis': self.cats.get_respondent_analysis_unit(kit_id)
        }

        self.error_text = ''
        
        self._check_filter('task_type', task_type)

        if self._check_filter('date_filter', date_filter):
            for r in date_filter:                
                self._check_filter('date_filter_item', r)

        self._check_scales(statistics, scales)
        if self._check_filter('weekdate_filter', weekdate_filter):
            self._check_filter_units(task_type, 'weekdate_filter', weekdate_filter)
        
        if self._check_filter('daytype_filter', daytype_filter):
            self._check_filter_units(task_type, 'daytype_filter', daytype_filter)
                    
        if self._check_filter('company_filter', company_filter):
            self._check_filter_units(task_type, 'company_filter', company_filter)
                    
        if region_filter is not None:
            print("regionFilter в настоящее время не используется. Используйте companyFilter")
        #if self._check_filter('region_filter', region_filter):
        #    self._check_filter_units(task_type, 'region_filter', region_filter)
        
        if self._check_filter('time_filter', time_filter):
            self._check_filter_units(task_type, 'time_filter', time_filter)
            
        if self._check_filter('location_filter', location_filter):
            self._check_filter_units(task_type, 'location_filter', location_filter)
            
        if self._check_filter('basedemo_filter', basedemo_filter):
            self._check_filter_units(task_type, 'basedemo_filter', basedemo_filter)
            
        if self._check_filter('targetdemo_filter', targetdemo_filter):
            self._check_filter_units(task_type, 'targetdemo_filter', targetdemo_filter)
        
        if self._check_filter('program_filter', program_filter):
            self._check_filter_units(task_type, 'program_filter', program_filter)
        
        if self._check_filter('break_filter', break_filter):
            self._check_filter_units(task_type, 'break_filter', break_filter)
        
        if self._check_filter('ad_filter', ad_filter):
            self._check_filter_units(task_type, 'ad_filter', ad_filter)
        
        if self._check_filter('subject_filter', subject_filter):
            self._check_filter_units(task_type, 'subject_filter', subject_filter)
        
        if self._check_filter('duration_filter', duration_filter):
            self._check_filter_units(task_type, 'duration_filter', duration_filter)

        if self._check_filter('duplication_company_filter', duplication_company_filter):
            self._check_filter_units(task_type, 'duplication_company_filter', duplication_company_filter)

        if self._check_filter('duplication_time_filter', duplication_time_filter):
            self._check_filter_units(task_type, 'duplication_time_filter', duplication_time_filter)

        if self._check_filter('bigtv_filter', bigtv_filter):
            self._check_filter_units(task_type, 'bigtv_filter', bigtv_filter)

        if self._check_filter('platform_filter', platform_filter):
            self._check_filter_units(task_type, 'platform_filter', platform_filter)

        if self._check_filter('playbacktype_filter', playbacktype_filter):
            self._check_filter_units(task_type, 'playbacktype_filter', playbacktype_filter)
        
        if slices is not None:
            if type(slices) is not list:
                self.error_text += f'Неверно заданы срезы (slices).\n'
            else:
                for s in slices:
                    if type(s) is not str:
                        self.error_text += f'Неверно задан срез (slices): {s}.\n'

        if sortings is not None:
            if type(sortings) != dict:
                 self.error_text += f'Некорректный тип параметра sortings: допускается тип - dict.\n'

        if len(self.error_text) > 0:
            print('Ошибка при формировании задания')
            print(self.error_text)
            return False
        else:
            return True

    def _check_scales(self, statistics, scales):
        for scale_stat in ['drfd', 'reachN']:
            if scale_stat in statistics:
                if scales is None or type(scales) != dict or len(scales) == 0:
                    self.error_text += f'1 нe задана шкала для статистики "{scale_stat}".\n'
                elif scales.get(scale_stat) is None:
                    self.error_text += f'2 нe задана шкала для статистики "{scale_stat}".\n'
                else:
                    scale_val = scales.get(scale_stat)
                    if type(scale_val) != list or len(scale_val) == 0:
                        self.error_text += f'3 нe задана шкала для статистики "{scale_stat}".\n'
                        self.error_text += f'формат: "{scale_stat}":[(F, T), ...].\n'
                    else:
                        for val_ft in scale_val:
                            if type(val_ft) != tuple or type(val_ft[0]) != int or type(val_ft[1]) != int:
                                self.error_text += f'4 шкала для статистики "{scale_stat} задана не верно,".\n'
                                self.error_text += f'формат: "{scale_stat}":[(F, T), ...].\n'

    def check_units_in_task(self, task_type, tsk):
        error_text = ''

        if type(tsk['statistics']) == list:
            for s in tsk['statistics']:
                if s not in self.task_types[task_type]['statistics']:
                    error_text += f'Неизвестная статистика "{s}". '
                    probably_matches = dl.get_close_matches(s, self.task_types[task_type]['statistics'], n=3)
                    if len(probably_matches) > 0:
                        matches = '" или "'.join(probably_matches)
                        error_text += f'Возможно соответствует "{matches}".\n'
        if type(tsk['filter']) == list:
            for filter_name in tsk['filter']:                
                error_text = self.check_units(f'фильтрах {filter_name}', filter_name,
                                              self.task_types[task_type]['filters'],
                                              error_text)
        if task_type != 'consumption-target':
            if type(tsk['slices']) == list:
                avl_slices = self.get_avl_slices(task_type)
                for slice_name in tsk['slices']:
                    if slice_name not in avl_slices:
                        error_text += f'Недопустимое название среза: "{slice_name}". '
                        probably_matches = dl.get_close_matches(slice_name, avl_slices, n=3)
                        if len(probably_matches) > 0:
                            matches = '" или "'.join(probably_matches)
                            error_text += f'Возможно соответствует "{matches}".\n'
        if 'sorting' in tsk.keys():
            avl_cols = tsk['statistics'] + tsk['slices']
            avaliable = '", "'.join(avl_cols)
            for i in tsk["sorting"]['sortingUnits']:  
                if i['unit'] not in avl_cols:
                    error_text += f"Cортировка по {i['unit']} невозможна, так как этого элемента нет среди заданных срезов и статистик: {avaliable}.\n"
                    
                if i["direction"] not in(['ASC','DESC']):
                    error_text += f"Недопустимое значение в параметре sortings: {i['direction']}, допустимые значения: 'ASC','DESC'.\n"        
        
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
                error_text += f'Недопустимое название атрибута: "{unit}" в {task_item_name}'
                probably_matches = dl.get_close_matches(unit, avl_units, n=3)
                if len(probably_matches) > 0:
                    matches = '" или "'.join(probably_matches)
                    error_text += f'Возможно соответствует "{matches}".\n'
        return error_text

    def get_avl_slices(self, task_type):        
        return self.task_types[task_type]['slices']
    
    def get_avl_stats(self, task_type):
        return self.task_types[task_type]['statistics']