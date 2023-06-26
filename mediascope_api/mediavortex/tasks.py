import datetime as dt
import json
import numpy as np
import pandas as pd
import time
from pandas import DataFrame
from . import catalogs
from . import checks
from ..core import errors
from ..core import net
from ..core import tasks


class MediaVortexTask:
    task_urls = {
        'timeband': '/task/timeband',
        'simple': '/task/simple',
        'crosstab': '/task/crosstab',
        'consumption-target': '/task/consumption-target',
        'duplication-timeband': '/task/duplication-timeband'
    }

    def __new__(cls, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MediaVortexTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.network_module = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                       root_url, client_id, client_secret, keycloak_url)
        self.task_builder = tasks.TaskBuilder()
        self.cats = catalogs.MediaVortexCats(0, settings_filename, cache_path, cache_enabled, username, passw,
                                             root_url, client_id, client_secret, keycloak_url)
        self.task_checker = checks.MediaVortexTaskChecker(self.cats)

    def build_task(self, task_type, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                   company_filter=None, region_filter=None, time_filter=None, location_filter=None,
                   basedemo_filter=None, targetdemo_filter=None, program_filter=None, break_filter=None,
                   ad_filter=None, subject_filter=None, duration_filter=None, duplication_company_filter=None,
                   duplication_time_filter=None, respondent_filter=None, slices=None,
                   statistics=None, scales=None, options=None, reach_conditions=None, custom_demo_variable_id=None,
                   custom_company_variable_id=None, custom_respondent_variable_id=None, custom_time_variable_id=None,
                   custom_duplication_time_variable_id=None, custom_duplication_company_variable_id=None,
                   consumption_target_options=None, frequency_dist_conditions=None):
        """
        Формирует текст задания для расчета статистик

        Parameters
        ----------

        task_type : str
            Тип задания, возможные варианты:
            - media

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр компаний

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр времени

        location_filter : str
            Фильтр локации

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр перерывов

        ad_filter : str
            Фильтр рекламы

        subject_filter : str
            Фильтр темы

        duration_filter : str
            Фильтр продолжительности

        respondent_filter : str
            Фильтр респондентов

        duplication_company_filter : str
            Фильтр компаний

        duplication_time_filter : str
            Фильтр времени

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        custom_duplication_time_variable_id : str
            Id кастомной time переменной

        custom_duplication_company_variable_id : str
            Id кастомной company переменной

        consumption_target_options : dict
            Словарь условий consumption_target

        frequency_dist_conditions : dict
            Словарь условия для FrequencyDist статистик

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """

        if not self.task_checker.check_task(task_type, date_filter, weekday_filter, daytype_filter,
                                            company_filter, region_filter, time_filter, location_filter,
                                            basedemo_filter, targetdemo_filter, program_filter, break_filter,
                                            ad_filter, subject_filter, duration_filter, duplication_company_filter,
                                            duplication_time_filter, slices, statistics, scales):
            return

        # Собираем JSON
        tsk = {
            "task_type": task_type,
            "statistics": statistics,
            "filter": {}
        }
        # Добавляем фильтры
        self.task_builder.add_range_filter(tsk, date_filter)
        self.task_builder.add_filter(tsk, weekday_filter, 'weekDayFilter')
        self.task_builder.add_filter(tsk, daytype_filter, 'dayTypeFilter')
        self.task_builder.add_filter(tsk, company_filter, 'companyFilter')
        self.task_builder.add_filter(tsk, region_filter, 'regionFilter')
        self.task_builder.add_filter(tsk, time_filter, 'timeFilter')
        self.task_builder.add_filter(tsk, location_filter, 'locationFilter')
        self.task_builder.add_filter(tsk, basedemo_filter, 'baseDemoFilter')
        self.task_builder.add_filter(tsk, targetdemo_filter, 'targetDemoFilter')
        self.task_builder.add_filter(tsk, program_filter, 'programFilter')
        self.task_builder.add_filter(tsk, break_filter, 'breakFilter')
        self.task_builder.add_filter(tsk, ad_filter, 'adFilter')
        self.task_builder.add_filter(tsk, subject_filter, 'subjectFilter')
        self.task_builder.add_filter(tsk, duration_filter, 'durationFilter')
        self.task_builder.add_filter(tsk, duplication_company_filter, 'duplicationCompanyFilter')
        self.task_builder.add_filter(tsk, duplication_time_filter, 'duplicationTimeFilter')
        if respondent_filter is not None:
            resp_filter = {
                "operand": "AND",
                "elements": [
                    {
                        "unit": 'respondentId',
                        "relation": "IN",
                        "value": respondent_filter
                    }
                ]
            }
            tsk['filter']['respondentFilter'] = resp_filter
        self.task_builder.add_slices(tsk, slices)
        self.task_builder.add_scales(tsk, scales)

        # добавляем Опции таска
        if options is not None:
            tsk['options'] = options

        # добавляем reach conditions таска
        if reach_conditions is not None:
            tsk['reachConditions'] = reach_conditions

        # добавляем блок consumption target таска
        if custom_demo_variable_id is not None:
            tsk["customDemoVariableId"] = custom_demo_variable_id

        if custom_company_variable_id is not None:
            tsk["customCompanyVariableId"] = custom_company_variable_id

        if custom_respondent_variable_id is not None:
            tsk["customRespondentVariableId"] = custom_respondent_variable_id

        if consumption_target_options is not None:
            tsk["consumptionTargetOptions"] = consumption_target_options

        if frequency_dist_conditions is not None:
            tsk["frequencyDistConditions"] = frequency_dist_conditions

        # add duplication-timeband params
        if custom_time_variable_id is not None:
            tsk["customTimeVariableId"] = custom_time_variable_id

        if custom_duplication_time_variable_id is not None:
            tsk["customDuplicationTimeVariableId"] = custom_duplication_time_variable_id

        if custom_duplication_company_variable_id is not None:
            tsk["customDuplicationCompanyVariableId"] = custom_duplication_company_variable_id

        if not self.task_checker.check_units_in_task(task_type, tsk):
            return

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        tinfo = {
            'task_name': task_name,
            'task_type': task_type,
            'date_filter': date_filter,
            'weekday_filter': weekday_filter,
            'daytype_filter': daytype_filter,
            'company_filter': company_filter,
            'region_filter': region_filter,
            'time_filter': time_filter,
            'location_filter': location_filter,
            'basedemo_filter': basedemo_filter,
            'targetdemo_filter': targetdemo_filter,
            'program_filter': program_filter,
            'break_filter': break_filter,
            'ad_filter': ad_filter,
            'subject_filter': subject_filter,
            'duration_filter': duration_filter,
            'respondent_filter': respondent_filter,
            'duplication_company_filter': duplication_company_filter,
            'duplication_time_filter': duplication_time_filter,
            'slices': slices,
            'statistics': statistics,
            'scales': scales,
            'options': options,
            'reachConditions': reach_conditions,
            'customDemoVariableId': custom_demo_variable_id,
            'customCompanyVariableId': custom_company_variable_id,
            'customRespondentVariableId': custom_respondent_variable_id,
            'customTimeVariableId': custom_time_variable_id,
            'customDuplicationTimeVariableId': custom_duplication_time_variable_id,
            'customDuplicationCompanyVariableId': custom_duplication_company_variable_id,
            'consumptionTargetOptions': consumption_target_options,
            'frequencyDistConditions': frequency_dist_conditions
        }
        self.task_builder.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)

    def build_timeband_task(self, task_name='', date_filter=None, weekday_filter=None,
                            daytype_filter=None, company_filter=None, region_filter=None, time_filter=None,
                            location_filter=None, basedemo_filter=None, targetdemo_filter=None,
                            respondent_filter=None, slices=None,
                            statistics=None, scales=None, options=None, reach_conditions=None,
                            custom_demo_variable_id=None,
                            custom_company_variable_id=None, custom_respondent_variable_id=None,
                            custom_time_variable_id=None):
        """
        Формирует текст задания timeband для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр компаний

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр времени

        location_filter : str
            Фильтр локации

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        respondent_filter : str
            Фильтр респондентов

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='timeband', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               respondent_filter=respondent_filter,
                               slices=slices, statistics=statistics, scales=scales, options=options,
                               reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id)

    def build_simple_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                          company_filter=None, region_filter=None, time_filter=None, location_filter=None,
                          basedemo_filter=None, targetdemo_filter=None, program_filter=None, break_filter=None,
                          ad_filter=None, subject_filter=None, duration_filter=None, respondent_filter=None,
                          slices=None, statistics=None, scales=None, options=None, reach_conditions=None,
                          custom_demo_variable_id=None, custom_company_variable_id=None,
                          custom_time_variable_id=None,
                          custom_respondent_variable_id=None, frequency_dist_conditions=None):
        """
        Формирует текст задания simple для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр компаний

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр времени

        location_filter : str
            Фильтр локации

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр перерывов

        ad_filter : str
            Фильтр рекламы

        subject_filter : str
            Фильтр темы

        duration_filter : str
            Фильтр продолжительности

        respondent_filter : str
            Фильтр респондентов

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        frequency_dist_conditions : dict
            Словарь условия для FrequencyDist статистик

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='simple', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, subject_filter=subject_filter,
                               duration_filter=duration_filter, respondent_filter=respondent_filter,
                               slices=slices, statistics=statistics,
                               scales=scales, options=options, reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               frequency_dist_conditions=frequency_dist_conditions)

    def build_crosstab_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                            company_filter=None, region_filter=None, time_filter=None, location_filter=None,
                            basedemo_filter=None, targetdemo_filter=None, program_filter=None, break_filter=None,
                            ad_filter=None, subject_filter=None, duration_filter=None, respondent_filter=None,
                            slices=None, statistics=None, scales=None, options=None, reach_conditions=None,
                            custom_demo_variable_id=None, custom_company_variable_id=None,
                            custom_time_variable_id=None,
                            custom_respondent_variable_id=None, frequency_dist_conditions=None):
        """
        Формирует текст задания crosstab для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр компаний

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр времени

        location_filter : str
            Фильтр локации

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр перерывов

        ad_filter : str
            Фильтр рекламы

        subject_filter : str
            Фильтр темы

        duration_filter : str
            Фильтр продолжительности

        respondent_filter : str
            Фильтр респондентов

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        frequency_dist_conditions : dict
            Словарь условия для FrequencyDist статистик

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='crosstab', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, subject_filter=subject_filter,
                               duration_filter=duration_filter, respondent_filter=respondent_filter,
                               slices=slices, statistics=statistics,
                               scales=scales, options=options, reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               frequency_dist_conditions=frequency_dist_conditions)

    def build_consumption_target_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                                      company_filter=None, region_filter=None, time_filter=None, location_filter=None,
                                      basedemo_filter=None, targetdemo_filter=None, program_filter=None,
                                      break_filter=None, ad_filter=None, subject_filter=None, duration_filter=None,
                                      slices=None, statistics=None, scales=None, options=None, reach_conditions=None,
                                      custom_demo_variable_id=None, custom_company_variable_id=None,
                                      custom_time_variable_id=None,
                                      custom_respondent_variable_id=None, consumption_target_options=None):
        """
        Формирует текст задания consumption target для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр компаний

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр времени

        location_filter : str
            Фильтр локации

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр перерывов

        ad_filter : str
            Фильтр рекламы

        subject_filter : str
            Фильтр темы

        duration_filter : str
            Фильтр продолжительности

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        consumption_target_options : dict
            Словарь условий consumption_target

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='consumption-target', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, subject_filter=subject_filter,
                               duration_filter=duration_filter, slices=slices, statistics=statistics,
                               scales=scales, options=options, reach_conditions=reach_conditions,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               consumption_target_options=consumption_target_options)

    def build_duplication_timeband_task(self, task_name='', date_filter=None, daytype_filter=None, weekday_filter=None,
                                        basedemo_filter=None, targetdemo_filter=None, company_filter=None,
                                        location_filter=None, time_filter=None, duration_filter=None,
                                        duplication_company_filter=None, duplication_time_filter=None, slices=None,
                                        statistics=None, scales=None, options=None, reach_conditions=None,
                                        custom_demo_variable_id=None, custom_time_variable_id=None,
                                        custom_company_variable_id=None, custom_respondent_variable_id=None,
                                        custom_duplication_time_variable_id=None,
                                        custom_duplication_company_variable_id=None):
        """
        Формирует текст задания duplication_timeband для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр дат

        daytype_filter : str
            Фильтр типов дней

        weekday_filter : str
            Фильтр дней недели

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        company_filter : str
            Фильтр компаний

        location_filter : str
            Фильтр локации

        time_filter : str
            Фильтр времени

        duration_filter : str
            Фильтр продолжительности

        duplication_company_filter : str
            Фильтр компаний

        duplication_time_filter : str
            Фильтр времени

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Словарь условий reach

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_duplication_time_variable_id : str
            Id кастомной time переменной

        custom_duplication_company_variable_id : str
            Id кастомной company переменной

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='duplication-timeband', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               duration_filter=duration_filter, duplication_company_filter=duplication_company_filter,
                               duplication_time_filter=duplication_time_filter,
                               slices=slices, statistics=statistics, scales=scales, options=options,
                               reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_duplication_time_variable_id=custom_duplication_time_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               custom_duplication_company_variable_id=custom_duplication_company_variable_id)

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
            print(f'Не верно указать тип задания, допустимые значения: {" ,".join(self.task_urls.keys())}')
            return
        return self._send_task(task_type, data)

    def send_timeband_task(self, data):
        """
        Отправить задание timeband

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('timeband', data)

    def send_simple_task(self, data):
        """
        Отправить задание simple

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('simple', data)

    def send_crosstab_task(self, data):
        """
        Отправить задание crosstab

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('crosstab', data)

    def send_consumption_target_task(self, data):
        """
        Отправить задание consumption target

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('consumption-target', data)

    def send_duplication_timeband_task(self, data):
        """
        Отправить задание duplication timeband

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('duplication-timeband', data)

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
        errs = dict()
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
                if task_state == 'FAILED':
                    print(f"Задача завершилась с ошибкой: {task_state_obj.get('message', '')}")
                time.sleep(1)
                e = dt.datetime.now()
                print(f"] время расчета: {str(e - s)}")
                if task_state == 'DONE':
                    tsk['message'] = 'DONE'
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

                    if task_state == 'IN_PROGRESS' \
                            or task_state == 'PENDING' \
                            or task_state == 'IN_QUEUE' \
                            or task_state == 'IDLE':
                        continue
                    elif task_state == 'DONE':
                        t['task']['message'] = 'DONE'
                        done_count += 1
                    else:
                        errs[tid] = t
                        errs[tid]['state'] = task_state
                        errs[tid]['message'] = task_state_obj.get('message', '')
                        done_count += 1
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

    def restart_task(self, tsk: dict):
        """
        Перезапустить задание.

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
            task_state_obj = self.network_module.send_request('get', '/task/state/restart/{}'.format(tid))
            return task_state_obj

    def restart_tasks(self, tsk_ids: list):
        """
        Перезапустить задания.

        Parameters
        ----------

        tsk_ids : list
            Список taskId заданий

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
        post_data = {
            "taskIds": tsk_ids
        }

        task_state_obj = self.network_module.send_request('post', '/task/state/restart', json.dumps(post_data))
        return task_state_obj

    def cancel_task(self, tsk: dict):
        """
        Отменить задание.

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
            task_state_obj = self.network_module.send_request('get', '/task/state/cancel/{}'.format(tid))
            return task_state_obj

    def cancel_tasks(self, tsk_ids: list):
        """
        Отменить задания.

        Parameters
        ----------

        tsk_ids : list
            Список taskId заданий

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
        post_data = {
            "taskIds": tsk_ids
        }

        task_state_obj = self.network_module.send_request('post', '/task/state/cancel', json.dumps(post_data))
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

    def result2table(self, data, project_name=None, time_separator=True):
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
                    res[k] = []

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
                res[k].append(v)

        df = pd.DataFrame(res)
        self._get_text_names(df, time_separator=time_separator)
        df.replace(to_replace=[None], value=np.nan, inplace=True)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        # df['date'] = pd.to_datetime(df['date'])
        return df

    def _get_text_names(self, df, with_id=False, time_separator=True):
        df = self._get_text_name_for(df, with_id)
        if time_separator:
            df = self._get_time_separator_name_for(df)
        df = self._get_text_name_for_weekday(df)
        return df

    def _get_text_name_for(self, df: pd.DataFrame, with_id=True):
        if type(df) != pd.DataFrame:
            return
        id_name = ''
        if with_id:
            id_name = 'Name'

        for col in df.columns:
            if col not in self.cats.tv_demo_attribs['entityName'].unique().tolist():
                continue
            _attrs = self.cats.tv_demo_attribs[
                self.cats.tv_demo_attribs['entityName'] == col
                ].copy()[['valueId', 'valueName']]
            if not _attrs.empty:
                df[col] = df[col].astype('int32', errors='ignore')
                _attrs['valueId'] = _attrs['valueId'].astype('int32', errors='ignore')
                df[col + id_name] = df.merge(_attrs, how='left', left_on=col, right_on='valueId')['valueName']
        return df

    # добавление разделителей в строковое поле с временем
    def _get_time_separator_name_for(self, df: pd.DataFrame):
        if type(df) != pd.DataFrame:
            return

        for col in df.columns:
            if col == 'programStartTime' or col == 'programFinishTime' or \
                    col == 'breaksStartTime' or col == 'breaksFinishTime' or \
                    col == 'adStartTime' or col == 'adFinishTime':
                df[col] = df[col].str[0:-4] + ':' + df[col].str[-4:-2] + ':' + df[col].str[-2:]

        return df

        # замена ид дня недели на текст

    def _get_text_name_for_weekday(self, df: pd.DataFrame):
        if type(df) != pd.DataFrame:
            return

        di = {
            "1": "Понедельник",
            "2": "Вторник",
            "3": "Среда",
            "4": "Четверг",
            "5": "Пятница",
            "6": "Суббота",
            "7": "Воскресенье"
        }

        for col in df.columns:
            if col == 'researchWeekDay':
                df[col] = df[col].map(di)

        return df
