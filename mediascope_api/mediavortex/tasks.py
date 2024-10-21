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
from ..core import utils


class MediaVortexTask:
    task_urls = {
        'timeband': '/task/timeband',
        'simple': '/task/simple',
        'crosstab': '/task/crosstab',
        'consumption-target': '/task/consumption-target',
        'duplication-timeband': '/task/duplication-timeband',
        'respondent-analysis': '/task/respondent-analysis'
    }

    def __new__(cls, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, check_version: bool = True, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MediaVortexTask, cls).__new__(cls, *args)
        return cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, check_version: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if check_version:
            utils.check_version()
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
                   duplication_time_filter=None, respondent_filter=None, platform_filter=None,
                   playbacktype_filter=None, bigtv_filter=None, slices=None,
                   statistics=None, scales=None, options=None, reach_conditions=None, custom_demo_variable_id=None,
                   custom_company_variable_id=None, custom_respondent_variable_id=None, custom_time_variable_id=None,
                   custom_duplication_time_variable_id=None, custom_duplication_company_variable_id=None,
                   consumption_target_options=None, frequency_dist_conditions=None, sortings=None,
                   add_city_to_basedemo_from_region=False, add_city_to_targetdemo_from_region=False):
        """
        Сформировать задание в JSON формате

        Parameters
        ----------

        task_type : str
            Тип задания, возможные варианты:
            - timeband
            - simple

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : list of tuples
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр временных интервалов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр блоков

        ad_filter : str
            Фильтр роликов

        subject_filter : str
            Фильтр темы

        duration_filter : str
            Фильтр продолжительности

        respondent_filter : str
            Фильтр респондентов

        duplication_company_filter : str
            Фильтр каналов

        duplication_time_filter : str
            Фильтр временных интервалов

        platform_filter : str
            Фильтр платформы (бигтв)

        playbacktype_filter : str
            Фильтр типа плейбека (бигтв)

        bigtv_filter : str
            Фильтр срезов бигтв

        slices : list of str
            Список срезов

        statistics : list of str
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Настройка условий охватов

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

        sortings : dict
            Настройки сортировки: словарь, где ключ - название столбца (тип str), значение - направление сортировки (тип str), например:
            {"researchDate":"ASC", "RtgPer":"DESC"}

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        Returns
        -------
        text : json
            Задание в формате JSON
        """
        if add_city_to_basedemo_from_region:
            basedemo_filter = self._add_city_to_demo_from_region(
                company_filter, basedemo_filter, options.get('kitId'))

        if add_city_to_targetdemo_from_region:
            targetdemo_filter = self._add_city_to_demo_from_region(
                company_filter, targetdemo_filter, options.get('kitId'))

        if not self.task_checker.check_task(
                task_type=task_type, date_filter=date_filter, weekdate_filter=weekday_filter,
                daytype_filter=daytype_filter, company_filter=company_filter, region_filter=region_filter,
                time_filter=time_filter, location_filter=location_filter, basedemo_filter=basedemo_filter,
                targetdemo_filter=targetdemo_filter, program_filter=program_filter, break_filter=break_filter,
                ad_filter=ad_filter, subject_filter=subject_filter, duration_filter=duration_filter,
                duplication_company_filter=duplication_company_filter, duplication_time_filter=duplication_time_filter,
                platform_filter=platform_filter, playbacktype_filter=playbacktype_filter,
                bigtv_filter=bigtv_filter, slices=slices, statistics=statistics, scales=scales,
                sortings=sortings, kit_id=options.get('kitId')):
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
        self.task_builder.add_filter(tsk, respondent_filter, 'respondentFilter')
        self.task_builder.add_filter(tsk, bigtv_filter, 'bigTvFilter')
        self.task_builder.add_filter(tsk, platform_filter, 'platformFilter')
        self.task_builder.add_filter(tsk, playbacktype_filter, 'playBackTypeFilter')

        self.task_builder.add_slices(tsk, slices)
        self.task_builder.add_scales(tsk, scales)

        # Добавляем сортировку
        self.task_builder.add_sortings(tsk, sortings)

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
            'platform_filter': platform_filter,
            'playbacktype_filter': playbacktype_filter,
            'bigtv_filter': bigtv_filter,
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
            'frequencyDistConditions': frequency_dist_conditions,
            "sorting": sortings
        }
        self.task_builder.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)

    def build_timeband_task(self, task_name='', date_filter=None, weekday_filter=None,
                            daytype_filter=None, company_filter=None, region_filter=None, time_filter=None,
                            location_filter=None, basedemo_filter=None, targetdemo_filter=None,
                            respondent_filter=None, platform_filter=None, playbacktype_filter=None,
                            bigtv_filter=None, slices=None,
                            statistics=None, sortings=None, options=None, reach_conditions=None,
                            custom_demo_variable_id=None,
                            custom_company_variable_id=None, custom_respondent_variable_id=None,
                            custom_time_variable_id=None,
                            add_city_to_basedemo_from_region=False, add_city_to_targetdemo_from_region=False):
        """
        Сформировать задание для отчета Timeband в JSON формате

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : list of tuples
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр временных интервалов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        respondent_filter : str
            Фильтр респондентов

        platform_filter : str
            Фильтр платформы (бигтв)

        playbacktype_filter : str
            Фильтр типа плейбека (бигтв)

        bigtv_filter : str
            Фильтр срезов бигтв

        slices : list
            Список срезов

        statistics : list
            Список статистик

        sortings : dict
            Настройки сортировки: словарь, где ключ - название столбца (тип str), значение - направление сортировки (тип str), например:
            {"researchDate":"ASC", "RtgPer":"DESC"}

        options : dict
            Словарь настроек

        reach_conditions : dict
            Настройка условий охватов

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        Returns
        -------
        text : json
            Задание в формате JSON
        """
        return self.build_task(task_type='timeband', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               respondent_filter=respondent_filter,
                               platform_filter=platform_filter, playbacktype_filter=playbacktype_filter,
                               bigtv_filter=bigtv_filter,
                               slices=slices, statistics=statistics, sortings=sortings, options=options,
                               reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                               add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

    def build_simple_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                          company_filter=None, region_filter=None, location_filter=None,
                          basedemo_filter=None, targetdemo_filter=None, program_filter=None, break_filter=None,
                          ad_filter=None, subject_filter=None, respondent_filter=None,
                          platform_filter=None, playbacktype_filter=None,
                          bigtv_filter=None, slices=None, statistics=None, sortings=None, options=None,
                          reach_conditions=None, custom_demo_variable_id=None, custom_company_variable_id=None,
                          custom_time_variable_id=None,
                          custom_respondent_variable_id=None, frequency_dist_conditions=None,
                          add_city_to_basedemo_from_region=False, add_city_to_targetdemo_from_region=False):
        """
        Сформировать задание для отчета Simple в JSON формате

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        region_filter : str
            Фильтр регионов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр блоков

        ad_filter : str
            Фильтр роликов

        subject_filter : str
            Фильтр темы

        respondent_filter : str
            Фильтр респондентов

        platform_filter : str
            Фильтр платформы (бигтв)

        playbacktype_filter : str
            Фильтр типа плейбека (бигтв)

        bigtv_filter : str
            Фильтр срезов бигтв

        slices : list
            Список срезов

        statistics : list
            Список статистик

        sortings : dict
            Настройки сортировки: словарь, где ключ - название столбца (тип str), значение - направление сортировки (тип str), например:
            {"researchDate":"ASC", "RtgPer":"DESC"}

        options : dict
            Словарь настроек

        reach_conditions : dict
            Настройка условий охватов

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

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        Returns
        -------
        text : json
            Задание в формате JSON
        """
        return self.build_task(task_type='simple', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, subject_filter=subject_filter,
                               respondent_filter=respondent_filter,
                               platform_filter=platform_filter, playbacktype_filter=playbacktype_filter,
                               bigtv_filter=bigtv_filter,
                               slices=slices, statistics=statistics, sortings=sortings,
                               options=options, reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               frequency_dist_conditions=frequency_dist_conditions,
                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                               add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

    def build_crosstab_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                            company_filter=None, region_filter=None, location_filter=None,
                            basedemo_filter=None, targetdemo_filter=None, program_filter=None, break_filter=None,
                            ad_filter=None, subject_filter=None, respondent_filter=None,
                            platform_filter=None, playbacktype_filter=None,
                            bigtv_filter=None, slices=None, statistics=None, sortings=None, options=None,
                            reach_conditions=None, custom_demo_variable_id=None, custom_company_variable_id=None,
                            custom_time_variable_id=None, custom_respondent_variable_id=None,
                            frequency_dist_conditions=None,
                            add_city_to_basedemo_from_region=False, add_city_to_targetdemo_from_region=False):
        """
        Сформировать задание для отчета Crosstab в JSON формате

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        region_filter : str
            Фильтр регионов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр блоков

        ad_filter : str
            Фильтр роликов

        subject_filter : str
            Фильтр темы

        respondent_filter : str
            Фильтр респондентов

        platform_filter : str
            Фильтр платформы (бигтв)

        playbacktype_filter : str
            Фильтр типа плейбека (бигтв)

        bigtv_filter : str
            Фильтр срезов бигтв

        slices : list
            Список срезов

        statistics : list
            Список статистик

        sortings : dict
            Настройки сортировки: словарь, где ключ - название столбца (тип str), значение - направление сортировки (тип str), например:
            {"researchDate":"ASC", "RtgPer":"DESC"}

        options : dict
            Словарь настроек

        reach_conditions : dict
            Настройка условий охватов

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

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        Returns
        -------
        text : json
            Задание в формате JSON
        """
        return self.build_task(task_type='crosstab', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter, region_filter=region_filter,
                               location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, subject_filter=subject_filter,
                               respondent_filter=respondent_filter,
                               platform_filter=platform_filter, playbacktype_filter=playbacktype_filter,
                               bigtv_filter=bigtv_filter,
                               slices=slices, statistics=statistics, sortings=sortings,
                               options=options, reach_conditions=reach_conditions,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               frequency_dist_conditions=frequency_dist_conditions,
                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                               add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

    def build_consumption_target_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                                      company_filter=None, region_filter=None, time_filter=None, location_filter=None,
                                      basedemo_filter=None, targetdemo_filter=None, program_filter=None,
                                      break_filter=None, ad_filter=None, subject_filter=None, duration_filter=None,
                                      slices=None, statistics=None, scales=None, options=None, reach_conditions=None,
                                      custom_demo_variable_id=None, custom_company_variable_id=None,
                                      custom_time_variable_id=None,
                                      custom_respondent_variable_id=None, consumption_target_options=None,
                                      add_city_to_basedemo_from_region=False, add_city_to_targetdemo_from_region=False):
        """
        Формирует текст задания consumption target для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        region_filter : str
            Фильтр регионов

        time_filter : str
            Фильтр временных интервалов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр блоков

        ad_filter : str
            Фильтр роликов

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
            Настройка условий охватов

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

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

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
                               consumption_target_options=consumption_target_options,
                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                               add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

    def build_duplication_timeband_task(self, task_name='', date_filter=None, daytype_filter=None, weekday_filter=None,
                                        basedemo_filter=None, targetdemo_filter=None, company_filter=None,
                                        location_filter=None, time_filter=None, duration_filter=None,
                                        duplication_company_filter=None, duplication_time_filter=None, slices=None,
                                        statistics=None, scales=None, options=None, reach_conditions=None,
                                        custom_demo_variable_id=None, custom_time_variable_id=None,
                                        custom_company_variable_id=None, custom_respondent_variable_id=None,
                                        custom_duplication_time_variable_id=None,
                                        custom_duplication_company_variable_id=None,
                                        add_city_to_basedemo_from_region=False,
                                        add_city_to_targetdemo_from_region=False):
        """
        Формирует текст задания duplication_timeband для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр периода

        daytype_filter : str
            Фильтр типов дней

        weekday_filter : str
            Фильтр дней недели

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        company_filter : str
            Фильтр каналов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        time_filter : str
            Фильтр временных интервалов

        duration_filter : str
            Фильтр продолжительности

        duplication_company_filter : str
            Фильтр каналов

        duplication_time_filter : str
            Фильтр временных интервалов

        slices : list
            Список срезов

        statistics : list
            Список статистик

        scales : list
            Список шкал

        options : dict
            Словарь настроек

        reach_conditions : dict
            Настройка условий охватов

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

        add_city_to_basedemo_from_region : bool
            Включение режима автоматического добавления базового демо фильтра по городам на основании фильтра по регионам. По умолчанию false

        add_city_to_targetdemo_from_region : bool
            Включение режима автоматического добавления целевого демо фильтра по городам на основании фильтра по регионам. По умолчанию false

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
                               custom_duplication_company_variable_id=custom_duplication_company_variable_id,
                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                               add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

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

    def wait_task(self, tsk, status_delay=3, task_delay=0.2):
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

        status_delay : int
            Задержка в секундах между опросом статуса. По умолчанию 3 с

        task_delay : int
            Задержка в секундах между опросом статуса каждого задания для списка заданий. По умолчанию 0.2 с

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
                        time.sleep(status_delay)
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
                    time.sleep(status_delay)
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
                    tsk['dtRegister'] = task_state_obj.get('dtRegister', '')
                    tsk['dtFinish'] = task_state_obj.get('dtFinish', '')
                    tsk['taskProcessingTimeSec'] = task_state_obj.get('taskProcessingTimeSec', '')
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
                time.sleep(status_delay)
                # запросим состояние
                done_count = 0
                for t in task_list:
                    tid = t['task']['taskId']
                    task_state = ''
                    task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tid))
                    time.sleep(task_delay)
                    if task_state_obj is not None:
                        task_state = task_state_obj.get('taskStatus', '')

                    if task_state == 'IN_PROGRESS' \
                            or task_state == 'PENDING' \
                            or task_state == 'IN_QUEUE' \
                            or task_state == 'IDLE':
                        continue
                    elif task_state == 'DONE':
                        t['task']['message'] = 'DONE'
                        t['task']['dtRegister'] = task_state_obj.get('dtRegister', '')
                        t['task']['dtFinish'] = task_state_obj.get('dtFinish', '')
                        t['task']['taskProcessingTimeSec'] = task_state_obj.get('taskProcessingTimeSec', '')
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
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
                    'dtFinish': '2024-09-30 12:17:54',
                    'taskProcessingTimeSec': 21
                }
        """
        if tsk.get('taskId') is not None:
            tid = tsk.get('taskId', None)
            task_state_obj = self.network_module.send_request('get', '/task/state/{}'.format(tid))
            return task_state_obj

    def get_statuses(self, tsk_ids: list):
        """
        Получить статус расчета заданий.

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
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
                    'dtFinish': '2024-09-30 12:17:54',
                    'taskProcessingTimeSec': 21
                }
        """
        post_data = {
            "taskIds": tsk_ids
        }

        task_state_obj = self.network_module.send_request('post', '/task/state', json.dumps(post_data))
        return task_state_obj.get('data')

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
                    'taskStatus': 'IN_QUEUE',
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
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
                    'taskStatus': 'IN_QUEUE',
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
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
                    'taskStatus': 'CANCELLED',
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
                    'dtFinish': '2024-09-30 12:17:54',
                    'taskProcessingTimeSec': 21
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
                    'taskStatus': 'CANCELLED',
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33',
                    'dtFinish': '2024-09-30 12:17:54',
                    'taskProcessingTimeSec': 21
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

    def result2table(self, data, project_name=None, time_separator=True, to_lists=False):
        """
        Преобразовать результат выполнения задания из JSON в DataFrame

        Parameters
        ----------

        data : dict
            Результат выполнения задания в JSON формате

        project_name : str
            Название проекта

        time_separator : bool, default True
            Настройка формата атрибутов времени (время начала, время окончания). 
            True: значения выгружаются в виде строки ЧЧ:ММ:СС; False: в формате int без разделителей. 

        to_lists : bool, default False
            Объединить несколько значений по одному выходу ролика в список

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

        if to_lists == True:
            df_merged = self.merge_rows(df)
            return df_merged

        else:
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
                df[col] = df[col].astype(str, errors='ignore')
                _attrs['valueId'] = _attrs['valueId'].astype(str, errors='ignore')
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

    def merge_rows(self, df: pd.DataFrame):
        """
        Объединить разные значения по атрибутам для одного adSpotId выхода 

        Parameters
        ----------

        df : pd.DataFrame
            Результат выполнения задания в формате DataFrame

        Returns
        -------
        result : DataFrame
            DataFrame, где для каждого adSpotId все различающиеся атрибуты объединены в списки
        """
        if type(df) != pd.DataFrame:
            return

        if "adSpotId" not in df.columns:
            return df

        x = df.copy()  # создаем копию df
        x = x.groupby("adSpotId").agg("first")  # удаляем дубликаты по adSpotId, оставляем одно первое значение

        for col in x.columns:
            if col in [
                'advertiserId',
                'advertiserName',
                'advertiserEName',
                'advertiserNotes',
                'brandId',
                'brandName',
                'brandEName',
                'subbrandId',
                'subbrandName',
                'subbrandEName',
                'modelId',
                'modelName',
                'modelEName',
                'articleLevel1Id',
                'articleLevel1Name',
                'articleLevel1EName',
                'articleLevel2Id',
                'articleLevel2Name',
                'articleLevel2EName',
                'articleLevel3Id',
                'articleLevel3Name',
                'articleLevel3EName',
                'articleLevel4Id',
                'articleLevel4Name',
                'articleLevel4EName',
                'advertiserTvAreaId',
                'advertiserTvAreaName',
                'advertiserTvAreaEName',
                'brandTvAreaId',
                'brandTvAreaName',
                'brandTvAreaEName',
                'subbrandTvAreaId',
                'subbrandTvAreaName',
                'subbrandTvAreaEName',
                'modelTvAreaId',
                'modelTvAreaName',
                'modelTvAreaEName'
            ]:
                y = df.copy()
                y = y[['adSpotId', col]]  # делаем срез из исходного df
                y.sort_values(by=['adSpotId', col], inplace=True)  # сортируем
                y.drop_duplicates(['adSpotId', col], keep='first', inplace=True)  # удаляем дубликаты
                y = y.groupby("adSpotId").agg(
                    {col: "; ".join})  # все осташиеся уникальные для id выхода значения объединяем в одну ячейку
                x.update(y)  # обновляем исходный df
        x.reset_index(inplace=True)
        return x

    def send_respondent_analysis_task(self, data):
        """
        Отправить задание respondent analysis

        Parameters
        ----------

        data : str
            Текст задания в JSON формате


        Returns
        -------
        text : json
            Ответ сервера, содержит taskid, который необходим для получения результата

        """
        return self._send_task('respondent-analysis', data)

    def build_respondent_analysis_task(self, task_name='', date_filter=None, weekday_filter=None, daytype_filter=None,
                                       company_filter=None, basedemo_filter=None, targetdemo_filter=None,
                                       program_filter=None, time_filter=None, location_filter=None,
                                       respondent_filter=None, break_filter=None, ad_filter=None, duration_filter=None,
                                       slices=None, statistics=None, scales=None, options=None, reach_conditions=None,
                                       custom_demo_variable_id=None, custom_company_variable_id=None,
                                       custom_time_variable_id=None, custom_respondent_variable_id=None,
                                       sortings=None):
        """
        Формирует текст задания respondent analysis для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : str
            Фильтр периода

        weekday_filter : str
            Фильтр дней недели

        daytype_filter : str
            Фильтр типов дней

        company_filter : str
            Фильтр каналов

        time_filter : str
            Фильтр временных интервалов

        location_filter : str
            Фильтр места просмотра (дом/дача)

        respondent_filter : str
            Фильтр респондентов

        basedemo_filter : str
            Фильтр базовой аудитории

        targetdemo_filter : str
            Фильтр целевой аудитории

        program_filter : str
            Фильтр программ

        break_filter : str
            Фильтр блоков

        ad_filter : str
            Фильтр роликов

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
            Настройка условий охватов

        custom_demo_variable_id : str
            Id кастомной demo переменной

        custom_company_variable_id : str
            Id кастомной company переменной

        custom_respondent_variable_id : str
            Id кастомной respondent переменной

        custom_time_variable_id : str
            Id кастомной time переменной

        sortings : dict
            Настройки сортировки: словарь, где ключ - название столбца (тип str), значение - направление сортировки (тип str), например:
            {"researchDate":"ASC", "RtgPer":"DESC"}

        Returns
        -------
        text : json
            Задание в формате MediaVortex API
        """
        return self.build_task(task_type='respondent-analysis', task_name=task_name, date_filter=date_filter,
                               weekday_filter=weekday_filter, daytype_filter=daytype_filter,
                               company_filter=company_filter,
                               time_filter=time_filter, location_filter=location_filter,
                               basedemo_filter=basedemo_filter, targetdemo_filter=targetdemo_filter,
                               program_filter=program_filter, break_filter=break_filter,
                               ad_filter=ad_filter, respondent_filter=respondent_filter,
                               duration_filter=duration_filter, slices=slices, statistics=statistics,
                               scales=scales, options=options, reach_conditions=reach_conditions,
                               custom_demo_variable_id=custom_demo_variable_id,
                               custom_company_variable_id=custom_company_variable_id,
                               custom_respondent_variable_id=custom_respondent_variable_id,
                               custom_time_variable_id=custom_time_variable_id,
                               sortings=sortings)

    def _add_city_to_demo_from_region(self, company_filter=None, demo_filter=None, kit_id=None):
        # автоматическое добавление фильтра городов по значениям региона

        # если фильтр по регионам не задан, то возвращаем демо без изменений
        if company_filter is None:
            return demo_filter

        # Собираем JSON
        tsk = {
            "filter": {}
        }
        self.task_builder.add_filter(tsk, company_filter, 'companyFilter')

        for element in tsk['filter']['companyFilter']['elements']:
            if element['unit'] == 'regionId':
                region_value = element['value']
                if not type(element['value']) is list:
                    region_value = [element['value']]

                city_ids = self.cats.get_tv_monitoring_cities(region_id=region_value,
                                                              kit_id=kit_id,
                                                              return_city_ids_as_string=True,
                                                              show_header=False)
                if city_ids:
                    relation_element = 'IN'
                    if element['relation'] == 'NIN':
                        relation_element = element['relation']

                    if demo_filter is None:
                        return 'city ' + relation_element + ' (' + city_ids + ')'
                    else:
                        if 'city' not in demo_filter:
                            return demo_filter + ' AND city ' + relation_element + ' (' + city_ids + ')'
                else:
                    return demo_filter
