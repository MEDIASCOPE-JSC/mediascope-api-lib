import json
import pandas as pd
import numpy as np
import pendulum
from ..core import net
from ..core import tasks
from ..core import errors


class CounterTask:
    device_types = {"ALL": None,
                    "MOBILE": "deviceTypeName in ('MOBILE', 'TABLET', 'SMARTPHONE', 'IOS', 'ANDROID',  'SMALLSCREEN', " \
                              + "'WINDOWS_MOBILE')",
                    "DESKTOP": "deviceTypeName = 'DESKTOP'",
                    "OTHER": "deviceTypeName nin ('DESKTOP', 'MOBILE', 'TABLET', 'SMARTPHONE', 'IOS', 'ANDROID', " \
                             + "'SMALLSCREEN', 'WINDOWS_MOBILE')"
                    }

    geo_types = {"W": None,
                 "R": "countryName = 'РОССИЯ'",
                 "M": "cityName='Москва'",
                 "P": "cityName='Санкт-Петербург'",
                 "E": "cityName='Екатеринбург'",
                 "N": "cityName='Новосибирск'",
                 "K": "countryName = 'KZ'",
                 "Z": "cityName='Алматы'"
                 }

    area_type_filter = ["audience", "advertisingCampaign", "advertisingNetwork"]
    
    def __new__(cls, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.task_builder = tasks.TaskBuilder()

    def build_task(self, task_name: str = '', date_filter: list = None, area_type_filter: list = None,
                   partner_filter: list = None, tmsec_filter: list = None, geo_filter: str = None,
                   device_type_filter: list = None, slices: list = None, statistics: list = None,
                   sampling: int = 42) -> dict:
        """
        Формирует текст задания для расчета статистик

        Parameters
        ----------

        task_name : str
            Название задания, если не задано - формируется как: пользователь + типа задания + дата/время

        date_filter : list
            Список периодов, период задается списком пар - (начало, конец):
            Пример:
                date_filter = [
                               ('2021-07-05', '2021-07-18'),
                               ('2021-09-06', '2021-09-26'),
                               ('2021-10-18', '2021-10-31')
                              ]

        area_type_filter : list
            Тип размещения счетчика, определяет область обрабатываемых данных и позволяет сократить время расчета:
                audience - В отбор попадают данные по Интернет площадкам
                advertisingCampaign - тегированные рекламные кампании
                advertisingNetwork - рекламные сети

            Пример:
                area_type_filter = ["advertisingCampaign", "advertisingNetwork"]
                area_type_filter = "audience"

        partner_filter : str|None
            Фильтр по имени партнера (он же account_name)
            Пример:
                partner_filter = ["anysite_ru"]
                partner_filter = ["newsite_com", "oldsite_net"]


        tmsec_filter : str|None
            Фильтр по tmsec
            Пример:
                tmsec_filter = ["newsite_total"]
                tmsec_filter ["newsite_total", "oldsite_news"]


        geo_filter: str|None
            Условия фильтрации по географии
            Пример:
                geo_filter = "countryName ='RU'
                geo_filter = "provinceName = 'Москва'"
                geo_filter = "cityName IN ('Москва', 'Тверь', 'Санкт-Петербург')"

        device_type_filter: list|None
            Условия фильтрации по типам устройств
            Пример:
                device_type_filter = ['DESKTOP']
                device_type_filter = ['DESKTOP', 'SMARTPHONE']

        slices: list
            Порядок разбивки результата расчета, задается в виде списка,
            возможные значения:
            - "researchDate" - дата
            - "countryName" - страна
            - "provinceName" - регион
            - "cityName" - город
            - "partnerName" - партнер (account_name)
            - "tmsec" - tmsec (тематический раздел, идентификатор РК, ...)
            - "deviceTypeName" - тип устройства
            - "areaType" - тип размещени счетчика
            Пример:
                slices = ["researchDate", "tmsec"]

        statistics : list
            Список статистик, которые необходимо рассчитать,
            возможные значения:
            - hitsVisits - хиты
            - uniqsVisits - уникальные посетители
            Пример:
                statistics = ['hitsVisits', 'uniqsVisits']
                statistics = ['hitsVisits']

        sampling : int|None
            Сэмпл данных используемых для расчета.
            Задается в процентах от 1 до 100.
            Значение по умолчанию = 42 (Answer to the Ultimate Question of Life, the Universe, and Everything).
            Сэмплирование позволяет выполнять запросы приближённо на основе указанного процента данных.
            К примеру, если указать значение 10, то выполнится фильтрация, после чего возьмётся 10% отсортированных
            данных и на их основе посчитается статистика.
        Returns
        -------
        text : json
            Результат расчета
        """

        # Собираем JSON
        tsk = {
            "statistics": statistics,
            "filter": {}
        }
        # Добавляем фильтры
        self.task_builder.add_range_filter(tsk, date_filter)
        self.task_builder.add_list_filter(tsk, 'areaTypeFilter', 'areaType', area_type_filter)
        self.task_builder.add_list_filter(tsk, 'partnerNameFilter', 'partnerName', partner_filter)
        self.task_builder.add_list_filter(tsk, 'tmsecFilter', 'tmsec', tmsec_filter)
        self.task_builder.add_filter(tsk, device_type_filter, 'deviceTypeFilter')
        self.task_builder.add_filter(tsk, geo_filter, 'geoFilter')

        self.task_builder.add_slices(tsk, slices)
        self.task_builder.add_sampling(tsk, sampling)

        # Сохраняем информацию о задании, для последующего сохранения в Excel
        tinfo = {
            'task_name': task_name,
            'date_filter': date_filter,
            'area_type_filter': area_type_filter,
            'partner_filter': partner_filter,
            'tmsec_filter': tmsec_filter,
            'geo_filter': geo_filter,
            'device_type_filter': device_type_filter,
            'slices': slices,
            'statistics': statistics,
            'sampling': sampling
        }
        self.task_builder.save_report_info(tinfo)
        # Возвращаем JSON
        return json.dumps(tsk)
        #return tsk

    def send_task(self, task: dict):
        """
        Отправить задание на расчет

        Parameters
        ----------

        task : str
            Текст задания в JSON формате

        Returns
        -------
        text : json
            Ответ сервера с результатом расчета

        """
        if task is None:
            print('Задание пустое')
            return None
        try:
            return self.msapi_network.send_request('post', '/daily-task', task)
        except errors.HTTP400Error as e:
            print(f"Ошибка: {e}")

    @staticmethod
    def result2table(data: dict, project_name: str = None):
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
                    res['stat.' + k] = []

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
                res['stat.' + k].append(v)

        df = pd.DataFrame(res)
        df.replace(to_replace=[None], value=np.nan, inplace=True)
        if project_name is not None:
            df.insert(0, 'prj_name', project_name)
        return df

    def get_tmsecs(self, date_filter: list = None, partners: list = None):
        """
        Получить tmsecs в требуемый диапазон дней

        Parameters
        ----------
        date_filter : list
            Поиск по диапазону дней (если день не указан, то ищем по вчерашнему дню)
        
        partners : list
            Поиск по списку партнеров

        Returns
        -------
        tmsecs : list
        
            Список с tmsecs
        """

        if date_filter is None:            
            # если день не указан, то берем вчерашний
            bday = pendulum.from_format(pendulum.now('Europe/Moscow').format('YYYY-MM-DD'), 'YYYY-MM-DD').add(days=-1)
            eday = bday
            date_filter = [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))] 
                
        partner_filter = partners
        project_name = f"{partners} tmsecs"        
        
        task_json = self.build_task(task_name=project_name,
                                         date_filter=date_filter,
                                         area_type_filter=self.area_type_filter,
                                         partner_filter=partner_filter,
                                         tmsec_filter=None,
                                         geo_filter=None,
                                         device_type_filter=None,
                                         slices=["tmsec"],
                                         statistics=["hitsVisits"],
                                         sampling=10) 
        
        df_tmsecs = self.result2table(self.send_task(task_json), project_name)       
        
        if df_tmsecs is not None:
            if len(df_tmsecs):
                if 'stat.hitsVisits' in df_tmsecs.columns and 'tmsec' in df_tmsecs.columns:
                    return df_tmsecs.sort_values(by=['stat.hitsVisits'], ascending=False)['tmsec'].to_list()
        
        return f"Нет данных за {eday.format('YYYY-MM-DD')}"
    
    def get_partners(self, date_filter: list = None):
        """
        Получить партнеров в требуемый диапазон дней

        Parameters
        ----------
        date_filter : list
            Поиск по диапазону дней (если день не указан, то ищем по вчерашнему дню)

        Returns
        -------
        partners : list
        
            Список партнеров
        """

        if date_filter is None:            
            # если день не указан, то берем вчерашний
            bday = pendulum.from_format(pendulum.now('Europe/Moscow').format('YYYY-MM-DD'), 'YYYY-MM-DD').add(days=-1)
            eday = bday
            date_filter = [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))]
            
        project_name = "Clients"        
        
        task_json = self.build_task(task_name=project_name,
                                         date_filter=date_filter,
                                         area_type_filter=self.area_type_filter,
                                         partner_filter=None,
                                         tmsec_filter=None,
                                         geo_filter=None,
                                         device_type_filter=None,
                                         slices=["partnerName"],
                                         statistics=["hitsVisits"],
                                         sampling=10) 
        
        df_tmsecs = self.result2table(self.send_task(task_json), project_name)       
        
        if df_tmsecs is not None:
            if len(df_tmsecs):
                if 'stat.hitsVisits' in df_tmsecs.columns and 'partnerName' in df_tmsecs.columns:
                    return df_tmsecs.sort_values(by=['stat.hitsVisits'], ascending=False)['partnerName'].to_list()
        
        return f"Нет данных за {eday.format('YYYY-MM-DD')}"
    
    
    def get_date_range(self, day: str = None, range_type: str = 'D'):
        """
        Получить нужный период времени (предыдущий день, последнюю закрытую неделю и месяц)

        Parameters
        ----------
        day : str
            Базовый день в формате 'YYYY-MM-DD' (если день не указан, то берем текущий день)
        
        range_type : str
            Требуемый диапазон ('D'- предыдущий день, 'W' - последняя закрытая неделя, 'M' - последний закрытй месяц)

        Returns
        -------
        date_list : list
        
            Диапазон дат, соответствующих нужному периоду времени
        """
        
        if day is None:            
            # если день не указан, то берем текущий
            day = pendulum.now('Europe/Moscow').format('YYYY-MM-DD')
        
        if range_type == 'D':
            # получаем вчерашний день
            bday = pendulum.from_format(day, 'YYYY-MM-DD').add(days=-1)
            eday = bday
            return [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))]
        elif range_type == 'W':
            # получаем последнюю закрытую неделю
            bday = pendulum.from_format(day, 'YYYY-MM-DD')        
            if bday.day_of_week == 0:
                bday = bday.start_of('week')
            else:
                bday = bday.start_of('week').add(weeks=-1)
            eday = bday.add(days=6)
            return [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))]
        elif range_type == 'M':
            # получаем последний закрытый месяц    
            current_date = pendulum.from_format(day, 'YYYY-MM-DD')
            start_of_current_month = current_date.start_of('month')
            end_of_current_month = start_of_current_month.add(months=1).add(days=-1)
            if current_date != end_of_current_month:
                bday = start_of_current_month.add(months=-1)
                eday = bday.add(months=1).add(days=-1)
            else:
                bday = start_of_current_month
                eday = end_of_current_month
            return [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))]
        else:
            raise ValueError(f'Wrong range type: {range_type}')
        
    def get_partner_data(self, date_filter: list = None, 
                 stats: list = ['hitsVisits'],
                 slices: list =["partnerName", 'tmsec'], 
                 partner: str = None,                 
                 tmsecs: list = None,
                 devices: list = ["ALL"],
                 geos: list = ['W'], 
                 sampling: int = 100):
        """
        Получить данные

        Parameters
        ----------
        

        Returns
        -------
        
        """        
        error_msg = None
        
        results = []        
        
        if date_filter is None:            
            # если день не указан, то берем вчерашний
            bday = pendulum.from_format(pendulum.now('Europe/Moscow').format('YYYY-MM-DD'), 'YYYY-MM-DD').add(days=-1)
            eday = bday
            date_filter = [(bday.format('YYYY-MM-DD'), eday.format('YYYY-MM-DD'))]        
        
        for geo in geos:
            for device in devices:                
                try:                        
                    project_name = f'{partner}-{date_filter}-{geo}-{device}-sampling {sampling}'
                    
                    geo_filter = self.geo_types[geo]
                    device_type_filter = self.device_types[device]
                    
                    task_json = self.build_task(task_name=project_name,
                                                    date_filter=date_filter,
                                                    area_type_filter=self.area_type_filter,
                                                    partner_filter=[partner],
                                                    tmsec_filter=tmsecs,
                                                    geo_filter=geo_filter,
                                                    device_type_filter=device_type_filter,
                                                    slices=slices,
                                                    statistics=stats,
                                                    sampling=sampling) 
                    
                    df_res = self.result2table(self.send_task(task_json), project_name) 
                    
                    df_res['deviceTypeName'] = device
                    df_res['geo'] = geo
                    
                    if df_res is not None:
                        if len(df_res):
                            results.append(df_res)                    
                except Exception as ex:
                    error_msg = f"{partner}-{date_filter}-{geo}-{device}-sampling {sampling} Error: {ex}"
                    break
        
        if results is not None:
            if len(results):                
                    return pd.concat(results)
                
        if error_msg is not None:
            return error_msg
        else:
            return f"Нет данных за {date_filter}"
