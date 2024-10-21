import json
import pandas as pd
import numpy as np
import datetime as dt
import time
import pendulum
from ..core import net
from ..core import tasks
from ..core import errors
from ..core import sql
from ..core import utils


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
                client_secret: str = None, keycloak_url: str = None, check_version: bool = True, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, check_version: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if check_version:
            utils.check_version()
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.task_builder = tasks.TaskBuilder()

    @staticmethod
    def add_device_type_filter(tsk: dict, device_type_names, filter_name):
        # Добавляем фильтр по типам устройств
        if device_type_names is not None and type(device_type_names) == list and len(device_type_names) > 0:
            device_type_filters = {
                "operand": "OR",
                "elements": []
            }
            for dt in device_type_names:
                if "elements" not in device_type_filters:
                    if dt in CounterTask.device_types.keys():
                        device_type_filters.update(
                                sql.sql_to_json(CounterTask.device_types[dt]))
                else:
                    if dt in CounterTask.device_types.keys():
                        device_type_filters["elements"].append(sql.sql_to_json(CounterTask.device_types[dt])['elements'][0])
            tsk['filter'][filter_name] = device_type_filters

    def build_task(self, task_name: str = '', date_filter: list = None, area_type_filter: list = None,
                   partner_filter: list = None, tmsec_filter: list = None, geo_filter: str = None,
                   device_type_filter: list = None, mart_filter: str = None, slices: list = None,
                   statistics: list = None, sampling: int = 42) -> dict:
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

        mart_filter: str|None
            Условия фильтрации по профилю
            Пример:
                mart_filter = "advertisementAgencyId = 1"

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
        self.add_device_type_filter(tsk, device_type_filter, 'deviceTypeFilter')
        self.task_builder.add_filter(tsk, geo_filter, 'geoFilter')
        self.task_builder.add_filter(tsk, mart_filter, 'profileFilter')

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
            'mart_filter': mart_filter,
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
                        task_state_obj = self.msapi_network.send_request('get', '/task/state/{}'.format(tid))
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
                    task_state_obj = self.msapi_network.send_request('get', '/task/state/{}'.format(tsk['taskId']))
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
            if len(task_list) > 0:
                print(f'Расчет задач ({len(task_list)}) [ ', end='')
                s = dt.datetime.now()
                while True:
                    time.sleep(status_delay)
                    # запросим состояние
                    done_count = 0
                    for t in task_list:
                        tid = t['task']['taskId']
                        task_state = ''
                        time.sleep(task_delay)
                        task_state_obj = self.msapi_network.send_request('get', '/task/state/{}'.format(tid))
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
            task_state_obj = self.msapi_network.send_request('get', '/task/state/{}'.format(tid))
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
                    'taskStatus': 'IN_QUEUE',
                    'additionalParameters': {},
                    'dtRegister': '2024-09-30 12:17:33'
                }
        """
        if tsk.get('taskId') is not None:
            tid = tsk.get('taskId', None)
            task_state_obj = self.msapi_network.send_request('get', '/task/state/restart/{}'.format(tid))
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
                    'dtRegister': '2024-09-30 12:17:33'
                }
        """
        post_data = {
            "taskIds": tsk_ids
        }

        task_state_obj = self.msapi_network.send_request('post', '/task/state/restart', json.dumps(post_data))
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
                    'dtFinish': '2024-09-30 12:49:43',
                    'taskProcessingTimeSec': 1930
                }
        """
        if tsk.get('taskId') is not None:
            tid = tsk.get('taskId', None)
            task_state_obj = self.msapi_network.send_request('get', '/task/state/cancel/{}'.format(tid))
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
                    'dtFinish': '2024-09-30 12:49:43',
                    'taskProcessingTimeSec': 1930
                }
        """
        post_data = {
            "taskIds": tsk_ids
        }

        task_state_obj = self.msapi_network.send_request('post', '/task/state/cancel', json.dumps(post_data))
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
        return self.msapi_network.send_request('get', '/task/result/{}'.format(tsk['taskId']))

    @staticmethod
    def result2table(data, project_name: str = None):
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

        task = self.wait_task(self.send_task(task_json))
        df_tmsecs = self.result2table(self.get_result(task), project_name)
        
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

        task = self.wait_task(self.send_task(task_json))
        df_tmsecs = self.result2table(self.get_result(task), project_name)

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
                         partners: list = None,
                         tmsecs: list = None,
                         devices: list = ["ALL"],
                         geos: list = ['W'],
                         mart: str = None,
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
        
        tasks = []
        for partner in partners:
            for geo in geos:
                for device in devices:
                    project_name = f'{partner}-{date_filter}-{geo}-{device}-sampling {sampling}'

                    if geo in self.geo_types:
                        geo_filter = self.geo_types[geo]
                    else:
                        geo_filter = geo
                    device_type_filter = self.device_types[device]

                    task_json = self.build_task(task_name=project_name,
                                                    date_filter=date_filter,
                                                    area_type_filter=self.area_type_filter,
                                                    partner_filter=[partner],
                                                    tmsec_filter=tmsecs,
                                                    geo_filter=geo_filter,
                                                    device_type_filter=device_type_filter,
                                                    mart_filter=mart,
                                                    slices=slices,
                                                    statistics=stats,
                                                    sampling=sampling)

                    tasks.append({
                        "task": self.send_task(task_json),
                        "geo": geo,
                        "device": device
                    })


        tasks = self.wait_task(tasks)
        for t in tasks:
            df_res = self.result2table(self.get_result(t.get("task")))

            if df_res is not None:
                if len(df_res):
                    df_res['deviceTypeName'] = t.get("device")
                    df_res['geo'] = t.get("geo")
                    results.append(df_res)
        
        if results is not None:
            if len(results):                
                return pd.concat(results)
                
        if error_msg is not None:
            return error_msg
        else:
            return f"Нет данных за {date_filter}"
