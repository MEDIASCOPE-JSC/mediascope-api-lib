import json
import pandas as pd
from ..core import net
from ..core import utils


class MediaVortexCats:
    _urls = {
        'tv-kit': '/kit',
        'tv-tv-ad': '/dictionary/tv/ad',
        'tv-subbrand': '/dictionary/tv/subbrand',
        'tv-subbrand-list': '/dictionary/tv/subbrand-list',
        'tv-research-day-type': '/dictionary/tv/research-day-type',
        'tv-region': '/dictionary/tv/region',
        'tv-program': '/dictionary/tv/program',
        'tv-program-type': '/dictionary/tv/program-type',
        'tv-program-sport': '/dictionary/tv/program-sport',
        'tv-program-sport-group': '/dictionary/tv/program-sport-group',
        'tv-program-issue-description': '/dictionary/tv/program-issue-description',
        'tv-program-category': '/dictionary/tv/program-category',
        'tv-net': '/dictionary/tv/net',
        'tv-model': '/dictionary/tv/model',
        'tv-model-list': '/dictionary/tv/model-list',
        'tv-location': '/dictionary/tv/location',
        'tv-grp-type': '/dictionary/tv/grp-type',
        'tv-exchange-rate': '/dictionary/tv/exchange-rate',
        'tv-digital-broadcasting-type': '/dictionary/tv/digital-broadcasting-type',
        'tv-day-week': '/dictionary/tv/day-week',
        'tv-company': '/dictionary/tv/company',
        'tv-company-merge': '/dictionary/tv/company-merge',
        'tv-calendar': '/dictionary/tv/calendar',
        'tv-breaks': '/dictionary/tv/breaks',
        'tv-brand': '/dictionary/tv/brand',
        'tv-brand-list': '/dictionary/tv/brand-list',
        'tv-article': '/dictionary/tv/article',
        'tv-article-list4': '/dictionary/tv/article-list4',
        'tv-article-list3': '/dictionary/tv/article-list3',
        'tv-article-list2': '/dictionary/tv/article-list2',
        'tv-appendix': '/dictionary/tv/appendix',
        'tv-advertiser': '/dictionary/tv/advertiser',
        'tv-advertiser-list': '/dictionary/tv/advertiser-list',
        'tv-advertiser-tree': '/dictionary/tv/advertiser-tree',
        'tv-ad': '/dictionary/tv/ad',
        'tv-ad-type': '/dictionary/tv/ad-type',
        'tv-ad-total': '/dictionary/tv/ad-total',
        'tv-ad-style': '/dictionary/tv/ad-style',
        'tv-ad-slogan-video': '/dictionary/tv/ad-slogan-video',
        'tv-ad-slogan-audio': '/dictionary/tv/ad-slogan-audio',
        'tv-ad-month': '/dictionary/tv/ad-month',
        'tv-time-band': '/dictionary/tv/time-band',
        'tv-stat': '/dictionary/tv/stat',
        'tv-relation': '/dictionary/tv/relation',
        'tv-monitoring-type': '/dictionary/tv/monitoring-type',
        'tv-db-rd-type': '/dictionary/tv/db-rd-type',
        'tv-ad-iss-sbtv': '/dictionary/tv/ad-iss-sbtv',
        'tv-demo-attribute': '/dictionary/tv/demo-attribute',
        'tv-program-country': '/dictionary/tv/program-country',
        'tv-company-holding': '/dictionary/tv/company-holding',
        'tv-company-media-holding': '/dictionary/tv/company-media-holding',
        'tv-thematic': '/dictionary/tv/thematic',
        'custom-respondent-variable': '/custom-variable/respondent',
        'tv-issue-status': '/dictionary/tv/issue-status',
        'tv-area': '/dictionary/tv/area',
        'tv-prime-time-status': '/dictionary/tv/prime-time-status',
        'tv-ad-position': '/dictionary/tv/ad-position',
        'tv-breaks-style': '/dictionary/tv/breaks-style',
        'tv-breaks-position': '/dictionary/tv/breaks-position',
        'tv-breaks-distribution': '/dictionary/tv/breaks-distribution',
        'tv-breaks-content': '/dictionary/tv/breaks-content',
        'tv-program-producer-country': '/dictionary/tv/program-producer-country',
        'tv-program-producer': '/dictionary/tv/program-producer',
        'tv-program-group': '/dictionary/tv/program-group',
        'tv-no-yes-na': '/dictionary/tv/no-yes-na',
        'tv-language': '/dictionary/tv/language',
        'tv-company-monitoring': '/dictionary/tv/company-monitoring',
        'tv-company-group': '/dictionary/tv/company-group',
        'tv-company-category': '/dictionary/tv/company-category',
        'tv-company-status': '/dictionary/tv/company-status',
        'tv-age-restriction': '/dictionary/tv/age-restriction',
        'availability-period': '/period/availability-period',
        'monitoring-cities': '/dictionary/tv/monitoring-cities',
        'tv-platform': '/dictionary/tv/platform',
        'tv-playbacktype': '/dictionary/tv/play-back-type'
    }

    def __new__(cls, facility_id=None, settings_filename: str = None, cache_path: str = None,
                cache_enabled: bool = True, username: str = None, passw: str = None, root_url: str = None,
                client_id: str = None, client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(MediaVortexCats, cls).__new__(
                cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id=None, settings_filename: str = None, cache_path: str = None,
                 cache_enabled: bool = True, username: str = None, passw: str = None, root_url: str = None,
                 client_id: str = None, client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.tv_demo_attribs = self.load_tv_property()
        self.tv_units = self.get_units()

    def load_tv_property(self):
        """
        Загрузить список демографических переменных

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демо переменными
        """
        data = self.get_tv_demo_attribute()
        # формируем столбец с именами срезов, относящихся к переменным (изменяем первую букву на строчную)
        # data['entityName'] = data['colName'].str[0].str.lower() + data['colName'].str[1:]
        return data

    def find_tv_property(self, text, expand=True, with_id=False):
        """
        Поиск по каталогу демографических переменных

        Parameters
        ----------

        text : str
            Строка поиска

        expand : bool
            Развернуть категории - True/False

        with_id : bool
            Флаг отвечающий за отображение id переменной,
            По умолчанию: False

        Returns
        -------

        result : DataFrame

            Переменные найденные по тексту


        """
        df = self.tv_demo_attribs

        df['id'] = df['id'].astype(str)
        df['valueId'] = df['valueId'].astype(str)

        if not expand:
            df = df[['id', 'name', 'entityName']].drop_duplicates()

        df_found = df[df['name'].str.contains(text, case=False) |
                      df['entityName'].str.contains(text, case=False)
                      ]
        if with_id:
            return df_found
        else:
            return df_found.drop(columns=['id'])

    @staticmethod
    def _get_query(vals):
        if type(vals) != dict:
            return None
        query = ''
        for k, v in vals.items():
            if v is None:
                continue
            val = str(v).strip()
            if len(val) > 0:
                query += f'&{k}={v}'
        if len(query) > 0:
            query = '?' + query[1:]
        return query

    @staticmethod
    def _get_post_data(vals):
        if type(vals) != dict:
            return None
        data = {}
        for k, v in vals.items():
            if v is None:
                data[k] = None
                continue

            if type(v) == str:
                val = []
                for i in v.split(','):
                    val.append(str(i).strip())
                v = val
            if type(v) == list:
                data[k] = v

        if len(data) > 0:
            return json.dumps(data)

    def get_slices(self, slice_name):
        if type(self.units) != dict or \
                self.units.get('slices', None) is None or \
                self.units['slices'].get(slice_name, None) is None:
            return
        return self.units['slices'][slice_name]

    @staticmethod
    def _print_header(header, offset, limit):
        if type(header) != dict or 'total' not in header:
            return
        total = header["total"]
        print(
            f'Запрошены записи: {offset} - {offset + limit}\nВсего найдено записей: {total}\n')

    def _get_dict(self, entity_name, search_params=None, body_params=None, offset=None, limit=None, use_cache=False,
                  request_type='post', show_header=True):
        """
        Получить словарь из API

        Parameters
        ----------

        entity_name : str
            Название объекта словаря, см (_dictionary_urls)

        search_params : dict
            Словарь с параметрами поиска

        body_params : dict
            Словарь с параметрами в теле запроса

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        request_type : str
            Тип запроса (по умолчанию post)

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        products : DataFrame

            DataFrame с объектами словаря
        """

        if limit is not None and offset is None:
            raise ValueError("Необходимо указать значение параметра offset. Укажите 0, если смещение не требуется")
        
        if offset is not None and limit is None:
            raise ValueError("Необходимо указать значение параметра limit")
        
        if self._urls.get(entity_name) is None:
            return None

        url = self._urls[entity_name]
        query_dict = search_params
        if offset is not None and limit is not None:
            query_dict['offset'] = offset
            query_dict['limit'] = limit

        query = self._get_query(query_dict)
        if query is not None or len(query) > 0:
            url += query

        post_data = self._get_post_data(body_params)

        data = self.msapi_network.send_request_lo(
            request_type, url, data=post_data, use_cache=use_cache)

        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return data

        # извлекаем все заголовки столбцов (их может быть разное количество, особенно для поля notes)
        res_headers = []
        for item in data['data']:
            for k, v in item.items():
                if k not in res_headers:
                    res_headers.append(k)

                    # инициализируем списки данных столбцов
        res = {}
        for h in res_headers:
            res[h] = []

        # наполняем найденные столбцы значениями
        for item in data['data']:
            for h in res_headers:
                if h in item.keys():
                    res[h].append(item[h])
                else:
                    res[h].append('')

        # print header        
        if show_header:
            if offset is not None and limit is not None:
                self._print_header(data['header'], offset, limit)
            else:
                self._print_header(data['header'], 0, data['header']['total'])
        return pd.DataFrame(res)

    def get_units(self):
        """
        Получить списки доступных атрибутов всех отчетов:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        units = self.msapi_network.send_request(
            'get', self._urls['tv-kit'], use_cache=False)

        result = {}
        for unit in units:
            if 'reports' in unit:
                for report in unit['reports']:
                    stats = []
                    slices = []
                    filters = []
                    if 'statistics' in report:
                        stats = [i['name'] for i in report['statistics']]
                    if 'slices' in report:
                        slices = [i['name'] for i in report['slices']]
                    if 'filters' in report:
                        filters = [i['name'] for i in report['filters']]
                    if str(unit['id']) not in result:
                        result[str(unit['id'])] = {
                            report['name']: {
                                'statistics': stats,
                                'slices': slices,
                                'filters': filters
                            }
                        }
                    else:
                        result[str(unit['id'])][report['name']] = {
                            'statistics': stats,
                            'slices': slices,
                            'filters': filters
                        }
        # хардкод для поставки 7 биг тв
        if "7" not in result:
            result["7"] = result.get("1")
        return result

    def get_timeband_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета Периоды (Timeband):
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('TimeBand')

    def get_simple_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета События (Simple):
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('Simple')

    def get_crosstab_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета Кросс Таблица (Crosstab):
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('CrossTab')

    def get_consumption_target_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета Consumption target:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('ConsumptionTarget')

    def get_duplication_timeband_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета Пересечение аудитории (Duplication timeband):
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('DuplicationTimeBand')

    def get_tv_subbrand(self, ids=None, name=None, ename=None, brand_ids=None, tv_area_ids=None, notes=None,
                        order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию суббрендов

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов суббрендов
        
        name : str or list of str
            Поиск по имени суббренда
        
        ename : str or list of str
            Поиск по англоязычному имени суббренда
            
        brand_ids : str or list of str
            Поиск по списку идентификаторов брендов

        tv_area_ids : str or list of str
            Поиск по списку идентификаторов областей выхода
        
        notes : str or list of str
            Поиск по заметкам
        
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с суббрендами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "brandId": brand_ids,
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "tvArea": tv_area_ids
        }

        df_sub = self._get_dict(entity_name='tv-subbrand', search_params=search_params, body_params=body_params,
                                offset=offset, limit=limit, use_cache=use_cache, show_header=show_header)

        return df_sub.reindex(columns=['id', 'name', 'ename', 'brandId', 'tvArea', 'notes'],fill_value='')

    def get_tv_subbrand_list(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                             offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков суббрендов

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов суббрендов
        
        name : str or list of str
            Поиск по имени суббренда
        
        ename : str or list of str
            Поиск по англоязычному имени суббренда
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками суббрендов
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-subbrand-list', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_research_day_type(self, order_by='id', order_dir=None, offset=None,
                                 limit=None, use_cache=False, show_header=True):
        """
        Получить типы дней

        Parameters
        ----------

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с типами дней
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": None,
            "name": None,
            "ename": None
        }

        return self._get_dict(entity_name='tv-research-day-type', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_region(self, ids=None, name=None, ename=None, notes=None, monitoring_type=None, order_by='id',
                      order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить регионы

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов регионов
        
        name : str or list of str
            Поиск по имени региона
        
        ename : str or list of str
            Поиск по англоязычному имени региона
            
        notes : str or list of str
            Поиск по заметкам
        
        monitoring_type : str or list of str
            Поиск по типу мониторинга
                
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с регионами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "monitoringType": monitoring_type
        }

        return self._get_dict(entity_name='tv-region', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program(self, ids=None, name=None, ename=None, extended_name=None, extended_ename=None,
                       first_issue_date=None, program_type_ids=None, program_category_ids=None, country_ids=None,
                       program_sport_ids=None, sport_group_ids=None, language_ids=None, program_producer_ids=None,
                       program_producer_year=None, is_program_group=None, is_child=None, notes=None, order_by='id',
                       order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию программ

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов программ

        name : str or list of str
            Поиск по имени программы
        
        ename : str or list of str
            Поиск по англоязычному имени программы
        
        extended_name : str or list of str
            Поиск по полному имени программы
        
        extended_ename : str or list of str
            Поиск по полному англоязычному имени программы

        first_issue_date : str or list of str
            Поиск по дате первого выхода
        
        program_type_ids : str or list of str
            Поиск по списку идентификаторов жанров программ
        
        program_category_ids : str or list of str
            Поиск по списку идентификаторов категорий программ

        country_ids : str or list of str
            Поиск по списку идентификаторов стран производства
        
        program_sport_ids : str or list of str
            Поиск по списку идентификаторов видов спорта
        
        sport_group_ids : str or list of str
            Поиск по списку идентификаторов групп спорта

        language_ids : str or list of str
            Поиск по списку идентификаторов языков
        
        program_producer_ids : str or list of str
            Поиск по списку идентификаторов производителей программ
        
        program_producer_year : str or list of str
            Поиск по году создания

        is_program_group : string
            Поиск по флагу программа группа
        
        is_child : string
            Поиск по флагу программа десткая
            
        notes : str or list of str
            Поиск по заметкам            
        
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.      
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с программами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "programTypeId": program_type_ids,
            "programCategoryId": program_category_ids,
            "programSportId": program_sport_ids,
            "sportGroupId": sport_group_ids,
            "languageId": language_ids,
            "programProducerId": program_producer_ids,
            "programProducerYear": program_producer_year,
            "isProgramGroup": is_program_group,
            "isChild": is_child,
            "id": ids,
            "countryId": country_ids,
            "name": name,
            "ename": ename,
            "extendedName": extended_name,
            "extendedEname": extended_ename,
            "notes": notes,
            "firstIssueDate": first_issue_date
        }

        df_prog =  self._get_dict(entity_name='tv-program', search_params=search_params,
                                  body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                  show_header=show_header)
        
        return df_prog.reindex(
            columns=['id', 'name', 'ename', 'extendedName', 'extendedEname', 'firstIssueDate', 'programTypeId',
                     'programCategoryId', 'programCountryId', 'programSportId', 'programSportGroupId', 'languageId',
                     'programProducerId', 'producerYear', 'isProgramGroup', 'isChild', 'notes'],
            fill_value='')

    def get_tv_program_type(self, ids=None, name=None, ename=None, notes=None,
                            order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию жанров программ

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов жанров программ
        
        name : str or list of str
            Поиск по имени жанра
        
        ename : str or list of str
            Поиск по англоязычному имени жанра
            
        notes : str or list of str
            Поиск по заметкам 
                       
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с жанрами программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict(entity_name='tv-program-type', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_sport(self, ids=None, name=None, ename=None, notes=None, order_by='id',
                             order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию видов спорта

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов видов спорта
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 
            
        notes : str or list of str
            Поиск по заметкам 
                        
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с видами спорта
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict(entity_name='tv-program-sport', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_sport_group(self, ids=None, name=None, ename=None, notes=None, order_by='id',
                                   order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию групп спорта

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов групп спорта
        
        name : str or list of str
            Поиск по имени группы
        
        ename : str or list of str
            Поиск по англоязычному имени группы
            
        notes : str or list of str
            Поиск по заметкам 
                                    
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с группами спорта
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict(entity_name='tv-program-sport-group', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_issue_description(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                                         offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию описаний выходов программ

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов описаний выпусков
        
        name : str or list of str
            Поиск по тексту описания
        
        ename : str or list of str
            Поиск по англоязычному тексту описания
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с описаниями выходов программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-program-issue-description', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_category(self, ids=None, name=None, ename=None, short_name=None, short_ename=None, type_ids=None, 
                                program_type_category_nums=None, notes=None, offset=None, limit=None, use_cache=False,
                                show_header=True):
        """
        Получить коллекцию категорий программ

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов категорий программ

        name : str or list of str
            Поиск по имени категории
        
        ename : str or list of str
            Поиск по англоязычному имени категории

        short_name : str or list of str
            Поиск по короткому имени категории
        
        short_ename : str or list of str
            Поиск по короткому англоязычному имени категории

        type_ids : str or list of str
            Поиск по списку идентификаторов жанров программ
        
        program_type_category_nums : str or list of str
            Поиск по порядковому номеру категории в жанре
            
        notes : str or list of str
            Поиск по заметкам 

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с категориями телепрограмм
        """

        search_params = {
            'orderBy': None,
            'orderDir': None
        }

        body_params = {
            "programTypeId": type_ids,
            "programTypeCategoryNum": program_type_category_nums,
            "id": ids,
            "name": name,
            "ename": ename,
            "shortName": short_name,
            "shortEname": short_ename,
            "notes": notes
        }

        df_pr_cat = self._get_dict(entity_name='tv-program-category', search_params=search_params,
                                   body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                   show_header=show_header)
        
        df_pr_cat = df_pr_cat.reindex(
            columns=['id', 'name', 'ename', 'shortName', 'shortEname', 'programTypeId', 'programTypeCategoryNum',
                     'notes'],
            fill_value='')
        
        return df_pr_cat.sort_values(by=['programTypeId', 'programTypeCategoryNum'])

    def get_tv_net(self, ids=None, name=None, ename=None, order_by='id',
                   order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию телесетей

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов сетей
        
        name : str or list of str
            Поиск по имени сети
        
        ename : str or list of str
            Поиск по англоязычному имени сети 
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с телесетями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        df_net = self._get_dict(entity_name='tv-net', search_params=search_params,
                                body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                show_header=show_header)
        
        return df_net.reindex(columns=['id', 'name', 'ename'], fill_value='')

    def get_tv_model(self, ids=None, name=None, ename=None, subbrand_ids=None, article_ids=None, tv_area_ids=None, notes=None,
                     order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию продуктов

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов моделей
        
        name : str or list of str
            Поиск по имени
        
        ename : str or list of str
            Поиск по англоязычному имени

        subbrand_ids : str or list of str
            Поиск по списку идентификаторов суббрендов
            
        article_ids : str or list of str
            Поиск по списку идентификаторов статей

        tv_area_ids : str or list of str
            Поиск по списку идентификаторов областей выхода 
            
        notes : str or list of str
            Поиск по заметкам
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с продуктами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "subbrandId": subbrand_ids,
            "articleId": article_ids,
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "tvArea": tv_area_ids
        }

        df_mod = self._get_dict(entity_name='tv-model', search_params=search_params,
                                body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                show_header=show_header)

        return df_mod.reindex(
            columns=['id', 'name', 'ename', 'subbrandId', 'articleId', 'tvArea', 'notes'],
            fill_value='')

    def get_tv_model_list(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                          offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков продуктов

        Parameters
        ----------       
        ids : str or list of str
            Поиск по списку идентификаторов моделей
        
        name : str or list of str
            Поиск по имени
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками продуктов
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-model-list', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_location(self, ids=None, name=None, ename=None, order_by='id',
                        order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию мест просмотра

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов мест

        name : str or list of str
            Поиск по имени места
        
        ename : str or list of str
            Поиск по англоязычному имени места
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0. 

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с местами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-location', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_grp_type(self, ids=None, name=None, notes=None, expression=None, order_by='name',
                        order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить типы баинговых аудиторий

        Parameters
        ----------       
        ids : str or list of str
            Поиск по списку идентификаторов типов групп
        
        name : str or list of str
            Поиск по имени
        
        notes : str or list of str
            Поиск по заметке
        
        expression : str or list of str
            Поиск по выражению 
            
        order_by : string, default 'name'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с типами баинговых аудиторий
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "notes": notes,
            "expression": expression
        }

        return self._get_dict(entity_name='tv-grp-type', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_exchange_rate(self, research_date=None, rate=None, order_by='researchDay',
                             order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить курсы обмена

        Parameters
        ----------       
        research_date : str or list of str
            Поиск по списку дней
        
        rate : str or list of str
            Поиск по списку курсов
            
        order_by : string, default 'researchDay'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с курсами обмена
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "researchDay": research_date,
            "rate": rate
        }

        return self._get_dict(entity_name='tv-exchange-rate', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_day_week(self, ids=None, name=None, ename=None, order_by='id',
                        order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллецию дней недели

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов дней недели
            
        name : str or list of str
            Поиск по имени дня
        
        ename : str or list of str
            Поиск по англоязычному имени дня  
                      
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с днями недели
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-day-week', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company(self, ids=None, name=None, ename=None, tv_net_ids=None, region_ids=None, tv_company_group_ids=None,
                       tv_company_category_ids=None, information=None, offset=None,
                       limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию телекомпаний

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов телекомпаний
        
        name : str or list of str
            Поиск по имени телекомпании
        
        ename : str or list of str
            Поиск по англоязычному имени телекомпании
           
        tv_net_ids : str or list of str
            Поиск по списку идентификаторов телесетей
            
        region_ids : str or list of str
            Поиск по списку идентификаторов регионов
            
        tv_company_group_ids : str or list of str
            Поиск по списку идентификаторов групп телекомпаний
            
        tv_company_category_ids : str or list of str
            Поиск по списку идентификаторов категорий телекомпаний
             
        information : str or list of str
            Поиск по информации      
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с телекомпаниями
        """

        search_params = {
            'orderBy': None,
            'orderDir': None
        }

        body_params = {
            "tvNetId": tv_net_ids,
            "regionId": region_ids,
            "tvCompanyGroupId": tv_company_group_ids,
            "tvCompanyCategoryId": tv_company_category_ids,
            "name": name,
            "ename": ename,
            "id": ids,
            "information": information
        }

        df_comp = self._get_dict(entity_name='tv-company', search_params=search_params,
                                 body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                 show_header=show_header)

        df_comp = df_comp.reindex(
            columns=['id', 'name', 'ename', 'tvNetId', 'regionId', 'tvCompanyHoldingId', 'tvCompanyMediaHoldingId',
                     'tvThematicId', 'tvCompanyGroupId', 'tvCompanyCategoryId', 'tvCompanyMediaType', 'information'],
            fill_value='')
        
        return df_comp.sort_values(by=['regionId','id'])

    def get_tv_company_merge(self, ids=None, tv_channel_merge_ids=None, tv_company_ids=None, 
                             order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить объединенные компании в регионах

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов объединенных телекомпаний

        tv_channel_merge_ids : str or list of str
            Поиск по списку идентификаторов объединенных каналов
        
        tv_company_ids : str or list of str
            Поиск по списку идентификаторов телекомпаний
                               
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с объединенными компаниями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "tvChannelMergeId": tv_channel_merge_ids,
            "tvCompanyId": tv_company_ids,
            "id": ids
        }

        return self._get_dict(entity_name='tv-company-merge', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_calendar(self, research_date=None, research_day_type=None, order_by='researchDate', order_dir=None,
                        offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить календарь

        Parameters
        ----------
        research_date : str or list of str
            Поиск по списку дат
        
        research_day_type : str or list of str
            Поиск по списку типов дат 
                        
        order_by : string, default 'researchDate'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с календарем
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "researchDate": research_date,
            "researchDayType": research_day_type
        }

        df_calendar = self._get_dict(entity_name='tv-calendar', search_params=search_params,
                                     body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                     show_header=show_header)

        return df_calendar.reindex(columns=['researchDate', 'researchDayTypeId'], fill_value='')

    def get_tv_breaks(self, ids=None, name=None, ename=None, pos_types=None, distrib_types=None, cont_types=None,
                      style_ids=None, notes=None, order_by='id', order_dir=None, offset=None, limit=None,
                      use_cache=False, show_header=True):
        """
        Получить коллекцию рекламных блоков

        Parameters
        ----------   
        ids : str or list of str
            Поиск по списку идентификаторов блоков

        name : str or list of str
            Поиск по имени блока
        
        ename : str or list of str
            Поиск по англоязычному имени блока
           
        pos_types : str or list of str
            Поиск по списку идентификаторов типов блоков

        distrib_types : str or list of str
            Поиск по списку идентификаторов типов распространения блоков

        cont_types : str or list of str
            Поиск по списку идентификаторов типов содержания блоков

        style_ids : str or list of str
            Поиск по списку идентификаторов стилей блоков

        notes : str or list of str
            Поиск по заметкам 

        order_by : string, default 'id
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с рекламными блоками
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "name": name,
            "ename": ename,
            "id": ids,
            "notes": notes,
            "positionType": pos_types,
            "distributionType": distrib_types,
            "contentType": cont_types,
            "styleId": style_ids
        }

        return self._get_dict(entity_name='tv-breaks', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_brand(self, ids=None, name=None, ename=None, notes=None, tv_area_ids=None,
                     order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию брендов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов брендов
        
        name : str or list of str
            Поиск по имени бренда
        
        ename : str or list of str
            Поиск по англоязычному имени бренда
            
        notes : str or list of str
            Поиск по заметкам
        
        tv_area_ids : str or list of str
            Поиск по списку идентификаторов областей выхода
        
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с брендами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "tvArea": tv_area_ids
        }

        df_brand = self._get_dict(entity_name='tv-brand', search_params=search_params,
                                  body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                  show_header=show_header)

        return df_brand.reindex(columns=['id', 'name', 'ename', 'tvArea', 'notes'], fill_value='')

    def get_tv_brand_list(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                          offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков брендов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов 
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками брендов
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-brand-list', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_article(self, ids=None, name=None, ename=None, levels=None, parent_ids=None, notes=None,
                       order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить товарные категории

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов 
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 

        levels : str or list of str
            Поиск по списку уровней  

        parent_ids : str or list of str
            Поиск по списку родительских идентификаторов
        
        notes : str or list of str
            Поиск по заметкам

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с товарными категориями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "parentId": parent_ids,
            "level": levels,
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        df_art = self._get_dict(entity_name='tv-article', search_params=search_params,
                                body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                show_header=show_header)

        return df_art.reindex(columns=['id', 'name', 'ename', 'level', 'parentId', 'notes'], fill_value='')

    def get_tv_article_list4(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                             offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков товарных категорий 4 уровня

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов 
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками товарных категорий 4 уровня
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-article-list4', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_article_list3(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                             offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков товарных категорий 3 уровня

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов 
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками товарных категорий 3 уровня 
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-article-list3', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_article_list2(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                             offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков товарных категорий 2 уровня

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов 
        
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 

        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками товарных категорий 2 уровня
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-article-list2', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_appendix(self, ids=None, advertiser_ids=None, brand_ids=None, subbrand_ids=None, model_ids=None,
                        article2_ids=None, article3_ids=None, article4_ids=None, 
                        order_by='adId', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить аппендикс

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов роликов

        advertiser_ids : str or list of str
            Поиск по списку идентификаторов рекламодателей

        brand_ids : str or list of str
            Поиск по списку идентификаторов брендов

        subbrand_ids : str or list of str
            Поиск по списку идентификаторов суббрендов       
        
        model_ids : str or list of str
            Поиск по списку идентификаторов моделей

        article2_ids : str or list of str
            Поиск по списку идентификаторов 
            
        article3_ids : str or list of str
            Поиск по списку идентификаторов 
            
        article4_ids : str or list of str
            Поиск по списку идентификаторов 
            
        order_by : string, default 'adId'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с аппендиксом
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "adId": ids,
            "modelId": model_ids,
            "advertiserId": advertiser_ids,
            "article2Id": article2_ids,
            "article3Id": article3_ids,
            "article4Id": article4_ids,
            "subbrandId": subbrand_ids,
            "brandId": brand_ids
        }

        df_appndx = self._get_dict(entity_name='tv-appendix', search_params=search_params,
                                   body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                   show_header=show_header)

        return df_appndx.reindex(
            columns=['adId', 'advertiserId', 'brandId', 'subbrandId', 'modelId', 'articleLevel_1Id',
                     'articleLevel_2Id', 'articleLevel_3Id', 'articleLevel_4Id'],
            fill_value='')

    def get_tv_advertiser(self, ids=None, name=None, ename=None, notes=None, tv_area_ids=None,
                          order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию рекламодателей

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
       
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 
        
        notes : str or list of str
            Поиск по заметкам
            
        tv_area_ids : str or list of str
            Поиск по списку идентификаторов областей выхода
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с рекламодателями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "tvArea": tv_area_ids
        }

        df_advert = self._get_dict(entity_name='tv-advertiser', search_params=search_params,
                                   body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                   show_header=show_header)
    
        return df_advert.reindex(columns=['id', 'name', 'ename', 'tvArea', 'notes'], fill_value='')

    def get_tv_advertiser_list(self, ids=None, name=None, ename=None,
                               order_by='id', order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию списков рекламодателей

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
       
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени 
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками рекламодателей
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-advertiser-list', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_advertiser_tree(self, advertiser_ids=None, brand_ids=None, subbrand_ids=None,
                               model_ids=None, advertiser_names=None, advertiser_enames=None,
                               brand_names=None, brand_enames=None, subbrand_names=None,
                               subbrand_enames=None, model_names=None, model_enames=None,
                               kit_ids=None, order_by='id', order_dir=None, offset=None, limit=None,
                               use_cache=False, show_header=True):
        """
        Получить дерево рекламы

        Parameters
        ----------
        advertiser_ids : str or list of str
            Поиск по списку идентификаторов рекламодателей

        brand_ids : str or list of str
            Поиск по списку идентификаторов брендов

        subbrand_ids : str or list of str
            Поиск по списку идентификаторов суббрендов

        model_ids : str or list of str
            Поиск по списку идентификаторов моделей

        advertiser_names : str or list of str
            Поиск по списку имен рекламодателей

        advertiser_enames : str or list of str
            Поиск по списку англоязычных имен рекламодателей

        brand_names : str or list of str
            Поиск по списку имен брендов

        brand_enames : str or list of str
            Поиск по списку англоязычных имен брендов

        subbrand_names : str or list of str
            Поиск по списку имен суббрендов

        subbrand_enames : str or list of str
            Поиск по списку англоязычных имен суббрендов

        model_names : str or list of str
            Поиск по списку имен моделей

        model_enames : str or list of str
            Поиск по списку англоязычных имен моделей

        kit_ids : int or list of int
            Поиск по списку номеров поставок

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со списками рекламодателей
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "advertiserId": advertiser_ids,
            "brandId": brand_ids,
            "subbrandId": subbrand_ids,
            "modelId": model_ids,
            "advertiserName": advertiser_names,
            "advertiserEname": advertiser_enames,
            "brandName": brand_names,
            "brandEname": brand_enames,
            "subbrandName": subbrand_names,
            "subbrandEname": subbrand_enames,
            "modelName": model_names,
            "modelEname": model_enames,
            "kitId": kit_ids
        }

        return self._get_dict(entity_name='tv-advertiser-tree', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_ad(self, ids=None, tv_ad_type_ids=None, name=None, ename=None, notes=None, standard_durations=None, 
                  ad_style_ids=None, slogan_audio_ids=None, slogan_video_ids=None, first_issue_dates=None,  
                  advertiser_list_ids=None, brand_list_ids=None, subbrand_list_ids=None, model_list_ids=None,
                  article_list2_ids=None, article_list3_ids=None, article_list4_ids=None, 
                  advertiser_list_main_ids=None, brand_list_main_ids=None, subbrand_list_main_ids=None,
                  model_list_main_ids=None, article_list2_main_ids=None, article_list3_main_ids=None,
                  article_list4_main_ids=None, age_restriction_ids=None, tv_area_ids=None, order_by='id',
                  order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию рекламных роликов

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов рекламы
            
        tv_ad_type_ids : str or list of str
            Поиск по списку идентификаторов типов рекламы

        name : str or list of str
            Поиск по имени рекламы
        
        ename : str or list of str
            Поиск по англоязычному имени рекламы  

        notes : str or list of str
            Поиск по заметкам

        standard_durations : str or list of str
            Поиск по списку продолжительности рекламы
        
        ad_style_ids : str or list of str
            Поиск по списку идентификаторов стилей рекламы

        slogan_audio_ids : str or list of str
            Поиск по списку идентификаторов аудио слоганов
        
        slogan_video_ids : str or list of str
            Поиск по списку идентификаторов видео слоганов

        first_issue_dates : str or list of str
            Поиск по списку дат первого выхода    
                 
        advertiser_list_ids : str or list of str
            Поиск по списку идентификаторов рекламодателей
        
        brand_list_ids : str or list of str
            Поиск по списку идентификаторов брендов

        subbrand_list_ids : str or list of str
            Поиск по списку идентификаторов суббрендов
        
        model_list_ids : str or list of str
            Поиск по списку идентификаторов моделей
        
        article_list2_ids : str or list of str
            Поиск по списку идентификаторов мест
        
        article_list3_ids : str or list of str
            Поиск по списку идентификаторов мест
        
        article_list4_ids : str or list of str
            Поиск по списку идентификаторов мест
        
        advertiser_list_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка рекламодателей
        
        brand_list_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка брендов

        subbrand_list_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка суббрендов
        
        model_list_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка моделей
            
        article_list2_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка статей 2
        
        article_list3_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка статей 3
        
        article_list4_main_ids : str or list of str
            Поиск по списку идентификаторов основного списка статей 4
        
        age_restriction_ids : str or list of str
            Поиск по списку идентификаторов возрастных ограничений

        tv_area_ids : str or list of str
            Поиск по списку идентификаторов областей выхода        
        
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.      
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с рекламой
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "tvAdTypeId": tv_ad_type_ids,
            "advertiserListId": advertiser_list_ids,
            "brandListId": brand_list_ids,
            "modelListId": model_list_ids,
            "articleList2Id": article_list2_ids,
            "articleList3Id": article_list3_ids,
            "articleList4Id": article_list4_ids,
            "subbrandListId": subbrand_list_ids,
            "adStyleId": ad_style_ids,
            "advertiserListMainId": advertiser_list_main_ids,
            "brandListMainId": brand_list_main_ids,
            "modelListMainId": model_list_main_ids,
            "articleList2MainId": article_list2_main_ids,
            "articleList3MainId": article_list3_main_ids,
            "articleList4MainId": article_list4_main_ids,
            "subbrandListMainId": subbrand_list_main_ids,
            "ageRestrictionId": age_restriction_ids,
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "standardDuration": standard_durations,
            "tvArea": tv_area_ids,
            "tvAdSloganAudioId": slogan_audio_ids,
            "tvAdSloganVideoId": slogan_video_ids,
            "firstIssueDate": first_issue_dates
        }

        df_ad = self._get_dict(entity_name='tv-ad', search_params=search_params,
                               body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                               show_header=show_header)

        return df_ad.reindex(
            columns=['id', 'adTypeId', 'name', 'ename', 'notes', 'standardDuration', 'adStyleId', 'sloganAudioId',
                     'sloganVideoId', 'firstIssueDate', 'advertiserListId', 'brandListId', 'subbrandListId',
                     'modelListId', 'articleList_2Id', 'articleList_3Id', 'articleList_4Id', 'advertiserListMainId',
                     'brandListMainId', 'subbrandListMainId', 'modelListMainId', 'articleList_2MainId',
                     'articleList_3MainId', 'articleList_4MainId', 'ageRestrictionId', 'tvArea'],
            fill_value='')

    def get_tv_ad_type(self, ids=None, name=None, ename=None, notes=None, accounting_duration_type_ids=None,
                       is_override=None, position_type=None, is_price=None, order_by='id', order_dir=None, offset=None,
                       limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов роликов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов роликов
            
        name : str or list of str
            Поиск по имени роликов
        
        ename : str or list of str
            Поиск по англоязычному имени роликов        
            
        notes : str or list of str
            Поиск по заметкам 
        
        accounting_duration_type_ids : str or list of str
            Поиск по списку идентификаторов режима учета длительности рекламы
        
        is_override : string
            Поиск по признаку наложения в эфире рекламы данного типа на рекламы других типов
        
        position_type : str or list of str
            Поиск по списку идентификаторов положения рекламы в эфире относительно телепередач и рекламных блоков
        
        is_price : string
            Поиск по признаку учета стоимости рекламы данного типа
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.      
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с типами роликов
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "accountingDurationType": accounting_duration_type_ids,
            "isOverride": is_override,
            "positionType": position_type,
            "isPrice": is_price
        }

        return self._get_dict(entity_name='tv-ad-type', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_ad_style(self, ids=None, name=None, ename=None, notes=None, order_by='id',
                        order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию стилей роликов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
        
        notes : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со стилями роликов
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict(entity_name='tv-ad-style', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_ad_slogan_video(self, ids=None, name=None, ename=None, order_by='id', order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию видео слоганов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с видео слоганами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-ad-slogan-video', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_ad_slogan_audio(self, ids=None, name=None, ename=None, order_by='id', order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию аудио слоганов

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с аудио слоганами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-ad-slogan-audio', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_time_band(self):
        """
        Получить коллекцию временных интервалов
        
        Returns
        -------
        info : DataFrame
            DataFrame с time-band
        """
        df_tb =  pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-time-band'], use_cache=False))
        df_tb['order'] = df_tb['name']

        order_dict = {
            'timeBand1':1,
            'timeBand5':2,
            'timeBand10':3,
            'timeBand15':4,
            'timeBand30':5,
            'timeBand60':6,
            }
        
        df_tb['order'] = df_tb['order'].map(order_dict)
        df_tb.sort_values(by=['order'], inplace=True)
        df_tb = df_tb.reindex(columns=['value', 'name'], fill_value='')
        
        return df_tb.reset_index(drop=True)

    def get_tv_stat(self):
        """
        Получить справочник статусов статистик
        
        Returns
        -------
        info : DataFrame
            DataFrame с stat
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-stat'], use_cache=False))

    def get_tv_relation(self):
        """
        Получить справочник отношений атрибутов
        
        Returns
        -------
        info : Dict
            Словарь с relation
        """
        return self.msapi_network.send_request('get', self._urls['tv-relation'], use_cache=False)

    def get_tv_monitoring_type(self):
        """
        Получить справочник типов мониторинга
        
        Returns
        -------
        info : DataFrame
            DataFrame с типами мониторинга
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-monitoring-type'], use_cache=False))

    def get_tv_demo_attribute(self, ids=None, names=None, entity_names=None, value_ids=None, value_names=None,
                              offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию демографических переменных

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов переменных

        names : str or list of str
            Поиск по списку имен переменных
        
        entity_names : str or list of str
            Поиск по списку entity имен переменных (атрибуты, которые задаются в параметры задания)
        
        value_ids : str or list of str
            Поиск по списку идентификаторов категорий переменных
        
        value_names : str or list of str
            Поиск по списку имен категорий переменных
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с атрибутами
        """

        search_params = {
            'orderBy': None,
            'orderDir': None
        }

        body_params = {
            "id": ids,
            "valueId": value_ids,
            "name": names,
            "colName": entity_names,
            "valueName": value_names,
            "demoAttributeColName": entity_names,
            "demoAttributeValueId": value_ids
        }

        df_dem = self._get_dict(entity_name='tv-demo-attribute', search_params=search_params,
                                body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                show_header=show_header)
        
        df_dem['entityName'] = df_dem['colName'].str[0].str.lower() + df_dem['colName'].str[1:]

        df_dem = df_dem.reindex(columns=['id', 'name', 'entityName', 'valueId', 'valueName'], fill_value='')

        return df_dem.sort_values(by=['id', 'valueId'])


    def get_tv_program_country(self, ids=None, name=None, ename=None, notes=None,
                               order_by='id', order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию стран производства программ

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
        
        notes : str or list of str
            Поиск по заметкам
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame со странами производства программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict(entity_name='tv-program-country', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company_holding(self, ids=None, name=None, ename=None,
                               order_by='id', order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию холдингов телекомпаний

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с холдингами телекомпаний
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-company-holding', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company_media_holding(self, ids=None, name=None,
                                     ename=None, order_by='id', order_dir=None, offset=None,
                                     limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию медиа холдингов телекомпаний

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с медиа холдингами телекомпаний
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-company-media-holding', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_thematic(self, ids=None, name=None, ename=None, order_by='id', order_dir=None,
                        offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию жанров телекомпаний

        Parameters
        ----------        
        ids : str or list of str
            Поиск по списку идентификаторов
            
        name : str or list of str
            Поиск по имени 
        
        ename : str or list of str
            Поиск по англоязычному имени
            
        order_by : string, default 'id'
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.
              
        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с тематиками тв рекламы
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-thematic', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_custom_respondent_variable(self, ids=None, mart_type=None, name=None,
                                       order_by=None, order_dir=None, offset=0, limit=1000, use_cache=False,
                                       show_header=True):
        """
        Получение списка кастомных respondent переменных

        Parameters
        ----------

        ids : str or list of str
            Фильтр на список значений id

        mart_type : str
            Тип витрины

        name : str
            Фильтр на имя

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame со списком кастомных respondent переменных
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir,
            "ids": ids,
            "mart-type": mart_type,
            "name": name
        }

        body_params = {}

        return self._get_dict(entity_name='custom-respondent-variable', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              request_type='get', show_header=show_header)

    def add_custom_respondent_variable(self, resp, name, mart_type='mediavortex', is_public=True):
        """
        Создание кастомной respondent переменной

        Parameters
        ----------

        resp : json str или pandas dataframe
            Закодированные данные пользователей c информацией о фильтрах. Можно указать результат consumption target

        name : str
            Имя переменной

        mart_type : str
            Тип витрины

        is_public : bool
            Флаг доступности

        Returns
        -------
            Информация о созданной переменной
        """

        if isinstance(resp, pd.DataFrame):
            resp = json.dumps(utils.get_dict_from_dataframe(resp))

        data = {
            "value": resp,
            "name": name,
            "isPublic": is_public,
            "martType": mart_type
        }

        return self.msapi_network.send_request('post', self._urls['custom-respondent-variable'],
                                               data=json.dumps(data), use_cache=False)

    def delete_custom_respondent_variable(self, id_value):
        """
        Удаление кастомной respondent переменной

        Parameters
        ----------

        id_value : str
            id переменной

        Returns
        -------
            Информация о результатах удаления
        """

        return self.msapi_network.send_request('delete', self._urls['custom-respondent-variable'] + f"/{id_value}",
                                               use_cache=False)

    def get_tv_program_producer_country(self, ids=None, name=None, ename=None,
                                        order_by='id', order_dir=None, offset=None,
                                        limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов производства программ

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с типами производства программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "empty": True
        }

        return self._get_dict(entity_name='tv-program-producer-country', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_prime_time_status(self, ids=None, name=None, ename=None,
                                 order_by='id', order_dir=None, offset=None,
                                 limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию прайм-тайм статусов

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с прайм-тайм статусами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-prime-time-status', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_issue_status(self, ids=None, name=None, ename=None,
                            order_by='id', order_dir=None, offset=None,
                            limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию статусов выходов

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame со статусами выпусков
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-issue-status', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_breaks_style(self, ids=None, name=None, ename=None,
                            order_by='id', order_dir='DESC', offset=None,
                            limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию стилей блоков

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string, default 'DESC'
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame со стилями блоков
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-breaks-style', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_breaks_position(self, ids=None, name=None, ename=None,
                               order_by=None, order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов блоков

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        result : DataFrame

            DataFrame с типами блоков
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-breaks-position', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_breaks_distribution(self, ids=None, name=None, ename=None,
                                   order_by='type', order_dir=None, offset=None,
                                   limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов распространения блоков

        Parameters
        ----------
        ids : str or list of str
            Ид типа распространения для фильтрации (L, N, O, U)

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'type'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с типами распространения блоков
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "type": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-breaks-distribution', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_breaks_content(self, ids=None, name=None, ename=None,
                              order_by='type', order_dir=None, offset=None,
                              limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов содержания блоков

        Parameters
        ----------
        ids : str or list of str
            Ид содержания блока для фильтрации (A, C, P, S, U)

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'type'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с типами содержания блоков
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "type": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-breaks-content', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_area(self, ids=None, name=None, ename=None,
                    order_by='id', order_dir=None, offset=None,
                    limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию областей выходов

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с зонами компании тв рекламы
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "empty": True
        }

        return self._get_dict(entity_name='tv-area', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_ad_position(self, ids=None, name=None, ename=None,
                           order_by='type', order_dir=None, offset=None,
                           limit=None, use_cache=False, show_header=True):
        """
        Получить типы позиции клипа

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'type'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с типами позиции клипа
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "type": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-ad-position', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company_status(self, ids=None, name=None, ename=None,
                              order_by=None, order_dir=None, offset=None,
                              limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию статусов телекомпаний

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame со статусами компании тв рекламы
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "empty": True
        }

        return self._get_dict(entity_name='tv-company-status', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_producer(self, ids=None, name=None, ename=None,
                                order_by=None, order_dir=None, offset=None,
                                limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию производителей программ

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с производителями программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename,
            "empty": True
        }

        return self._get_dict(entity_name='tv-program-producer', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_program_group(self, ids=None, name=None, ename=None,
                             order_by='id', order_dir=None, offset=None,
                             limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию групповых имен программ

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с групповыми именами программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-program-group', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_no_yes_na(self, ids=None, name=None, ename=None,
                         order_by=None, order_dir=None, offset=None,
                         limit=None, use_cache=False, show_header=True):
        """
        Получить Да-Нет-Неизвестно флаги

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с Да-Нет-Неизвестно флагами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-no-yes-na', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_language(self, ids=None, name=None, ename=None,
                        order_by='id', order_dir=None, offset=None,
                        limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию языков программ

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с языками программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-language', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company_group(self, ids=None, name=None, ename=None,
                             order_by=None, order_dir=None, offset=None,
                             limit=None, use_cache=False, show_header=True):
        """
        Получить типы групп телекомпаний

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с типами групп телекомпаний
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-company-group', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_company_category(self, ids=None, name=None, ename=None,
                                order_by='id', order_dir=None, offset=None,
                                limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию категорий телекомпаний

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        ename : str or list of str
            Английское имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных. 
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных. 
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с категориями компаний тв рекламы
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict(entity_name='tv-company-category', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_age_restriction(self, ids=None, name=None,
                               order_by='id', order_dir=None, offset=None,
                               limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию возрастных ограничений

        Parameters
        ----------
        ids : str or list of str
            Ид для фильтрации

        name : str or list of str
            Имя для фильтрации

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с коллекцией возрастных ограничений
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name
        }

        return self._get_dict(entity_name='tv-age-restriction', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_availability_period(self, order_by='name', order_dir=None, offset=0, limit=1000):
        """
        Получить доступный период

        Parameters
        ----------

        order_by : string, default 'name'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        Returns
        -------
        data : DataFrame

            DataFrame с доступным периодом
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
        }

        url = self._urls['availability-period']
        query_dict = search_params
        if offset is not None and limit is not None:
            query_dict['offset'] = offset
            query_dict['limit'] = limit

        query = self._get_query(query_dict)
        if query is not None or len(query) > 0:
            url += query

        post_data = self._get_post_data(body_params)

        data = self.msapi_network.send_raw_request(
            'get', url, data=post_data)

        json_data = json.loads(data)

        # извлекаем все заголовки столбцов
        res_headers = []
        for item in json_data:
            for k, v in item.items():
                if k not in res_headers:
                    res_headers.append(k)

        # инициализируем списки данных столбцов
        res = {}
        for h in res_headers:
            res[h] = []

        # наполняем найденные столбцы значениями
        for item in json_data:
            for h in res_headers:
                if h in item.keys():
                    res[h].append(item[h])
                else:
                    res[h].append('')

        return pd.DataFrame(res)

    def get_respondent_analysis_unit(self, kit_id=1):
        """
        Получить списки доступных атрибутов отчета Анализ отдельных респондентов (Respondent analysis):
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками

        kit_id : int
            Id набора данных. Значение по умолчанию 1 (TV Index All Russia)
        """
        if not str(kit_id) in self.tv_units:
            print(f"Недоступны данные для kit_id={str(kit_id)}. Проверьте заданный kit_id")
        else:
            return self.tv_units.get(str(kit_id)).get('RespondentAnalysis')

    def get_tv_monitoring_cities(self, region_id=None, region_name=None, demo_attribute_value_id=None,
                                 demo_attribute_value_name=None, demo_attribute_id=None, demo_attribute_col_name=None,
                                 kit_id=None, order_by='regionId', order_dir=None, offset=None,
                                 limit=None, use_cache=False, return_city_ids_as_string=False, show_header=True):
        """
        Получить коллекцию связей регион-город

        Parameters
        ----------
        region_id : str or list of str
            Ид региона для фильтрации

        region_name : str or list of str
            Имя региона для фильтрации

        demo_attribute_value_id : str or list of str
            Ид значения демо атрибута для фильтрации

        demo_attribute_value_name : str or list of str
            Имя значения демо атрибута для фильтрации

        demo_attribute_id : str or list of str
            Ид демо атрибута для фильтрации

        demo_attribute_col_name : str or list of str
            Имя колонки демо атрибута для фильтрации

        kit_id : integer
            Ид набора данных

        order_by : string, default 'regionId'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        return_city_ids_as_string : bool
            Возврат результата, как строки с id городов. По умолчанию выключено (False).

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        result : DataFrame

            DataFrame с коллекцией связей регион-город
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "regionId": region_id,
            "regionName": region_name,
            "demoAttributeValueId": demo_attribute_value_id,
            "demoAttributeValueName": demo_attribute_value_name,
            "demoAttributeId": demo_attribute_id,
            "demoAttributeColName": demo_attribute_col_name,
            "kitId": kit_id
        }

        full_dict = self._get_dict(entity_name='monitoring-cities', search_params=search_params,
                                   body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                                   show_header=show_header)

        if return_city_ids_as_string and not full_dict.empty:
            return ", ".join([str(x) for x in full_dict['demoAttributeValueId'].unique().tolist()])
        else:
            return full_dict

    def get_tv_platform(self, ids=None, name=None, order_by='id',
                        order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию платформ

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов платформ

        name : str or list of str
            Поиск по имени платформы

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с местами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name
        }

        return self._get_dict(entity_name='tv-platform', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)

    def get_tv_playbacktype(self, ids=None, name=None, order_by='id',
                            order_dir=None, offset=None, limit=None, use_cache=False, show_header=True):
        """
        Получить коллекцию типов плейбеков

        Parameters
        ----------
        ids : str or list of str
            Поиск по списку идентификаторов типов плейбеков

        name : str or list of str
            Поиск по имени типа плейбека

        order_by : string, default 'id'
            Поле, по которому происходит сортировка

        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.

        offset : int
            Смещение от начала набора отобранных данных.
            Используется в связке с параметром 'limit': в случае использования одного параметра, другой также должен быть задан.

        limit : int
            Количество записей в возвращаемом наборе данных.
            Используется в связке с параметром 'offset': в случае использования одного параметра, другой также должен быть задан.
            Если смещение не требуется, то в 'offset' может быть передан 0.

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        show_header : bool
            Вывод информации о количестве загруженных записей. По умолчанию включено (True).

        Returns
        -------
        media : DataFrame

            DataFrame с местами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "name": name
        }

        return self._get_dict(entity_name='tv-playbacktype', search_params=search_params,
                              body_params=body_params, offset=offset, limit=limit, use_cache=use_cache,
                              show_header=show_header)
