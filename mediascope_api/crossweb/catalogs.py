import os
import json
import pandas as pd
from ..core import net


class CrossWebCats:
    _urls = {
        'media': '/dictionary/common/media-tree',
        'theme': '/dictionary/common/theme',
        'resource_theme': '/dictionary/common/resource-theme',
        'holding': '/dictionary/common/holding',
        'product': '/dictionary/common/product',
        'resource': '/dictionary/common/resource',
        'agency': '/dictionary/common/profile-agency',
        'brand': '/dictionary/common/brand',
        'campaign': '/dictionary/common/profile-campaign',
        'ad': '/dictionary/common/profile-tree',
        'property': '/dictionary/common/property/full',
        'media_property': '/dictionary/media/property/full',
        'monitoring_property': '/dictionary/monitoring/property/full',
        'media_duplication_property': '/dictionary/media-duplication/property/full',
        'profile_duplication_property': '/dictionary/profile-duplication/property/full',
        'media_unit': '/unit/media',
        'ad_unit': '/unit/profile',
        'total_unit': '/unit/media-total',
        'monitoring_unit': '/unit/monitoring',
        'media_duplication_unit': '/unit/media-duplication',
        'media_profile_unit': '/unit/media-profile',
        'profile_duplication_unit': '/unit/profile-duplication',
        'usetype': '/dictionary/common/use-type',
        'media_usetype': '/dictionary/media/use-type',
        'media_total_usetype': '/dictionary/media-total/use-type',
        'media_duplication_usetype': '/dictionary/media-duplication/use-type',
        'profile_usetype': '/dictionary/profile/use-type',
        'monitoring_usetype': '/dictionary/monitoring/use-type',
        'profile_duplication_usetype': '/dictionary/profile-duplication/use-type',
        'date_range': '/dictionary/common/availability',
        'product_brand': '/dictionary/common/product-brand',
        'product_category_l1': '/dictionary/common/product-category-l1',
        'product_category_l2': '/dictionary/common/product-category-l2',
        'product_category_l3': '/dictionary/common/product-category-l3',
        'product_category_l4': '/dictionary/common/product-category-l4',
        'product_model': '/dictionary/common/product-model',
        'product_subbrand': '/dictionary/common/product-subbrand',
        'ad_source_type': '/dictionary/common/ad-source-type',
        'ad_network': '/dictionary/common/ad-network',
        'ad_placement': '/dictionary/common/ad-placement',
        'ad_player': '/dictionary/common/ad-player',
        'ad_server': '/dictionary/common/ad-server',
        'ad_video_utility': '/dictionary/common/ad-video-utility',
        'advertiser': '/dictionary/common/advertiser',
        'monitoring': '/dictionary/common/monitoring-link-tree',
        'product-category-tree': '/dictionary/common/product-category-tree',
        'ad_list': '/dictionary/common/profile',
        'monitoring_holding': '/dictionary/monitoring/holding',
        'monitoring_product': '/dictionary/monitoring/product',
        'monitoring_resource': '/dictionary/monitoring/resource',
        'monitoring_resource_theme': '/dictionary/monitoring/resource-theme',
        'monitoring_theme': '/dictionary/monitoring/theme',
        'monitoring_media_tree': '/dictionary/monitoring/media-tree'
    }

    def __new__(cls, facility_id=None, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(CrossWebCats, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id=None, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.usetypes = self.get_usetype()
        self.demo_attribs = self.load_property()
        self.media_attribs = self.load_media_property()
        self.monitoring_attribs = self.load_monitoring_property()
        self.media_duplication_attribs = self.load_media_duplication_property()
        self.themes = self.get_theme()
        self.resource_themes = self.get_resource_theme()
        self.holdings = self.get_holding()
        self.resources = self.get_resource()
        self.products = self.get_product()
        self.units = self.get_media_unit()
        self.units_total = self.get_media_total_unit()
        self.units_ad = self.get_ad_unit()
        self.units_monitoring = self.get_monitoring_unit()
        self.units_media_duplication = self.get_media_duplication_unit()
        #self.product_brands = self.get_product_brand()
        #self.product_category_l1 = self.get_product_category_l1()
        #self.product_category_l2 = self.get_product_category_l2()
        #self.product_category_l3 = self.get_product_category_l3()
        #self.product_category_l4 = self.get_product_category_l4()
        #self.product_model = self.get_product_model()
        #self.product_subbrand = self.get_product_subbrand()
        self.ad_network = self.get_ad_network()
        self.ad_placement = self.get_ad_placement()
        self.ad_player = self.get_ad_player()
        self.ad_server = self.get_ad_server()
        self.ad_source_type = self.get_ad_source_type()
        self.ad_video_utility = self.get_ad_video_utility()

    def load_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', self._urls['property'], use_cache=True, limit=1000)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['entityTitle'] = []
        res['optionValue'] = []
        res['optionName'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['entityTitle'].append(item['entityTitle'])
            res['optionValue'].append(item['optionValue'])
            if item['hasOptions']:
                res['optionName'].append(item['optionName'])
            else:
                res['optionName'].append('')
        return pd.DataFrame(res)

    def load_media_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_property'], use_cache=True, limit=1000)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['entityTitle'] = []
        res['optionValue'] = []
        res['optionName'] = []
        res['sliceUnit'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['entityTitle'].append(item['entityTitle'])
            res['optionValue'].append(item['optionValue'])
            res['sliceUnit'].append(item['sliceUnit'])
            if item['hasOptions']:
                res['optionName'].append(item['optionName'])
            else:
                res['optionName'].append('')
        return pd.DataFrame(res)
    
    def load_monitoring_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', self._urls['monitoring_property'], use_cache=True, limit=1000)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['entityTitle'] = []
        res['optionValue'] = []
        res['optionName'] = []
        res['sliceUnit'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['entityTitle'].append(item['entityTitle'])
            res['optionValue'].append(item['optionValue'])
            res['sliceUnit'].append(item['sliceUnit'])
            if item['hasOptions']:
                res['optionName'].append(item['optionName'])
            else:
                res['optionName'].append('')
        return pd.DataFrame(res)
    
    def load_media_duplication_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_duplication_property'], use_cache=True, limit=1000)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['entityTitle'] = []
        res['optionValue'] = []
        res['optionName'] = []
        res['sliceUnit'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['entityTitle'].append(item['entityTitle'])
            res['optionValue'].append(item['optionValue'])
            res['sliceUnit'].append(item['sliceUnit'])
            if item['hasOptions']:
                res['optionName'].append(item['optionName'])
            else:
                res['optionName'].append('')
        return pd.DataFrame(res)

    def load_profile_duplication_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', self._urls['profile_duplication_property'], use_cache=True, limit=1000)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['entityTitle'] = []
        res['optionValue'] = []
        res['optionName'] = []
        res['sliceUnit'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['entityTitle'].append(item['entityTitle'])
            res['optionValue'].append(item['optionValue'])
            res['sliceUnit'].append(item['sliceUnit'])
            if item['hasOptions']:
                res['optionName'].append(item['optionName'])
            else:
                res['optionName'].append('')
        return pd.DataFrame(res)

    def get_property(self, with_id=False):
        """
        Получить полный каталог Демографических и Географических переменных


        Parameters
        ----------

        with_id : bool
            Флаг отвечающий за отображение id переменной,
            По умолчанию: False

        Returns
        -------

        result : DataFrame

            Демографические и Географические переменные

        """
        if with_id:
            return self.demo_attribs
        else:
            return self.demo_attribs.drop(columns=['id'])

    def find_property(self, text, expand=True, with_id=False):
        """
        Поиск по каталогу Демографических и Географических переменных


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
        df = self.demo_attribs

        df['id'] = df['id'].astype(str)
        df['optionValue'] = df['optionValue'].astype(str)

        if not expand:
            df = df[['id', 'name', 'entityTitle']].drop_duplicates()

        df_found = df[df['id'].str.contains(text, case=False) |
                      df['name'].str.contains(text, case=False) |
                      df['entityTitle'].str.contains(text, case=False)
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
                data[k] = []
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
        if slice_name == "adDescription" or slice_name == "eventDescription":
            if type(self.units_monitoring) != dict or \
                    self.units_monitoring.get('slices', None) is None or \
                    self.units_monitoring['slices'].get(slice_name, None) is None:
                return
            return self.units_monitoring['slices'][slice_name]
        else:
            if slice_name == "duplicationMart":
                if type(self.units_monitoring) != dict or \
                    self.units_media_duplication.get('slices', None) is None or \
                    self.units_media_duplication['slices'].get(slice_name, None) is None:
                    return
                return self.units_media_duplication['slices'][slice_name]
            else:            
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
        print(f'Запрошены записи: {offset} - {offset + limit}\nВсего найдено записей: {total}\n')

    def _get_dict(self, entity_name, search_params=None, body_params=None, offset=None, limit=None, use_cache=True):
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
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        products : DataFrame

            DataFrame с объектами словаря
        """
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

        data = self.msapi_network.send_request_lo('post', url, data=post_data, use_cache=use_cache)
        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None

        res = {}
        for item in data['data']:
            for k, v in item.items():
                if k not in res:
                    res[k] = []
                res[k].append(v)
        # print header
        if offset is not None and limit is not None:
            self._print_header(data['header'], offset, limit)
        else:
            self._print_header(data['header'], 0, data['header']['total'])
        return pd.DataFrame(res)

    def get_media(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                  product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                  resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список объектов Медиа-дерева

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        media : DataFrame

            DataFrame с объектами Медиа-дерева
        """

        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('media', search_params, body_params, offset, limit, use_cache)

    def get_theme(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                  product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                  resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список тематик

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Тематиками
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('theme', search_params, body_params, offset, limit, use_cache)

    def get_resource_theme(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                           product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                           resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список тематик для ресурсов

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Тематиками ресурсов
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('resource_theme', search_params, body_params, offset, limit, use_cache)

    def get_holding(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                    product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                    resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список холдингов

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Холдингами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('holding', search_params, body_params, offset, limit, use_cache)

    def get_resource(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                     product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                     resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список ресурсов

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с найденными ресурсами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('resource', search_params, body_params, offset, limit, use_cache)

    def get_product(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                    product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                    resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список продуктов

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }
        return self._get_dict('product', search_params, body_params, offset, limit, use_cache)

    def get_ad_agency(self, agency=None, brand=None, campaign=None, ad=None,
                      agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                      offset=None, limit=None, use_cache=True):
        """
        Получить список рекламных агентств

        Parameters
        ----------

        agency : str
            Поиск по названию агентства. Допускается задавать часть названия.

        brand : str
            Поиск по названию бренда. Допускается задавать часть названия.

        campaign : str
            Поиск по названию рекламной кампании. Допускается задавать часть названия.

        ad : str
            Поиск по названию рекламной позиции. Допускается задавать часть названия.

        agency_ids : list
            Поиск по списку идентификаторов агентств.

        brand_ids : list
            Поиск по списку идентификаторов брендов.

        campaign_ids : list
            Поиск по списку идентификаторов рекламных кампаний.

        ad_ids : list
            Поиск по списку идентификаторов рекламных позиций.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'advertisementAgencyName': agency,
                         'brandName': brand,
                         'advertisementCampaignName': campaign,
                         'advertisementName': ad}
        body_params = {
            'advertisementAgencyIds': agency_ids,
            'brandIds': brand_ids,
            'advertisementCampaignIds': campaign_ids,
            'advertisementIds': ad_ids
        }
        return self._get_dict('agency', search_params, body_params, offset, limit, use_cache)

    def get_brand(self, agency=None, brand=None, campaign=None, ad=None,
                  agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                  offset=None, limit=None, use_cache=True):
        """
        Получить список брендов

        Parameters
        ----------

        agency : str
            Поиск по названию агентства. Допускается задавать часть названия.

        brand : str
            Поиск по названию бренда. Допускается задавать часть названия.

        campaign : str
            Поиск по названию рекламной кампании. Допускается задавать часть названия.

        ad : str
            Поиск по названию рекламной позиции. Допускается задавать часть названия.

        agency_ids : list
            Поиск по списку идентификаторов агентств.

        brand_ids : list
            Поиск по списку идентификаторов брендов.

        campaign_ids : list
            Поиск по списку идентификаторов рекламных кампаний.

        ad_ids : list
            Поиск по списку идентификаторов рекламных позиций.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'advertisementAgencyName': agency,
                         'brandName': brand,
                         'advertisementCampaignName': campaign,
                         'advertisementName': ad}
        body_params = {
            'advertisementAgencyIds': agency_ids,
            'brandIds': brand_ids,
            'advertisementCampaignIds': campaign_ids,
            'advertisementIds': ad_ids
        }
        return self._get_dict('brand', search_params, body_params, offset, limit, use_cache)

    def get_ad_campaign(self, agency=None, brand=None, campaign=None, ad=None,
                        agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                        offset=None, limit=None, use_cache=True):
        """
        Получить список рекламных кампаний

        Parameters
        ----------

        agency : str
            Поиск по названию агентства. Допускается задавать часть названия.

        brand : str
            Поиск по названию бренда. Допускается задавать часть названия.

        campaign : str
            Поиск по названию рекламной кампании. Допускается задавать часть названия.

        ad : str
            Поиск по названию рекламной позиции. Допускается задавать часть названия.

        agency_ids : list
            Поиск по списку идентификаторов агентств.

        brand_ids : list
            Поиск по списку идентификаторов брендов.

        campaign_ids : list
            Поиск по списку идентификаторов рекламных кампаний.

        ad_ids : list
            Поиск по списку идентификаторов рекламных позиций.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'advertisementAgencyName': agency,
                         'brandName': brand,
                         'advertisementCampaignName': campaign,
                         'advertisementName': ad}
        body_params = {
            'advertisementAgencyIds': agency_ids,
            'brandIds': brand_ids,
            'advertisementCampaignIds': campaign_ids,
            'advertisementIds': ad_ids
        }
        return self._get_dict('campaign', search_params, body_params, offset, limit, use_cache)

    def get_ad(self, agency=None, brand=None, campaign=None, ad=None,
               agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
               offset=None, limit=None, use_cache=True):
        """
        Получить список рекламных позиций

        Parameters
        ----------

        agency : str
            Поиск по названию агентства. Допускается задавать часть названия.

        brand : str
            Поиск по названию бренда. Допускается задавать часть названия.

        campaign : str
            Поиск по названию рекламной кампании. Допускается задавать часть названия.

        ad : str
            Поиск по названию рекламной позиции. Допускается задавать часть названия.

        agency_ids : list
            Поиск по списку идентификаторов агентств.

        brand_ids : list
            Поиск по списку идентификаторов брендов.

        campaign_ids : list
            Поиск по списку идентификаторов рекламных кампаний.

        ad_ids : list
            Поиск по списку идентификаторов рекламных позиций.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'advertisementAgencyName': agency,
                         'brandName': brand,
                         'advertisementCampaignName': campaign,
                         'advertisementName': ad}
        body_params = {
            'advertisementAgencyIds': agency_ids,
            'brandIds': brand_ids,
            'advertisementCampaignIds': campaign_ids,
            'advertisementIds': ad_ids
        }
        return self._get_dict('ad', search_params, body_params, offset, limit, use_cache)

    def get_media_unit(self):
        """
        Получить списки доступных для использования в заданиях для медиа:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['media_unit'], use_cache=False)

    def get_ad_unit(self):
        """
        Получить списки доступных для использования в заданиях для рекламы:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['ad_unit'], use_cache=False)

    def get_media_total_unit(self):
        """
        Получить списки доступных для использования в заданиях для медиа-тотал:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['total_unit'], use_cache=False)
    
    def get_monitoring_unit(self):
        """
        Получить списки доступных для использования в заданиях для мониторинга:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['monitoring_unit'], use_cache=False)
    
    def get_media_duplication_unit(self):
        """
        Получить списки доступных для использования в заданиях для пересечений:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['media_duplication_unit'], use_cache=False)

    def get_profile_duplication_unit(self):
        """
        Получить списки доступных для использования в заданиях для пересечений profile duplication:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['profile_duplication_unit'], use_cache=False)

    def get_media_profile_unit(self):
        """
        Получить списки доступных для использования в заданиях для расчета совокупных данных по Profile и Media:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        return self.msapi_network.send_request('get', self._urls['media_profile_unit'], use_cache=False)

    def get_usetype(self):
        """
        Получить списка usetype

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['usetype'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_date_range(self, ids=None, name=None):
        """
        Получить списки доступных периодов данных

        Returns
        -------
        info : dict
            Словарь с доступными периодами
        """
        data = self.msapi_network.send_request_lo('get', self._urls['date_range'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []
        res['from'] = []
        res['to'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['from'].append(item['periodFrom'])
            res['to'].append(item['periodTo'])

        df = pd.DataFrame(res)

        if ids is not None:
            df = df[df['id'].isin(ids)]

        if name is not None:
            df = df[df['name'].str.contains(name, case=False)]

        return df

    def get_product_brand(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список товарных брендов

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с товарными брендами
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_brand', search_params, body_params, offset, limit, use_cache)
    
    def get_product_category_l1(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список категорий товаров и услуг (уровень 1)

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с категориями товаров и услуг (уровень 1)
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_category_l1', search_params, body_params, offset, limit, use_cache)
    
    def get_product_category_l2(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список категорий товаров и услуг (уровень 2)

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с категориями товаров и услуг (уровень 2)
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_category_l2', search_params, body_params, offset, limit, use_cache)
    
    def get_product_category_l3(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список категорий товаров и услуг (уровень 3)

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с категориями товаров и услуг (уровень 3)
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_category_l3', search_params, body_params, offset, limit, use_cache)
    
    def get_product_category_l4(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список категорий товаров и услуг (уровень 4)

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с категориями товаров и услуг (уровень 4)
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_category_l4', search_params, body_params, offset, limit, use_cache)
    
    def get_product_model(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список товарных моделей

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с товарными моделями
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_model', search_params, body_params, offset, limit, use_cache)
    
    def get_product_subbrand(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список товарных суббрендов

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с товарными суббрендами
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }

        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('product_subbrand', search_params, body_params, offset, limit, use_cache)
    
    def get_ad_network(self):
        """
        Получить список рекламных сетей

        Returns
        -------
        products : DataFrame

            DataFrame с рекламными сетями
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_network'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_ad_placement(self):
        """
        Получить список мест размещений для рекламы в социальных сетях

        Returns
        -------
        products : DataFrame

            DataFrame с местами размещения
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_placement'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_ad_player(self):
        """
        Получить список рекламных плееров

        Returns
        -------
        products : DataFrame

            DataFrame с рекламными плеерами
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_player'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_ad_server(self):
        """
        Получить список рекламных серверов

        Returns
        -------
        products : DataFrame

            DataFrame с рекламными серверами
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_server'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_ad_source_type(self):
        """
        Получить список типов рекламы

        Returns
        -------
        products : DataFrame

            DataFrame с типами рекламы
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_source_type'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_ad_video_utility(self):
        """
        Получить список принадлежности к видео

        Returns
        -------
        products : DataFrame

            DataFrame с принадлежностью к видео
        """
        data = self.msapi_network.send_request_lo('get', self._urls['ad_video_utility'], use_cache=True)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)
    
    def get_product_advertiser(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список рекламодателей

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с рекламодателями
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }
        
        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }
        return self._get_dict('advertiser', search_params, body_params, offset, limit, use_cache)
    
    def get_monitoring(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить дерево связей рекламных единиц контента в мониторинге

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с деревом связей рекламных единиц контента в мониторинге
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }
        
        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }

        return self._get_dict('monitoring', search_params, body_params, offset, limit, use_cache)
    
    def get_product_category_tree(self, advertiser=None, advertiser_eng=None, product_model=None, product_model_eng=None, product_brand=None, product_brand_eng=None,
            product_subbrand=None, product_subbrand_eng=None, product_category_l1=None, product_category_l1_eng=None, product_category_l2=None,
            product_category_l2_eng=None, product_category_l3=None, product_category_l3_eng=None, product_category_l4=None, product_category_l4_eng=None, 
            advertiser_ids=None, product_model_ids=None, product_brand_ids=None, product_subbrand_ids=None, product_category_l1_ids=None, product_category_l2_ids=None,
            product_category_l3_ids=None, product_category_l4_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить дерево категорий товаров и услуг

        Parameters
        ----------

        advertiser : str
            Поиск по названию рекламодателя. Допускается задавать часть названия.
            
        advertiser_eng : str
            Поиск по названию рекламодателя на английском языке. Допускается задавать часть названия.
        
        product_model : str
            Поиск по названию модели. Допускается задавать часть названия.
            
        product_model_eng : str
            Поиск по названию модели на английском языке. Допускается задавать часть названия.
                    
        product_brand : str
            Поиск по названию бренда. Допускается задавать часть названия.
                
        product_brand_eng : str
            Поиск по названию бренда на английском языке. Допускается задавать часть названия.
                
        product_subbrand : str
            Поиск по названию суббренда. Допускается задавать часть названия.

        product_subbrand_eng : str
            Поиск по названию суббренда на анг. Допускается задавать часть названия.
        
        product_category_l1 : str
            Поиск по названию категории товаров и услуг (уровень 1). Допускается задавать часть названия.
        
        product_category_l1_eng : str
            Поиск по названию категории товаров и услуг (уровень 1) на английском языке. Допускается задавать часть названия.
        
        product_category_l2 : str
            Поиск по названию категории товаров и услуг (уровень 2). Допускается задавать часть названия.
        
        product_category_l2_eng : str
            Поиск по названию категории товаров и услуг (уровень 2) на английском языке. Допускается задавать часть названия.
        
        product_category_l3 : str
            Поиск по названию категории товаров и услуг (уровень 3). Допускается задавать часть названия.
        
        product_category_l3_eng : str
            Поиск по названию категории товаров и услуг (уровень 3) на английском языке. Допускается задавать часть названия.
        
        product_category_l4 : str
            Поиск по названию категории товаров и услуг (уровень 4). Допускается задавать часть названия.
        
        product_category_l4_eng : str
            Поиск по названию категории товаров и услуг (уровень 4) на английском языке. Допускается задавать часть названия.
                                                
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей.
                                                
        product_model_ids : list
            Поиск по списку идентификаторов моделей.
                                                
        product_brand_ids : list
            Поиск по списку идентификаторов брендов.
                                                
        product_subbrand_ids : list
            Поиск по списку идентификаторов суббрендов.
                                                
        product_category_l1_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 1).
                                                
        product_category_l2_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 2).
                                                
        product_category_l3_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 3).
                                                
        product_category_l4_ids : list
            Поиск по списку идентификаторов категории товаров и услуг (уровень 4).

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с деревом категорий товаров и услуг
        """
        search_params = {
            'advertiserName': advertiser,
            'advertiserEngName': advertiser_eng,
            'productModelName': product_model,
            'productModelEngName': product_model_eng, 
            'productBrandName': product_brand,
            'productBrandEngName': product_brand_eng,
            'productSubbrandName': product_subbrand,
            'productSubbrandEngName': product_subbrand_eng,
            'productCategoryL1Name': product_category_l1,
            'productCategoryL1EngName': product_category_l1_eng,
            'productCategoryL2Name': product_category_l2,
            'productCategoryL2EngName': product_category_l2_eng,
            'productCategoryL3Name': product_category_l3,
            'productCategoryL3EngName': product_category_l3_eng,
            'productCategoryL4Name': product_category_l4, 
            'productCategoryL4EngName': product_category_l4_eng            
        }
        
        body_params = {
            'advertiserIds': advertiser_ids,
            'productModelIds': product_model_ids,
            'productBrandIds': product_brand_ids,
            'productSubbrandIds': product_subbrand_ids,
            'productCategoryL1Ids': product_category_l1_ids,
            'productCategoryL2Ids': product_category_l2_ids,
            'productCategoryL3Ids': product_category_l3_ids,
            'productCategoryL4Ids': product_category_l4_ids
        }

        return self._get_dict('product-category-tree', search_params, body_params, offset, limit, use_cache)

    def get_availability(self, ids=None, name=None, offset=None, limit=None, use_cache=True):
        """
        Получить список периодов доступных данных

        Parameters
        ----------

        ids : str
            Поиск по списку идентификаторов периодов

        name : str
            Параметр для поиска в имени сущности по like

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        availability : DataFrame

            DataFrame с периодами доступности данных
        """

        return self.msapi_network.send_request('get', self._urls['availability'], use_cache=False)

    def get_media_usetype(self):
        """
        Получить списка usetype для media

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_media_usetype(self):
        """
        Получить списка usetype для media

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_media_total_usetype(self):
        """
        Получить списка usetype для media_total

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_total_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_media_duplication_usetype(self):
        """
        Получить списка usetype для media duplication

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['media_duplication_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_profile_usetype(self):
        """
        Получить списка usetype для profile

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['profile_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_profile_duplication_usetype(self):
        """
        Получить списка usetype для profile duplication

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['profile_duplication_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_monitoring_usetype(self):
        """
        Получить списка usetype для monitoring

        Returns
        -------
        info : dataframe
            Датафрейм со списком
        """
        data = self.msapi_network.send_request_lo('get', self._urls['monitoring_usetype'], use_cache=False)
        res = {}
        if data is None or type(data) != dict:
            return None

        if 'data' not in data:
            return None

        res['id'] = []
        res['name'] = []

        for item in data['data']:
            res['id'].append(item['id'])
            res['name'].append(item['name'])

        return pd.DataFrame(res)

    def get_ad_list(self, agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                    offset=None, limit=None, use_cache=True):
        """
        Получить список реклам

        Parameters
        ----------
        agency_ids : list
            Поиск по списку идентификаторов агентств.

        brand_ids : list
            Поиск по списку идентификаторов брендов.

        campaign_ids : list
            Поиск по списку идентификаторов рекламных кампаний.

        ad_ids : list
            Поиск по списку идентификаторов рекламных позиций.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с рекламами
        """
        search_params = {}
        body_params = {
            'advertisementAgencyIds': agency_ids,
            'brandIds': brand_ids,
            'advertisementCampaignIds': campaign_ids,
            'advertisementIds': ad_ids
        }
        return self._get_dict('ad_list', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_theme(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                             product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                             resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список тематик для продуктов медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Тематиками
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('monitoring_theme', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_resource_theme(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                                      product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                                      resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список тематик для ресурсов медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Тематиками ресурсов
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('monitoring_resource_theme', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_holding(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                               product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                               resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список холдингов медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с Холдингами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('monitoring_holding', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_resource(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                                product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                                resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список ресурсов медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с найденными ресурсами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('monitoring_resource', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_product(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                               product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                               resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список продуктов медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }
        return self._get_dict('monitoring_product', search_params, body_params, offset, limit, use_cache)

    def get_monitoring_media(self, product=None, holding=None, theme=None, resource=None, resource_theme=None,
                             product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                             resource_theme_ids=None, offset=None, limit=None, use_cache=True):
        """
        Получить список объектов дерева медиа мониторинга

        Parameters
        ----------

        product : str
            Поиск по названию продукта. Допускается задавать часть названия.

        holding : str
            Поиск по названию холдинга. Допускается задавать часть названия.

        theme : str
            Поиск по названию тематики. Допускается задавать часть названия.

        resource : str
            Поиск по названию ресурса. Допускается задавать часть названия.

        resource_theme : str
            Поиск по названию тематики ресурса. Допускается задавать часть названия.

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        resource_theme_ids : list
            Поиск по списку идентификаторов тематик ресурсов.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        media : DataFrame

            DataFrame с объектами Медиа-дерева
        """

        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme,
                         'resourceThemeName': resource_theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids,
            'resourceThemeIds': resource_theme_ids
        }

        return self._get_dict('monitoring_media_tree', search_params, body_params, offset, limit, use_cache)