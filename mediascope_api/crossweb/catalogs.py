import os
import json
import pandas as pd
from ..core import net


class CrossWebCats:
    _dictionary_urls = {
        'media': '/dictionary/common/media-tree',
        'theme': '/dictionary/common/theme',
        'holding': '/dictionary/common/holding',
        'product': '/dictionary/common/product',
        'resource': '/dictionary/common/resource',
        'agency': '/dictionary/common/advertisement-agency',
        'brand': '/dictionary/common/brand',
        'campaign': '/dictionary/common/advertisement-campaign',
        'ad': '/dictionary/common/advertisement-tree'
    }

    def __new__(cls, facility_id=None, settings_filename=None, cache_path=None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(CrossWebCats, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id=None, settings_filename=None, cache_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path)
        self.usetypes = self.get_usetype()
        self.demo_attribs = self.load_property()
        self.media_attribs = self.load_media_property()
        self.themes = self.get_theme()
        self.holdings = self.get_holding()
        self.resources = self.get_resource()
        self.products = self.get_product()
        self.units = self.get_media_unit()
        self.units_total = self.get_media_total_unit()
        self.units_ad = self.get_ad_unit()


    def load_property(self):
        """
        Загрузить список переменных: все, по id или поиском по названию

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request_lo('get', '/dictionary/common/property/full', use_cache=True, limit=1000)
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
        data = self.msapi_network.send_request_lo('get', '/dictionary/media/property/full', use_cache=True, limit=1000)
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

    def _get_dict(self, entity_name, search_params=None, body_params=None, offset=None, limit=None):
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
        if self._dictionary_urls.get(entity_name) is None:
            return None

        url = self._dictionary_urls[entity_name]
        query_dict = search_params
        if offset is not None and limit is not None:
            query_dict['offset'] = offset
            query_dict['limit'] = limit

        query = self._get_query(query_dict)
        if query is not None or len(query) > 0:
            url += query

        post_data = self._get_post_data(body_params)

        data = self.msapi_network.send_request_lo('post', url, data=post_data, use_cache=True)
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

    def get_media(self, product=None, holding=None, theme=None, resource=None,
                  product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                  offset=None, limit=None):
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

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        media : DataFrame

            DataFrame с объектами Медиа-дерева
        """

        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids
        }

        return self._get_dict('media', search_params, body_params, offset, limit)

    def get_theme(self, product=None, holding=None, theme=None, resource=None,
                  product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                  offset=None, limit=None):
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

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        products : DataFrame

            DataFrame с Тематиками
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids
        }

        return self._get_dict('theme', search_params, body_params, offset, limit)

    def get_holding(self, product=None, holding=None, theme=None, resource=None,
                    product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                    offset=None, limit=None):
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

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        products : DataFrame

            DataFrame с Холдингами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids
        }

        return self._get_dict('holding', search_params, body_params, offset, limit)

    def get_resource(self, product=None, holding=None, theme=None, resource=None,
                     product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                     offset=None, limit=None):
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

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        products : DataFrame

            DataFrame с найденными ресурсами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme}

        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids
        }

        return self._get_dict('resource', search_params, body_params, offset, limit)

    def get_product(self, product=None, holding=None, theme=None, resource=None,
                    product_ids=None, holding_ids=None, resource_ids=None, theme_ids=None,
                    offset=None, limit=None):
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

        product_ids : list
            Поиск по списку идентификаторов продуктов.

        holding_ids : list
            Поиск по списку идентификаторов холдингов.

        resource_ids : list
            Поиск по списку идентификаторов ресурсов.

        theme_ids : list
            Поиск по списку идентификаторов тематик.

        offset : int
            Смещение от начала набора отобранных данных

        limit : int
            Количество записей в возвращаемом наборе данных

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'productName': product,
                         'holdingName': holding,
                         'resourceName': resource,
                         'themeName': theme}
        body_params = {
            'productIds': product_ids,
            'holdingIds': holding_ids,
            'resourceIds': resource_ids,
            'themeIds': theme_ids
        }
        return self._get_dict('product', search_params, body_params, offset, limit)

    def get_ad_agency(self, agency=None, brand=None, campaign=None, ad=None,
                      agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                      offset=None, limit=None):
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

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'agencyName': agency,
                         'brandName': brand,
                         'campaignName': campaign,
                         'adName': ad}
        body_params = {
            'agencyIds': agency_ids,
            'brandIds': brand_ids,
            'campaignIds': campaign_ids,
            'adIds': ad_ids
        }
        return self._get_dict('agency', search_params, body_params, offset, limit)

    def get_brand(self, agency=None, brand=None, campaign=None, ad=None,
                  agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                  offset=None, limit=None):
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

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'agencyName': agency,
                         'brandName': brand,
                         'campaignName': campaign,
                         'adName': ad}
        body_params = {
            'agencyIds': agency_ids,
            'brandIds': brand_ids,
            'campaignIds': campaign_ids,
            'adIds': ad_ids
        }
        return self._get_dict('brand', search_params, body_params, offset, limit)

    def get_ad_campaign(self, agency=None, brand=None, campaign=None, ad=None,
                        agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
                        offset=None, limit=None):
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

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'agencyName': agency,
                         'brandName': brand,
                         'campaignName': campaign,
                         'adName': ad}
        body_params = {
            'agencyIds': agency_ids,
            'brandIds': brand_ids,
            'campaignIds': campaign_ids,
            'adIds': ad_ids
        }
        return self._get_dict('campaign', search_params, body_params, offset, limit)

    def get_ad(self, agency=None, brand=None, campaign=None, ad=None,
               agency_ids=None, brand_ids=None, campaign_ids=None, ad_ids=None,
               offset=None, limit=None):
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

        Returns
        -------
        products : DataFrame

            DataFrame с продуктами
        """
        search_params = {'agencyName': agency,
                         'brandName': brand,
                         'campaignName': campaign,
                         'adName': ad}
        body_params = {
            'agencyIds': agency_ids,
            'brandIds': brand_ids,
            'campaignIds': campaign_ids,
            'adIds': ad_ids
        }
        return self._get_dict('ad', search_params, body_params, offset, limit)

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
        return self.msapi_network.send_request('get', '/unit/media', use_cache=False)

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
        return self.msapi_network.send_request('get', '/unit/advertisement', use_cache=False)

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
        return self.msapi_network.send_request('get', '/unit/media-total', use_cache=False)

    def get_usetype(self):
        """
        Получить списки доступных для использования в заданиях:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        data = self.msapi_network.send_request_lo('get', '/dictionary/common/use-type', use_cache=True)
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

    def get_date_range(self):
        """
        Получить списки доступных периодов данных

        Returns
        -------
        info : dict
            Словарь с доступными периодами
        """
        data = self.msapi_network.send_request_lo('get', '/dictionary/common/availability', use_cache=True)
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
            if str(item['name']).lower() != 'media':
                continue
            # print(item)
            # print(type(item))
            res['id'].append(item['id'])
            res['name'].append(item['name'])
            res['from'].append(item['periodFrom'])
            res['to'].append(item['periodTo'])

        return pd.DataFrame(res)

