import os
import json
from io import StringIO

import pandas as pd
from ..core import net


class CounterCats:
    _urls = {
        'ad-campaigns': '/dictionary/ad-campaigns',
        'area-type': '/dictionary/area-type'
    }

    def __new__(cls, facility_id=None, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(CounterCats, cls).__new__(
                cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id=None, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)

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

    @staticmethod
    def _print_header(header, offset, limit):
        if type(header) != dict or 'total' not in header:
            return
        total = header["total"]
        print(
            f'Запрошены записи: {offset} - {offset + limit}\nВсего найдено записей: {total}\n')
    
    def _get_dict(self, entity_name, search_params=None, body_params=None, offset=None, limit=None,
                  use_cache=True, request_type='post'):
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

        data = self.msapi_network.send_request_lo(
            request_type, url, data=post_data, use_cache=use_cache)

        if data is None or type(data) != dict:
            return None

        if 'header' not in data or 'data' not in data:
            return None
        
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
        if offset is not None and limit is not None:
            self._print_header(data['header'], offset, limit)
        else:
            self._print_header(data['header'], 0, data['header']['total'])
        return pd.DataFrame(res)

    def get_adcampaigns(self, advertisement_ids=None, advertisement_names=None, advertisement_campaign_ids=None,
                         advertisement_campaign_names=None, brand_ids=None, brand_names=None,
                         advertisement_agency_ids=None, advertisement_agency_names=None, tmsecs=None, order_by=None,
                         order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить рекламные кампании

        Parameters
        ----------
        advertisement_ids : list of int
            Поиск по списку идентификаторов рекламы

        advertisement_names : list of str
            Поиск по списку названий рекламы

        advertisement_campaign_ids : list of int
            Поиск по списку идентификаторов рекламных кампаний

        advertisement_campaign_names : list of str
            Поиск по списку названий рекламных кампаний
        
        brand_ids : list of int
            Поиск по списку идентификаторов брендов

        brand_names : list of str
            Поиск по списку имен брендов
        
        advertisement_agency_ids : list of int
            Поиск по списку идентификаторов рекламных агенств

        advertisement_agency_names : list of str
            Поиск по списку названий рекламных агентств

        tmsecs : list of str
            Поиск по списку названий tmsec
            
        order_by : string
            Поле, по которому происходит сортировка
            
        order_dir : string
            Направление сортировки данных. Возможные значения ASC - по возрастанию и DESC - по убыванию.      
              
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

            DataFrame с рекламными кампаниями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "advertisementId": advertisement_ids,
            "advertisementName": advertisement_names,
            "advertisementCampaignId": advertisement_campaign_ids,
            "advertisementCampaignName": advertisement_campaign_names,
            "brandId": brand_ids,
            "brandName": brand_names,
            "advertisementAgencyId": advertisement_agency_ids,
            "advertisementAgencyName": advertisement_agency_names,
            "tmsec": tmsecs
        }

        return self._get_dict('ad-campaigns', search_params, body_params, offset, limit, use_cache)

    def get_areatype(self, use_cache=False):
        """
        Получить типы размещения счетчика

        Parameters
        ----------
        use_cache : bool
            Использовать кэширование: True - да, False - нет
            Если опция включена (True), метод при первом получении справочника
            сохраняет его в кэш на локальном диске, а при следующих запросах этого же справочника
            с такими же параметрами - читает его из кэша, это позволяет существенно ускорить
            получение данных.

        Returns
        -------
        media : DataFrame

            DataFrame с типами размещения счетчика
        """
        return pd.DataFrame(self.msapi_network.send_request(
            'get', self._urls['area-type'], data={}, use_cache=use_cache))
