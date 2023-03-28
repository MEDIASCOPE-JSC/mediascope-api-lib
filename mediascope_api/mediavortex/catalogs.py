import json
import pandas as pd
from ..core import net


class MediaVortexCats:
    _urls = {
        'tv-kit': '/kit',
        'tv-tv-ad': '/dictionary/tv/tv-ad',
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
        'tv-prime-time': '/dictionary/tv/prime-time',
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
        'tv-program-prreg': '/dictionary/tv/program-prreg',
        'tv-monitoring-type': '/dictionary/tv/monitoring-type',
        'tv-db-rd-type': '/dictionary/tv/db-rd-type',
        'tv-ad-iss-sbtv': '/dictionary/tv/ad-iss-sbtv',
        'tv-demo-attribute': '/dictionary/tv/demo-attribute',
        'tv-program-country': '/dictionary/tv/program-country',
        'tv-company-holding': '/dictionary/tv/company-holding',
        'tv-company-media-holding': '/dictionary/tv/company-media-holding',
        'tv-company-thematic': '/dictionary/tv/thematic'
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
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path, cache_enabled, username, passw,
                                                      root_url, client_id, client_secret, keycloak_url)
        self.tv_demo_attribs = self.load_tv_property()

    def load_tv_property(self):
        """
        Загрузить список демо переменных

        Returns
        -------
        DemoAttribs : DataFrame

            DataFrame с демо переменными
        """
        data = self.get_tv_demo_attribute()
        # формируем столбец с именами срезов, относящихся к переменным (изменяем первую букву на строчную)
        data['entityName'] = data['colName'].str[0].str.lower() + data['colName'].str[1:]
        return data

    def find_tv_property(self, text, expand=True, with_id=False):
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

        data = self.msapi_network.send_request_lo(
            'post', url, data=post_data, use_cache=use_cache)

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

    def get_timeband_unit(self):
        """
        Получить списки доступных для использования в заданиях для timeband:
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

        stats = []
        slices = []
        filters = []
        if len(units) > 0:
            if 'reports' in units[0]:
                if len(units[0]['reports']) > 0:
                    if 'statistics' in units[0]['reports'][0]:
                        stats = [i['name'] for i in units[0]['reports'][0]['statistics']]
                    if 'slices' in units[0]['reports'][0]:
                        slices = [i['name'] for i in units[0]['reports'][0]['slices']]
                    if 'filters' in units[0]['reports'][0]:
                        filters = [i['name'] for i in units[0]['reports'][0]['filters']]

        result = {
            'statistics': stats,
            'slices': slices,
            'filters': filters
        }
        
        return result
    
    def get_simple_unit(self):
        """
        Получить списки доступных для использования в заданиях для simple:
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

        stats = []
        slices = []
        filters = []
        if len(units) > 0:
            if 'reports' in units[0]:
                if len(units[0]['reports']) > 1:
                    if 'statistics' in units[0]['reports'][1]:
                        stats = [i['name'] for i in units[0]['reports'][1]['statistics']]
                    if 'slices' in units[0]['reports'][1]:
                        slices = [i['name'] for i in units[0]['reports'][1]['slices']]
                    if 'filters' in units[0]['reports'][1]:
                        filters = [i['name'] for i in units[0]['reports'][1]['filters']]

        result = {
            'statistics': stats,
            'slices': slices,
            'filters': filters
        }        

        return result
    
    def get_crosstab_unit(self):
        """
        Получить списки доступных для использования в заданиях для crosstab:
        - статистик
        - срезов
        - фильтров

        Returns
        -------
        info : dict
            Словарь с доступными списками
        """
        
        result = {
            'statistics': ['SampleAvg', 'Universe000', 'Rtg000', 'Rtg000Avg', 'RtgPerAvg', 'Rtg000Sum', 'RtgPerSum',
                           'TAud', 'TAudAvg', 'TAudSum', 'ShareAvg', 'DurationAvg', 'DurationSum', 'Interval',
                           'RespondentCount', 'Day', 'SummedEventWeight', 'AvReach000', 'AvReachPer', 'TgAffinPer',
                           'TTVTAud', 'TTVRtg000Sum', 'TTVRtg000Avg', 'TTVRtgPerSum', 'TTVRtgPerAvg', 'SummedWeight',
                           'CrossTabSummedWeight', 'Universe000Avg', 'CumReach000', 'CumReachPer', 'ATVSum', 'ATVAvg',
                           'Quantity', 'QuantitySum', 'CostByMinSum', 'CostByGRPSum', 'ConsolidatedCostSum',
                           'ATVReachAvg', 'ATVReachSum', 'TgAffinPerAvg', 'OTS', 'GRP'],
            'slices': ['researchDate', 'researchWeek', 'researchMonth', 'researchQuarter', 'researchYear',
                       'researchHalfYear', 'researchWeekDay', 'researchDayType', 'locationId', 'locationName',
                       'locationEName', 'timeBand1', 'timeBand5', 'timeBand10', 'timeBand15', 'timeBand30',
                       'timeBand60', 'tvCompanyId', 'tvCompanyGroupId', 'tvCompanyCategoryId', 'tvCompanyName',
                       'tvCompanyEName', 'tvCompanyDescription', 'tvCompanyInformation', 'tvCompanyNotes',
                       'tvThematicID', 'tvCompanyHoldingID', 'tvCompanyMediaHoldingID', 'tvChannelId', 'tvChannelName',
                       'tvChannelEName', 'tvChannelDescription', 'tvNetId', 'tvNetName', 'tvNetEName',
                       'tvNetDescription', 'regionId', 'regionName', 'regionEName', 'scaleRange', 'reachInterval',
                       'geo', 'subGeo', 'sex', 'age', 'education', 'work', 'persNum', 'spendingsOnFood', 'tvNum',
                       'ageGroup', 'incomeGroupRussia', 'housewife', 'incomeEarner', 'video', 'incLevel', 'kidsNum',
                       'kidsAge1', 'kidsAge2', 'kidsAge3', 'kidsAge4', 'kidsAge5', 'kidsAge6', 'kidsAge7', 'status',
                       'business', 'enterprise', 'property', 'maritalStatus', 'lifeCycle', 'computer', 'internet',
                       'dacha', 'providerSputnik', 'providerAnalogEth', 'providerDigitalEth', 'providerAnalogCbl',
                       'providerDigitalCbl', 'hasVm', 'kidsAgeC1', 'kidsAgeC2', 'kidsAgeC3', 'kidsAgeC4', 'occupation',
                       'tradeIndustry', 'incomeGroup', 'federalOkrug', 'indexOkrug', 'incomeScale201401', 'equipmentV2',
                       'availTvPort1', 'availTvPort2', 'availTvPort3', 'availTvPort4', 'availTvPort5', 'availTvPort6',
                       'workCompIntern', 'housCompIntern', 'availAmsPort1', 'availAmsPort2', 'availAmsPort3',
                       'availAmsPort4', 'availAmsPort5', 'availAmsPort6', 'availAmsPort7', 'zdcHholdPersCnt',
                       'zdcIncomeGroup', 'zdcSex', 'zdcRAge18', 'zdcEducation', 'zdcWork', 'zodiacNet1View',
                       'zodiacNet2View', 'zodiacNet4View', 'zodiacNet11View', 'zodiacNet83View', 'zodiacNet13View',
                       'zodiacNet60View', 'zodiacNet12View', 'zodiacNet326View', 'zodiacNet206View', 'zodiacNet257View',
                       'zodiacNet205View', 'zodiacNet204View', 'zodiacNet255View', 'zodiacNet258View',
                       'zodiacNet259View', 'zodiacWorkPtview', 'zodiacWorkNptview', 'zodiacHolidayView',
                       'zodiacNet84View', 'zodiacNet260View', 'zodiacNet286View', 'zodiacNet40View', 'hhSignalKind1',
                       'hiddenFilterage1', 'hiddenFilterage18641', 'zodiacNet10View', 'zodiacNet270View',
                       'moscowRegion', 'wghPersNum1', 'existsSmartTv', 'isPrimaryPanel', 'isSecondaryPanel',
                       'secondPanelSeason', 'secondaryPanelMatrix', 'wghSuburbAgeGroup', 'zdcSocstat', 'wghLang',
                       'wghDachaTvExist', 'wghDachaSex', 'wghDachaAgeGroup16', 'smartTvYesNo', 'wghVideoGameWoAge',
                       'wghUsbTvHh', 'cubeCity', 'timezone0', 'hiddenFilterage18Zodiac', 'cube', 'cube100Plus100Minus',
                       'sputnikTv', 'geoFrom082020', 'hasRouterMeter', 'usageSmartTv', 'hiddenFilterage18640',
                       'rage1864', 'populationZdk', 'providerIptv', 'providerOtt', 'city', 'programStartTime',
                       'programFinishTime', 'programRoundedStartTime', 'programRoundedFinishTime', 'programDuration',
                       'programRoundedDuration', 'programIssueDescriptionName', 'programIssueDescriptionEName',
                       'programSpotId', 'programId', 'programName', 'programEName', 'programTypeId', 'programTypeName',
                       'programTypeEName', 'programCountryId', 'programCountryName', 'programCountryEName',
                       'programCategoryId', 'programCategoryName', 'programCategoryEName', 'programSportId',
                       'programSportName', 'programSportEName', 'programSportGroupId', 'programSportGroupName',
                       'programSportGroupEName', 'programProducerId', 'programProducerName', 'programProducerEName',
                       'programNotes', 'programLive', 'programNational', 'programFirstIssueDate', 'programLanguageId',
                       'breaksStartTime', 'breaksFinishTime', 'breaksRoundedStartTime', 'breaksRoundedFinishTime',
                       'breaksDuration', 'breaksRoundedDuration', 'breaksSpotId', 'breaksId', 'breaksName',
                       'breaksEName', 'breaksStyleId', 'breaksDistributionType', 'breaksPositionType', 
                       'breaksContentType', 'breaksPosition', 'GRPTypeId', 'GRPTypeName', 'GRPTypeEName', 'price',
                       'GRPPrice', 'GRPPriceRub', 'GRPCost', 'GRPCostRub', 'adStartTime', 'adFinishTime',
                       'adRoundedStartTime', 'adRoundedFinishTime', 'adDuration', 'adRoundedDuration', 'adSpotId',
                       'adId', 'adName', 'adEName', 'adTypeId', 'advertiserListId', 'advertiserListName',
                       'advertiserListEName', 'brandListId', 'brandListName', 'brandListEName', 'modelListId',
                       'modelListName', 'modelListEName', 'articleList2Id', 'articleList2Name', 'articleList2EName', 
                       'articleList3Id', 'articleList3Name', 'articleList3EName', 'articleList4Id', 'articleList4Name',
                       'articleList4EName', 'adFirstIssueDate', 'subbrandListId', 'subbrandListName',
                       'subbrandListEName', 'advertiserId', 'advertiserName', 'advertiserEName', 'brandId', 'brandName',
                       'brandEName', 'subbrandId', 'subbrandName', 'subbrandEName', 'modelId', 'modelName',
                       'modelEName', 'article2Id', 'article2Name', 'article2EName', 'article3Name', 'article3EName',
                       'article4Name', 'article4EName'],
            'filters': ['dataVersionId', 'researchDate', 'researchWeekDay', 'researchDayType', 'locationId',
                        'timeBand1', 'timeBand5', 'timeBand10', 'timeBand15', 'timeBand30', 'timeBand60',
                        'tvCompanyId', 'tvCompanyGroupId', 'tvCompanyCategoryId', 'tvChannelId', 'tvNetId', 'regionId',
                        'subjectGeoType', 'geo', 'subGeo', 'sex', 'age', 'education', 'work', 'persNum',
                        'spendingsOnFood', 'tvNum', 'ageGroup', 'incomeGroupRussia', 'housewife', 'incomeEarner',
                        'video', 'incLevel', 'kidsNum', 'kidsAge1', 'kidsAge2', 'kidsAge3', 'kidsAge4', 'kidsAge5',
                        'kidsAge6', 'kidsAge7', 'status', 'business', 'enterprise', 'property', 'maritalStatus',
                        'lifeCycle', 'computer', 'internet', 'dacha', 'providerSputnik', 'providerAnalogEth',
                        'providerDigitalEth', 'providerAnalogCbl', 'providerDigitalCbl', 'hasVm', 'kidsAgeC1',
                        'kidsAgeC2', 'kidsAgeC3', 'kidsAgeC4', 'occupation', 'tradeIndustry', 'incomeGroup',
                        'federalOkrug', 'indexOkrug', 'incomeScale201401', 'equipmentV2', 'availTvPort1',
                        'availTvPort2', 'availTvPort3', 'availTvPort4', 'availTvPort5', 'availTvPort6',
                        'workCompIntern', 'housCompIntern', 'availAmsPort1', 'availAmsPort2', 'availAmsPort3',
                        'availAmsPort4', 'availAmsPort5', 'availAmsPort6', 'availAmsPort7', 'zdcHholdPersCnt',
                        'zdcIncomeGroup', 'zdcSex', 'zdcRAge18', 'zdcEducation', 'zdcWork', 'zodiacNet1View',
                        'zodiacNet2View', 'zodiacNet4View', 'zodiacNet11View', 'zodiacNet83View', 'zodiacNet13View',
                        'zodiacNet60View', 'zodiacNet12View', 'zodiacNet326View', 'zodiacNet206View',
                        'zodiacNet257View', 'zodiacNet205View', 'zodiacNet204View', 'zodiacNet255View',
                        'zodiacNet258View', 'zodiacNet259View', 'zodiacWorkPtview', 'zodiacWorkNptview',
                        'zodiacHolidayView', 'zodiacNet84View', 'zodiacNet260View', 'zodiacNet286View',
                        'zodiacNet40View', 'hhSignalKind1', 'hiddenFilterage1', 'hiddenFilterage18641',
                        'zodiacNet10View', 'zodiacNet270View', 'moscowRegion', 'wghPersNum1', 'existsSmartTv',
                        'isPrimaryPanel', 'isSecondaryPanel', 'secondPanelSeason', 'secondaryPanelMatrix',
                        'wghSuburbAgeGroup', 'zdcSocstat', 'wghLang', 'wghDachaTvExist', 'wghDachaSex',
                        'wghDachaAgeGroup16', 'smartTvYesNo', 'wghVideoGameWoAge', 'wghUsbTvHh', 'cubeCity',
                        'timezone0', 'hiddenFilterage18Zodiac', 'cube', 'cube100Plus100Minus', 'sputnikTv',
                        'geoFrom082020', 'hasRouterMeter', 'usageSmartTv', 'hiddenFilterage18640', 'rage1864',
                        'populationZdk', 'providerIptv', 'providerOtt', 'city', 'programStartTime',
                        'programFinishTime', 'programRoundedStartTime', 'programRoundedFinishTime', 'programDuration',
                        'programRoundedDuration', 'programSpotId', 'programId', 'programName', 'programEName',
                        'programTypeId', 'programTypeName', 'programTypeEName', 'programCountryId',
                        'programCountryName', 'programCountryEName', 'programCategoryId', 'programCategoryName',
                        'programCategoryEName', 'programSportId', 'programSportName', 'programSportEName',
                        'programSportGroupId', 'programSportGroupName', 'programSportGroupEName', 'programProducerId',
                        'programProducerName', 'programProducerEName', 'programNotes', 'programLive', 'programNational',
                        'programFirstIssueDate', 'programLanguageId', 'breaksStartTime', 'breaksFinishTime',
                        'breaksRoundedStartTime', 'breaksRoundedFinishTime', 'breaksDuration', 'breaksRoundedDuration',
                        'breaksSpotId', 'breaksId', 'breaksName', 'breaksEName', 'breaksStyleId',
                        'breaksDistributionType', 'breaksPositionType', 'breaksContentType', 'breaksPosition',
                        'GRPTypeId', 'GRPTypeName', 'GRPTypeEName', 'price', 'GRPPrice', 'GRPPriceRub', 'GRPCost',
                        'GRPCostRub', 'adStartTime', 'adFinishTime', 'adRoundedStartTime', 'adRoundedFinishTime',
                        'adDuration', 'adRoundedDuration', 'adSpotId', 'adId', 'adName', 'adEName', 'adTypeId',
                        'advertiserListId', 'advertiserListName', 'advertiserListEName', 'brandListId', 'brandListName',
                        'brandListEName', 'modelListId', 'modelListName', 'modelListEName', 'articleList2Id',
                        'articleList2Name', 'articleList2EName', 'articleList3Id', 'articleList3Name',
                        'articleList3EName', 'articleList4Id', 'articleList4Name', 'articleList4EName',
                        'adFirstIssueDate', 'subbrandListId', 'subbrandListName', 'subbrandListEName', 'advertiserId',
                        'advertiserName', 'advertiserEName', 'brandId', 'brandName', 'brandEName', 'subbrandId',
                        'subbrandName', 'subbrandEName', 'modelId', 'modelName', 'modelEName', 'article2Id',
                        'article2Name', 'article2EName', 'article3Name', 'article3EName', 'article4Name',
                        'article4EName', 'articleId', 'article3Id', 'article4Id']
        }

        return result

    def get_tv_tv_ad(self, tv_ad_type_ids=None, advertiser_list_ids=None, brand_list_ids=None, model_list_ids=None,
                     article_list2_ids=None, article_list3_ids=None, article_list4_ids=None, subbrand_list_ids=None,
                     slogan_audio_ids=None, slogan_video_ids=None, ad_style_ids=None, tv_ad_ids=None, name=None,
                     ename=None, notes=None, standard_durations=None, file_type=None, order_by=None, order_dir=None,
                     offset=None, limit=None, use_cache=True):
        """
        Получить телерекламу

        Parameters
        ----------
        tv_ad_type_ids : list
            Поиск по списку идентификаторов типов рекламы
        
        advertiser_list_ids : list
            Поиск по списку идентификаторов рекламодателей
        
        brand_list_ids : list
            Поиск по списку идентификаторов брендов
        
        model_list_ids : list
            Поиск по списку идентификаторов моделей
        
        article_list2_ids : list
            Поиск по списку идентификаторов мест
        
        article_list3_ids : list
            Поиск по списку идентификаторов мест
        
        article_list4_ids : list
            Поиск по списку идентификаторов мест
        
        subbrand_list_ids : list
            Поиск по списку идентификаторов суббрендов
        
        slogan_audio_ids : list
            Поиск по списку идентификаторов аудио слоганов
        
        slogan_video_ids : list
            Поиск по списку идентификаторов видео слоганов
        
        ad_style_ids : list
            Поиск по списку идентификаторов стилей рекламы
        
        tv_ad_ids : list
            Поиск по списку идентификаторов рекламы
            
        name : string
            Поиск по имени рекламы
        
        ename : string
            Поиск по англоязычному имени рекламы
        
        standard_durations : list
            Поиск по списку продолжительности рекламы
            
        notes : string
            Поиск по заметкам 
        
        file_type : string
            Поиск по типу файла
            
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
            "tvAdSloganAudioId": slogan_audio_ids,
            "tvAdSloganVideoId": slogan_video_ids,
            "adStyleId": ad_style_ids,
            "id": tv_ad_ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "standardDuration": standard_durations,
            "fileType": file_type
        }

        return self._get_dict('tv-tv-ad', search_params, body_params, offset, limit, use_cache)

    def get_tv_subbrand(self, brand_ids=None, ids=None, name=None, ename=None, notes=None,
                        tv_area_ids=None, order_by=None, order_dir=None, offset=None, limit=None,
                        use_cache=True):
        """
        Получить суббренды

        Parameters
        ----------
        brand_ids : list
            Поиск по списку идентификаторов брендов
        
        ids : list
            Поиск по списку идентификаторов суббрендов
        
        name : string
            Поиск по имени суббренда
        
        ename : string
            Поиск по англоязычному имени суббренда
            
        notes : string
            Поиск по заметкам
        
        tv_area_ids : list
            Поиск по списку телеплощадок
        
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

        return self._get_dict('tv-subbrand', search_params, body_params, offset, limit, use_cache)

    def get_tv_subbrand_list(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                             offset=None, limit=None, use_cache=True):
        """
        Получить списки суббрендов

        Parameters
        ----------
        ids : list
            Поиск по списку идентификаторов суббрендов
        
        name : string
            Поиск по имени суббренда
        
        ename : string
            Поиск по англоязычному имени суббренда
            
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

        return self._get_dict('tv-subbrand-list', search_params, body_params, offset, limit, use_cache)

    def get_tv_research_day_type(self, research_day_type=None, name=None, order_by=None, order_dir=None, offset=None,
                                 limit=None, use_cache=True):
        """
        Получить типы дней

        Parameters
        ----------
        research_day_type : string
            Поиск по идентификатору типа дня
        
        name : string
            Поиск по названию типа дня. Можно использовать часть названия
        
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

            DataFrame с типами дней
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "researchDayType": research_day_type,
            "name": name
        }

        return self._get_dict('tv-research-day-type', search_params, body_params, offset, limit, use_cache)

    def get_tv_region(self, region_ids=None, name=None, ename=None, notes=None, monitoring_type=None, order_by=None,
                      order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить регионы

        Parameters
        ----------
        region_ids : list
            Поиск по списку идентификаторов регионов
        
        name : string
            Поиск по имени региона
        
        ename : string
            Поиск по англоязычному имени региона
            
        notes : string
            Поиск по заметкам
        
        monitoring_type : string
            Поиск по типу мониторинга
                
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

            DataFrame с регионами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": region_ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "monitoringType": monitoring_type
        }

        return self._get_dict('tv-region', search_params, body_params, offset, limit, use_cache)

    def get_tv_program(self, program_ids=None, program_type_ids=None, program_category_ids=None, program_sport_ids=None,
                       sport_group_ids=None, language_ids=None, program_producer_ids=None, program_producer_reg=None,
                       program_producer_year=None, is_program_group=None, is_child=None, country_ids=None, name=None,
                       ename=None, extended_name=None, extended_ename=None, notes=None, order_by=None,
                       order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить программы

        Parameters
        ----------
        program_ids : list
            Поиск по списку идентификаторов программ
        
        program_type_ids : list
            Поиск по списку идентификаторов типов программ
        
        program_category_ids : list
            Поиск по списку идентификаторов категорий программ
        
        program_sport_ids : list
            Поиск по списку идентификаторов спортивных программ
        
        sport_group_ids : list
            Поиск по списку идентификаторов спортивных групп

        language_ids : list
            Поиск по списку идентификаторов языков
        
        program_producer_ids : list
            Поиск по списку идентификаторов продюсеров програм
        
        program_producer_reg : string
            Поиск по 
        
        program_producer_year : string
            Поиск по году создания

        is_program_group : string
            Поиск по признаку группы программ
        
        is_child : string
            Поиск по признаку подчиненности
        
        country_ids : list
            Поиск по списку идентификаторов стран
        
        name : string
            Поиск по имени программы
        
        ename : string
            Поиск по англоязычному имени программы
        
        extended_name : string
            Поиск по расширенному имени программы
        
        extended_ename : string
            Поиск по расширенному англоязычному имени программы
            
        notes : string
            Поиск по заметкам            
        
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
            "programProducerReg": program_producer_reg,
            "programProducerYear": program_producer_year,
            "isProgramGroup": is_program_group,
            "isChild": is_child,
            "id": program_ids,
            "countryId": country_ids,
            "name": name,
            "ename": ename,
            "extendedName": extended_name,
            "extendedEname": extended_ename,
            "notes": notes
        }

        return self._get_dict('tv-program', search_params, body_params, offset, limit, use_cache)

    def get_tv_program_type(self, program_type_ids=None, name=None, ename=None, notes=None,
                            order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить типы программ

        Parameters
        ----------
        program_type_ids : list
            Поиск по списку идентификаторов типов программ
        
        name : string
            Поиск по имени типа
        
        ename : string
            Поиск по англоязычному имени типа
            
        notes : string
            Поиск по заметкам 
                       
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

            DataFrame с типами программ
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": program_type_ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict('tv-program-type', search_params, body_params, offset, limit, use_cache)

    def get_tv_program_sport(self, program_sport_ids=None, name=None, ename=None, notes=None, order_by=None,
                             order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить виды спорта в программах

        Parameters
        ----------
        program_sport_ids : list
            Поиск по списку идентификаторов видов спорта в программах
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 
            
        notes : string
            Поиск по заметкам 
                        
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

            DataFrame с видами спорта в программах
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": program_sport_ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict('tv-program-sport', search_params, body_params, offset, limit, use_cache)

    def get_tv_program_sport_group(self, program_sport_group_ids=None, name=None, ename=None, notes=None, order_by=None,
                                   order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить группы видов спорта в программах

        Parameters
        ----------
        program_sport_group_ids : list
            Поиск по списку идентификаторов групп видов спорта
        
        name : string
            Поиск по имени группы
        
        ename : string
            Поиск по англоязычному имени группы
            
        notes : string
            Поиск по заметкам 
                                    
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

            DataFrame с группами видов спорта
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": program_sport_group_ids,
            "name": name,
            "ename": ename,
            "notes": notes
        }

        return self._get_dict('tv-program-sport-group', search_params, body_params, offset, limit, use_cache)

    def get_tv_program_issue_description(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                                         offset=None, limit=None, use_cache=True):
        """
        Получить описания выпусков

        Parameters
        ----------
        ids : list
            Поиск по списку идентификаторов описаний выпусков
        
        name : string
            Поиск по имени описания
        
        ename : string
            Поиск по англоязычному имени описания
            
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

            DataFrame с описаниями выпусков
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

        return self._get_dict('tv-program-issue-description', search_params, body_params, offset, limit, use_cache)

    def get_tv_program_category(self, program_type_ids=None, program_type_category_nums=None, ids=None, name=None,
                                ename=None, short_name=None, short_ename=None, notes=None, order_by=None,
                                order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить категорию телепрограмм

        Parameters
        ----------
        program_type_ids : list
            Поиск по списку идентификаторов типов программ
        
        program_type_category_nums : list
            Поиск по списку номеров категорий
            
        ids : list
            Поиск по списку идентификаторов категорий
        
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени
            
        short_name : string
            Поиск по имени
        
        short_ename : string
            Поиск по англоязычному имени
            
        notes : string
            Поиск по заметкам 
            
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

            DataFrame с категориями телепрограмм
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "programTypeId": program_type_ids,
            "programTypeCategoryNum": program_type_category_nums,
            "id": ids,
            "name": name,
            "ename": ename,
            "shortName": short_name,
            "shortEname": short_ename,
            "notes": notes
        }

        return self._get_dict('tv-program-category', search_params, body_params, offset, limit, use_cache)

    def get_tv_prime_time(self, ids=None, name=None, ename=None, notes=None, order_by=None, order_dir=None,
                          offset=None, limit=None, use_cache=True):
        """
        Получить прайм-тайм

        Parameters
        ----------
        ids : list
            Поиск по списку идентификаторов прайм-тайма
        
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени
            
        notes : string
            Поиск по заметкам 
            
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

            DataFrame с категориями телепрограмм
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

        return self._get_dict('tv-prime-time', search_params, body_params, offset, limit, use_cache)

    def get_tv_net(self, net_ids=None, name=None, ename=None, order_by=None,
                   order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить сети

        Parameters
        ----------
        net_ids : list
            Поиск по списку идентификаторов сетей
        
        name : string
            Поиск по имени сети
        
        ename : string
            Поиск по англоязычному имени сети 
            
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

            DataFrame с сетями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": net_ids,
            "name": name,
            "ename": ename
        }

        df_net = self._get_dict('tv-net', search_params, body_params, offset, limit, use_cache)
        try:
            df_net.drop('notes', axis=1, inplace=True)
        except Exception:
            return df_net
        
        return df_net

    def get_tv_model(self, subbrand_ids=None, article_ids=None, ids=None, name=None, ename=None, notes=None,
                     tv_area_ids=None, order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить модели

        Parameters
        ----------
        subbrand_ids : list
            Поиск по списку идентификаторов суббрендов
            
        article_ids : list
            Поиск по списку идентификаторов статей
        
        ids : list
            Поиск по списку идентификаторов моделей
        
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени
            
        notes : string
            Поиск по заметкам
        
        tv_area_ids : list
            Поиск по списку идентификаторов телеплощадок 
            
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

            DataFrame с моделями
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

        return self._get_dict('tv-model', search_params, body_params, offset, limit, use_cache)

    def get_tv_model_list(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                          offset=None, limit=None, use_cache=True):
        """
        Получить списки моделей

        Parameters
        ----------       
        ids : list
            Поиск по списку идентификаторов моделей
        
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени   
            
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

            DataFrame с моделями
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

        return self._get_dict('tv-model-list', search_params, body_params, offset, limit, use_cache)

    def get_tv_location(self, location_ids=None, name=None, ename=None, order_by=None,
                        order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить места

        Parameters
        ----------
        location_ids : list
            Поиск по списку идентификаторов мест

        name : string
            Поиск по имени места
        
        ename : string
            Поиск по англоязычному имени места
            
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

            DataFrame с местами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": location_ids,
            "name": name,
            "ename": ename
        }

        return self._get_dict('tv-location', search_params, body_params, offset, limit, use_cache)

    def get_tv_grp_type(self, ids=None, name=None, notes=None, expression=None, order_by=None,
                        order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить типы групп

        Parameters
        ----------       
        ids : list
            Поиск по списку идентификаторов типов групп
        
        name : string
            Поиск по имени
        
        notes : string
            Поиск по заметке   
        
        expression : string
            Поиск по выражению 
            
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

            DataFrame с типами групп
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

        return self._get_dict('tv-grp-type', search_params, body_params, offset, limit, use_cache)

    def get_tv_exchange_rate(self, research_date=None, rate=None, order_by=None,
                             order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить курсы обмена

        Parameters
        ----------       
        research_date : list
            Поиск по списку дней
        
        rate : list
            Поиск по списку курсов
            
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

        return self._get_dict('tv-exchange-rate', search_params, body_params, offset, limit, use_cache)

    def get_tv_digital_broadcasting_type(self, ids=None, name=None, ename=None, notes=None, order_by=None,
                                         order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить типы цифрового вещания

        Parameters
        ----------
        ids : list
            Поиск по списку идентификаторов типов вещания
        
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени
            
        notes : string
            Поиск по заметкам 
            
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

            DataFrame с типами цифрового вещания
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

        return self._get_dict('tv-digital-broadcasting-type', search_params, body_params, offset, limit, use_cache)

    def get_tv_day_week(self, day_num_ids=None, day_week_name=None, day_week_ename=None, order_by=None,
                        order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить дни недели

        Parameters
        ----------
        day_num_ids : list
            Поиск по списку идентификаторов дней недели
            
        day_week_name : string
            Поиск по имени дня
        
        day_week_ename : string
            Поиск по англоязычному имени дня  
                      
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

            DataFrame с днями недели
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "dayNum": day_num_ids,
            "dayWeekName": day_week_name,
            "dayWeekEName": day_week_ename
        }

        return self._get_dict('tv-day-week', search_params, body_params, offset, limit, use_cache)

    def get_tv_company(self, tv_channel_ids=None, tv_net_ids=None, region_ids=None, tv_company_group_ids=None,
                       tv_company_category_ids=None, name=None, ename=None, ids=None, status=None,
                       information=None, monitoring_type=None, order_by=None, order_dir=None, offset=None, 
                       limit=None, use_cache=True):
        """
        Получить телекомпании

        Parameters
        ----------        
        tv_channel_ids : list
            Поиск по списку идентификаторов каналов
            
        tv_net_ids : list
            Поиск по списку идентификаторов сетей
            
        region_ids : list
            Поиск по списку идентификаторов регионов
            
        tv_company_group_ids : list
            Поиск по списку идентификаторов групп компаний
            
        tv_company_category_ids : list
            Поиск по списку идентификаторов категорий компаний
            
        name : string
            Поиск по имени
        
        ename : string
            Поиск по англоязычному имени
            
        ids : list
            Поиск по списку идентификаторов компаний    
        
        status : string
            Поиск по статусу
        
        information : string
            Поиск по информации
        
        monitoring_type : string
            Поиск по типу мониторинга
                               
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

            DataFrame с телекомпаниями
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "tvChannelId": tv_channel_ids,
            "tvNetId": tv_net_ids,
            "regionId": region_ids,
            "tvCompanyGroupId": tv_company_group_ids,
            "tvCompanyCategoryId": tv_company_category_ids,
            "name": name,
            "ename": ename,
            "id": ids,
            "status": status,
            "information": information,
            "monitoringType": monitoring_type
        }

        df_comp = self._get_dict('tv-company', search_params, body_params, offset, limit, use_cache)
        try:
            df_comp.drop('notes', axis=1, inplace=True)
        except Exception:
            return df_comp
        
        return df_comp

    def get_tv_company_merge(self, tv_channel_merge_ids=None, tv_company_ids=None, ids=None,
                             order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить объединенные компании

        Parameters
        ----------
        tv_channel_merge_ids : list
            Поиск по списку идентификаторов объединенных компаний
        
        tv_company_ids : list
            Поиск по списку идентификаторов компаний
        
        ids : list
            Поиск по списку идентификаторов
                               
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

        return self._get_dict('tv-company-merge', search_params, body_params, offset, limit, use_cache)

    def get_tv_calendar(self, research_date=None, research_day_type=None, order_by=None, order_dir=None,
                        offset=None, limit=None, use_cache=True):
        """
        Получить календарь

        Parameters
        ----------
        research_date : list
            Поиск по списку дат
        
        research_day_type : list
            Поиск по списку типов дат 
                        
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

        return self._get_dict('tv-calendar', search_params, body_params, offset, limit, use_cache)

    def get_tv_break(self, order_by=None, order_dir=None,
                     offset=None, limit=None, use_cache=True):
        """
        Получить перерывы

        Parameters
        ----------                        
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

            DataFrame с перерывами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {

        }

        return self._get_dict('tv-break', search_params, body_params, offset, limit, use_cache)

    def get_tv_brand(self, ids=None, name=None, ename=None, notes=None, tv_area_ids=None,
                     order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить бренды

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов брендов
        
        name : string
            Поиск по имени бренда
        
        ename : string
            Поиск по англоязычному имени бренда
            
        notes : string
            Поиск по заметкам
        
        tv_area_ids : list
            Поиск по списку телеплощадок
        
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

        return self._get_dict('tv-brand', search_params, body_params, offset, limit, use_cache)

    def get_tv_brand_list(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                          offset=None, limit=None, use_cache=True):
        """
        Получить списки брендов

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов 
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 

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

        return self._get_dict('tv-brand-list', search_params, body_params, offset, limit, use_cache)

    def get_tv_article(self, parent_ids=None, levels=None, ids=None, name=None, ename=None, notes=None,
                       order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить статьи

        Parameters
        ----------        
        parent_ids : list
            Поиск по списку родительских идентификаторов
        
        levels : list
            Поиск по списку уровней  

        ids : list
            Поиск по списку идентификаторов 
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 
        
        notes : string
            Поиск по заметкам

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

            DataFrame со статьями
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

        return self._get_dict('tv-article', search_params, body_params, offset, limit, use_cache)

    def get_tv_article_list4(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                             offset=None, limit=None, use_cache=True):
        """
        Получить список статей 4

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов 
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 

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

            DataFrame со списками статей 4
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

        return self._get_dict('tv-article-list4', search_params, body_params, offset, limit, use_cache)

    def get_tv_article_list3(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                             offset=None, limit=None, use_cache=True):
        """
        Получить список статей 3

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов 
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 

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

            DataFrame со списками статей 3
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

        return self._get_dict('tv-article-list3', search_params, body_params, offset, limit, use_cache)

    def get_tv_article_list2(self, ids=None, name=None, ename=None, order_by=None, order_dir=None,
                             offset=None, limit=None, use_cache=True):
        """
        Получить список статей 2

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов 
        
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 

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

            DataFrame со списками статей 2
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

        return self._get_dict('tv-article-list2', search_params, body_params, offset, limit, use_cache)

    def get_tv_appendix(self, tv_ad_ids=None, model_ids=None, advertiser_ids=None, article2_ids=None,
                        article3_ids=None, article4_ids=None, subbrand_ids=None, brand_ids=None,
                        order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить аппендикс

        Parameters
        ----------        
        tv_ad_ids : list
            Поиск по списку идентификаторов рекламы
        
        model_ids : list
            Поиск по списку идентификаторов моделей

        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей
            
        article2_ids : list
            Поиск по списку идентификаторов 
            
        article3_ids : list
            Поиск по списку идентификаторов 
            
        article4_ids : list
            Поиск по списку идентификаторов 
            
        subbrand_ids : list
            Поиск по списку идентификаторов суббрендов
            
        brand_ids : list
            Поиск по списку идентификаторов брендов        
        
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

            DataFrame с аппендиксом
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "tvAdId": tv_ad_ids,
            "modelId": model_ids,
            "advertiserId": advertiser_ids,
            "article2Id": article2_ids,
            "article3Id": article3_ids,
            "article4Id": article4_ids,
            "subbrandId": subbrand_ids,
            "brandId": brand_ids
        }

        return self._get_dict('tv-appendix', search_params, body_params, offset, limit, use_cache)

    def get_tv_advertiser(self, ids=None, name=None, ename=None, notes=None, tv_area_ids=None,
                          order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить рекламодателей

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
       
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 
        
        notes : string
            Поиск по заметкам
            
        tv_area_ids : list
            Поиск по списку идентификаторов телеплощадок
            
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

            DataFrame со статьями
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

        return self._get_dict('tv-advertiser', search_params, body_params, offset, limit, use_cache)

    def get_tv_advertiser_list(self, ids=None, name=None, ename=None,
                               order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить список рекламодателей

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
       
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени 
            
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

            DataFrame со статьями
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

        return self._get_dict('tv-advertiser-list', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad(self, tv_ad_type_ids=None, advertiser_list_ids=None, brand_list_ids=None, model_list_ids=None,
                  article_list2_ids=None, article_list3_ids=None, article_list4_ids=None, subbrand_list_ids=None,
                  ad_style_ids=None, advertiser_list_main_ids=None, brand_list_main_ids=None, model_list_main_ids=None,
                  article_list2_main_ids=None, article_list3_main_ids=None, article_list4_main_ids=None, 
                  subbrand_list_main_ids=None, age_restriction_ids=None, tv_ad_ids=None, name=None,
                  ename=None, notes=None, standard_durations=None, tv_area_ids=None, slogan_audio_ids=None, 
                  slogan_video_ids=None, order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить телерекламу

        Parameters
        ----------
        tv_ad_type_ids : list
            Поиск по списку идентификаторов типов рекламы
        
        advertiser_list_ids : list
            Поиск по списку идентификаторов рекламодателей
        
        brand_list_ids : list
            Поиск по списку идентификаторов брендов
        
        model_list_ids : list
            Поиск по списку идентификаторов моделей
        
        article_list2_ids : list
            Поиск по списку идентификаторов мест
        
        article_list3_ids : list
            Поиск по списку идентификаторов мест
        
        article_list4_ids : list
            Поиск по списку идентификаторов мест
        
        subbrand_list_ids : list
            Поиск по списку идентификаторов суббрендов
        
        ad_style_ids : list
            Поиск по списку идентификаторов стилей рекламы
        
        advertiser_list_main_ids : list
            Поиск по списку идентификаторов основного списка рекламодателей
        
        brand_list_main_ids : list
            Поиск по списку идентификаторов основного списка брендов
        
        model_list_main_ids : list
            Поиск по списку идентификаторов основного списка моделей
            
        article_list2_main_ids : list
            Поиск по списку идентификаторов основного списка статей 2
        
        article_list3_main_ids : list
            Поиск по списку идентификаторов основного списка статей 3
        
        article_list4_main_ids : list
            Поиск по списку идентификаторов основного списка статей 4
        
        subbrand_list_main_ids : list
            Поиск по списку идентификаторов основного списка суббрендов
            
        age_restriction_ids : list
            Поиск по списку идентификаторов возрастных ограничений        
        
        tv_ad_ids : list
            Поиск по списку идентификаторов рекламы
            
        name : string
            Поиск по имени рекламы
        
        ename : string
            Поиск по англоязычному имени рекламы
        
        standard_durations : list
            Поиск по списку продолжительности рекламы
            
        notes : string
            Поиск по заметкам 
        
        slogan_audio_ids : list
            Поиск по списку идентификаторов аудио слоганов
        
        slogan_video_ids : list
            Поиск по списку идентификаторов видео слоганов
            
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
            "id": tv_ad_ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "standardDuration": standard_durations,
            "tvArea": tv_area_ids,
            "tvAdSloganAudioId": slogan_audio_ids,
            "tvAdSloganVideoId": slogan_video_ids
        }

        return self._get_dict('tv-ad', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_type(self, tv_ad_ids=None, name=None, ename=None, notes=None, accounting_duration_type_ids=None,
                       is_override=None, position_type=None, is_price=None, order_by=None, order_dir=None, offset=None, 
                       limit=None, use_cache=True):
        """
        Получить типы телерекламы

        Parameters
        ----------        
        tv_ad_ids : list
            Поиск по списку идентификаторов рекламы
            
        name : string
            Поиск по имени рекламы
        
        ename : string
            Поиск по англоязычному имени рекламы        
            
        notes : string
            Поиск по заметкам 
        
        accounting_duration_type_ids : list
            Поиск по списку идентификаторов типов продолжительности
        
        is_override : string
            Поиск по признаку перезаписи
        
        position_type : string
            Поиск по типу позиции
        
        is_price : string
            Поиск по признаку цены
            
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

            DataFrame с типами рекламы
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        } 
  
        body_params = {
            "id": tv_ad_ids,
            "name": name,
            "ename": ename,
            "notes": notes,
            "accountingDurationType": accounting_duration_type_ids,
            "isOverride": is_override,
            "positionType": position_type,
            "isPrice": is_price
        }

        return self._get_dict('tv-ad-type', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_total(self, tv_ad_ids=None, research_month=None, tv_ad_type_ids=None, tv_company_ids=None,
                        ad_style_ids=None, advertiser_ids=None, brand_ids=None, subbrand_ids=None,
                        model_ids=None, region_ids=None, article1_ids=None, article2_ids=None, article3_ids=None,
                        article4_ids=None, file_type=None, order_by=None, order_dir=None, offset=None,
                        limit=None, use_cache=True):
        """
        Получить сводные данные по рекламе

        Parameters
        ----------
        tv_ad_ids : list
            Поиск по списку идентификаторов рекламы
        
        research_month : string
            Поиск по месяцу
            
        tv_ad_type_ids : list
            Поиск по списку идентификаторов типов рекламы
        
        tv_company_ids : list
            Поиск по списку идентификаторов компаний
        
        ad_style_ids : list
            Поиск по списку идентификаторов стилей рекламы
        
        advertiser_ids : list
            Поиск по списку идентификаторов рекламодателей
        
        brand_ids : list
            Поиск по списку идентификаторов брендов
        
        subbrand_ids : list
            Поиск по списку идентификаторов суббрендов
        
        model_ids : list
            Поиск по списку идентификаторов моделей
        
        region_ids : list
            Поиск по списку идентификаторов регионов
        
        article1_ids : list
            Поиск по списку идентификаторов статей 1
        
        article2_ids : list
            Поиск по списку идентификаторов статей 2
        
        article3_ids : list
            Поиск по списку идентификаторов статей 3
        
        article4_ids : list
            Поиск по списку идентификаторов статей 4
        
        file_type : string
            Поиск по типу файла
            
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

            DataFrame со сводными данными по рекламе
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "tvAdId": tv_ad_ids,
            "researchMonth": research_month,
            "tvAdTypeId": tv_ad_type_ids,
            "tvCompanyId": tv_company_ids,
            "adStyleId": ad_style_ids,
            "advertiserId": advertiser_ids,
            "brandId": brand_ids,
            "subbrandId": subbrand_ids,
            "modelId": model_ids,
            "regionId": region_ids,
            "article1Id": article1_ids,
            "article2Id": article2_ids,
            "article3Id": article3_ids,
            "article4Id": article4_ids,
            "fileType": file_type
        }

        return self._get_dict('tv-ad-total', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_style(self, ids=None, name=None, ename=None, notes=None, order_by=None, 
                        order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить рекламные стили

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени         
        
        notes : string
            Поиск по англоязычному имени
            
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

            DataFrame с рекламными стили
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

        return self._get_dict('tv-ad-style', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_slogan_video(self, ids=None, name=None, ename=None, order_by=None, order_dir=None, offset=None,
                               limit=None, use_cache=True):
        """
        Получить рекламные видео слоганы

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени         
            
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

            DataFrame с рекламными видео слоганами
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

        return self._get_dict('tv-ad-slogan-video', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_slogan_audio(self, ids=None, name=None, ename=None, order_by=None, order_dir=None, offset=None,
                               limit=None, use_cache=True):
        """
        Получить рекламные аудио слоганы

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени         
            
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

            DataFrame с рекламными аудио слоганами
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

        return self._get_dict('tv-ad-slogan-audio', search_params, body_params, offset, limit, use_cache)

    def get_tv_ad_month(self, tv_company_ids=None, research_month=None, ad_ids=None, from_tv_company_ids=None, 
                        from_research_month=None, volume=None, count=None, price=None, grp_price=None,
                        issue_status=None, cnd_cost=None, distribution=None, cost_rub=None, grp_cost_rub=None,
                        cnd_cost_rub=None, order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить месячную рекламу

        Parameters
        ----------        
        tv_company_ids : list
            Поиск по списку идентификаторов телекомпаний
            
        research_month : string
            Поиск по месяцу 
        
        ad_ids : list
            Поиск по списку идентификаторов рекламы
        
        from_tv_company_ids : list
            Поиск по списку идентификаторов телекомпаний
            
        from_research_month : string
            Поиск по месяцу 
        
        volume : integer
            Поиск по объему
        
        count : integer
            Поиск по количеству
        
        price : integer
            Поиск по цене
        
        grp_price : integer
            Поиск по цене
        
        issue_status : string
            Поиск по статусу выхода
        
        cnd_cost : integer
            Поиск по затратам
        
        distribution : string
            Поиск по распространению 
        
        cost_rub : integer
            Поиск по затратам 
        
        grp_cost_rub : integer
            Поиск по затратам
        
        cnd_cost_rub : integer
            Поиск по затратам      
            
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

            DataFrame с месячной рекламой
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        } 
  
        body_params = {
            "tvCompanyId": tv_company_ids,
            "researchMonth": research_month,
            "adId": ad_ids,
            "fromTvCompanyId": from_tv_company_ids,
            "fromResearchMonth": from_research_month,
            "volume": volume,
            "count": count,
            "price": price,
            "grpPrice": grp_price,
            "issueStatus": issue_status,
            "cndCost": cnd_cost,
            "distribution": distribution,
            "costRub": cost_rub,
            "grpCostRub": grp_cost_rub,
            "cndCostRub": cnd_cost_rub
        }

        return self._get_dict('tv-ad-month', search_params, body_params, offset, limit, use_cache)

    def get_tv_time_band(self):
        """
        Получить справочник time-band
        
        Returns
        -------
        info : DataFrame
            DataFrame с time-band
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-time-band'], use_cache=False))

    def get_tv_stat(self):
        """
        Получить справочник stat
        
        Returns
        -------
        info : DataFrame
            DataFrame с stat
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-stat'], use_cache=False))

    def get_tv_relation(self):
        """
        Получить справочник relation
        
        Returns
        -------
        info : Dict
            Словарь с relation
        """                
        return self.msapi_network.send_request('get', self._urls['tv-relation'], use_cache=False)
    
    def get_tv_program_prreg(self):
        """
        Получить справочник видов производства программ
        
        Returns
        -------
        info : DataFrame
            DataFrame с видами производства программ
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-program-prreg'], use_cache=False))

    def get_tv_monitoring_type(self):
        """
        Получить справочник типов мониторинга
        
        Returns
        -------
        info : DataFrame
            DataFrame с типами мониторинга
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-monitoring-type'], use_cache=False))

    def get_tv_db_rd_type(self):
        """
        Получить справочник db_rd_type
        
        Returns
        -------
        info : DataFrame
            DataFrame с db_rd_type
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-db-rd-type'], use_cache=False))

    def get_tv_ad_iss_sbtv(self):
        """
        Получить справочник ad_iss_sbtv
        
        Returns
        -------
        info : DataFrame
            DataFrame с ad_iss_sbtv
        """
        return pd.DataFrame(self.msapi_network.send_request('get', self._urls['tv-ad-iss-sbtv'], use_cache=False))

    def get_tv_demo_attribute(self, ids=None, value_ids=None, names=None, col_names=None, value_names=None,
                              order_by=None, order_dir=None, offset=None, limit=None, use_cache=True):
        """
        Получить атрибуты

        Parameters
        ----------
        ids : list
            Поиск по списку идентификаторов
        
        value_ids : list
            Поиск по списку идентификаторов значений
        
        names : list
            Поиск по списку имен
        
        col_names : list
            Поиск по списку имен колонок
        
        value_names : list
            Поиск по списку имен значений
            
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

            DataFrame с атрибутами
        """

        search_params = {
            'orderBy': order_by,
            'orderDir': order_dir
        }

        body_params = {
            "id": ids,
            "valueId": value_ids,
            "name": names,
            "colName": col_names,
            "valueName": value_names,
            "demoAttributeColName": col_names,
            "demoAttributeValueId": value_ids
        }

        return self._get_dict('tv-demo-attribute', search_params, body_params, offset, limit, use_cache)
    
    def get_tv_program_country(self, ids=None, name=None, ename=None, notes=None, 
                               order_by=None, order_dir=None, offset=None,
                               limit=None, use_cache=True):
        """
        Получить страны производства программ

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени
        
        notes : string
            Поиск по заметкам         
            
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

        return self._get_dict('tv-program-country', search_params, body_params, offset, limit, use_cache)

    def get_tv_company_holding(self, ids=None, name=None, ename=None, 
                               order_by=None, order_dir=None, offset=None,
                               limit=None, use_cache=True):
        """
        Получить список холдингов телекомпаний

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени     
            
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

        return self._get_dict('tv-company-holding', search_params, body_params, offset, limit, use_cache)

    def get_tv_company_media_holding(self, ids=None, name=None,
                                     ename=None, order_by=None, order_dir=None, offset=None,
                                     limit=None, use_cache=True):
        """
        Получить список медиа холдингов телекомпаний

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени     
            
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

        return self._get_dict('tv-company-media-holding', search_params, body_params, offset, limit, use_cache)
    
    def get_tv_company_thematic(self, ids=None, name=None,
                                     ename=None, order_by=None, order_dir=None, offset=None,
                                     limit=None, use_cache=True):
        """
        Получить список жанров телекомпаний

        Parameters
        ----------        
        ids : list
            Поиск по списку идентификаторов
            
        name : string
            Поиск по имени 
        
        ename : string
            Поиск по англоязычному имени     
            
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

            DataFrame с жанрами телекомпаний
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

        return self._get_dict('tv-company-thematic', search_params, body_params, offset, limit, use_cache)
