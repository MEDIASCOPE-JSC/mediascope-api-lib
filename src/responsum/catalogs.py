import os
import json
import pandas as pd
from ..core import net

class ResponsumCats:
    
    # TODO: Добавить документацию для публичных методов
    
    facility_id = None
    
    def __new__(cls, facility_id, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(ResponsumCats, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork()
        if facility_id != self.facility_id or not hasattr(self, 'demattr') or not hasattr(self, 'holdings'):
            self.facility_id = facility_id
            self.demattr = self.get_demo()
            self.holdings = self.get_holdings(facility_id)
    
    
    def get_demo(self, id=None, find_text=None, expand=False, frmt='df'):
        """
        Получить список демографических переменных: все, по id или поиском по названию
        
        Parameters
        ----------

        id : int
            Идентификатор демографической переменной для того, что бы получить одну переменную. По умолчанию - не задано (None)
        
        find_text : str
            Текст для поиска по названию переменной. По умолчанию - не задано (None)
        
        expand : bool
            Отобразить в таблице (DataFrame) доступные варианты для данной переменной - True/False? По умолчанию - False
        
        frmt: str
            Формат вывода разультата
            - "df" - DataFrame,
            - "json" - JSON,
            По умолчанию - "df"
            
        
        Returns
        -------
        
        DataFrame с демографическими переменными
        """
        data = self.msapi_network.send_request('get', '/demo/variables', use_cache=True)
        res = []

        for item in data:
            is_found = False
            if id is not None and str(item['varId']) == str(id):
                is_found = True
            elif find_text is not None and (item['varName'].lower().find(find_text.lower()) >= 0 or item['title'].lower().find(find_text.lower()) >= 0):
                is_found = True
            elif id is None and find_text is None:
                is_found = True
            if is_found:
                obj = {'varId': item['varId'],
                    'varName': item['varName'],
                    'varTitle': item['title'],
                    'categories': [],
                    'from': None,
                    'to': None
                    }
                if 'from' in item:
                    obj['from'] = item['from']
                if 'to' in item:
                    obj['to'] = item['to']
                if 'categories' in item:
                    obj['categories'] = item['categories']
                # expand categories
                if expand and len(item['categories']):
                    for citem in item['categories']:
                        cobj = obj.copy()
                        cobj['catNum'] = citem['catNum']
                        cobj['catTitle'] = citem['title']
                        res.append(cobj)
                else:
                    if 'from' in item:
                        obj['from'] = item['from']
                    if 'to' in item:
                        obj['to'] = item['to']
                    res.append(obj)

        if frmt == 'json':
            return res
        else:
            df = pd.DataFrame(res)
            #df['catNum'] = df[['catNum']].dropna().astype('int')
            return df


    def get_demo_dict(self, ds):
        """
        Получить список демографических атрибутов в виде словаря. Используется при формировании задания. Данный метод вызывается при инициализации
        
        Parameters
        ----------

        ds: DataFrame со списком атрибутов
        
        Returns
        -------
        
        Словарь с демографическими переменными: {attribute_name: {k: {"%key%"}, v: {"%%val"}}}
        
        """
        d = {}
        for item in ds[['varName', 'varId']].to_dict('split')['data']:
            key = item[0]
            val = item[1]
            d[key.lower()] = {'k': key, 'v': val}
        return d


    def get_holdings(self, facility_id, id=None, find_text=None, frmt='df'):
        """
        Получить список холдингов - все, по id или поиском по названию. Если id или find_text не заданы, то возвращает все доступные холдинги
        
        Parameters
        ----------
        facility_id : Установка: "desktop", "mobile", "desktop-pre". Обязательный параметр
        
        id : Идентификатор холдинга, по умолчания не задан (None)
        
        find_text: Текст для поиска по названию холдинга. По умолчанию - не задано (None)
        
        frmt: Формат вывода разультата
            - "df" - DataFrame,
            - "json" - JSON,
            По умолчанию - "df"
            
        
        Returns
        -------
        
        DataFrame с холдингами
        
        """
    
        data = self.msapi_network.send_request('get', '/media/holdings?facility_id={}'.format(facility_id), use_cache=True)    
        df = pd.DataFrame(data)
        # todo: Add check

        # parse result
        if id is not None:
            df = df[df['id'] == id]
        elif find_text is not None:
            df = df[df['title'].str.contains(find_text, case=False)]
        if frmt == 'df':
            return df
        else:
            return df.to_json(force_ascii=False)


    def get_holding(self, facility_id, id, find_text=None, use_cache=True):
        """
        Получить холдинг - получает все сайты, секции, субсекции, входящие в холдинг
        
        Parameters
        ----------

        facility_id : Установка: "desktop", "mobile", "desktop-pre". Обязательный параметр.
        
        id : идентификатор холдинга. Обязательный параметр
        
        
        find_text : Текст для поиска внутри холдинга по названию: сайта, секции, субсекции. 
        
        Returns
        -------
        
        DataFrame с найдеными объектами 
        
        """
        data = self.msapi_network.send_request('get', '/media/holdings/{}?facility_id={}'.format(id, facility_id))
        if 'id' in data:
            hid = data['id']
            title = data['title']
            jdata = []
            if len(data['sites']) > 0:
                for site in data['sites']:
                    if len(site['sections']) > 0:
                        for section in site['sections']:
                            if len(section['subSections']) > 0:
                                for subsection in section['subSections']:
                                    jdata.append(
                                        {'id': hid, 'title': title,
                                        'site_id': site['id'], 'site_title': site['title'],
                                        'section_id': section['id'], 'section_title': section['title'],
                                        'subsection_id': subsection['id'], 'subsection_title': subsection['title']
                                        })
                            else:
                                jdata.append(
                                    {'id': hid, 'title': title,
                                    'site_id': site['id'], 'site_title': site['title'],
                                    'section_id': section['id'], 'section_title': section['title']
                                    })
                    else:
                        jdata.append({'id': hid, 'title': title,
                                    'site_id': site['id'], 'site_title': site['title']
                                    })
            else:
                jdata.append({'id': hid, 'title': title})

            df = pd.DataFrame(jdata)
            df['id'] = df['id'].astype(str)
            df['site_id'] = df['site_id'].astype(str)
            df['section_id'] = df['section_id'].astype(str)
            df['subsection_id'] = df['subsection_id'].astype(str)
        else:
            df = pd.DataFrame(data)

        # todo: Add check

        # parse result
        if find_text is not None:
            df = df[df['title'].str.contains(find_text, case=False) | \
                    df['site_title'].str.contains(find_text, case=False) | \
                    df['section_title'].str.contains(find_text, case=False) | \
                    df['subsection_title'].str.contains(find_text, case=False) | \
                    df['site_id'].str.contains(find_text, case=False) | \
                    df['section_id'].str.contains(find_text, case=False) | \
                    df['subsection_id'].str.contains(find_text, case=False)
                    ]
        return df


    def load_mediatree(self, facility_id, holdings, reload=False):
        """
        Загружает список объектов входящих в холдинг из  списку холдингов переданных в параметре holdings, а затем сохраняет ее в DataFrame и внешний кэш-файл.
        При следующей загрузке данных, если не задан параметр reload=True, и объекты холдинга присутствуют в кэш-файле - холдинг загружается из кэша.
        Не рекомендуется передавать большой список холдингов на загрузку, т.к. получение каждого отдельного холдинга занимает время.
        
        
        Parameters
        ----------

        facility_id : str
            Установка: "desktop", "mobile", "desktop-pre". Обязательный параметр.
        
        holdings : DataFrame
            Список холдингов, по которым нужно нужно получить их состав
        
        
        reload : bool
            Флаг перезагрузки: 
                True - загружает информацию по холдингам без использования кэш-файла, т.е. получает информацию с сервера 
                False - использует кэш-файл.
            Рекомендуется переодически вызывать функцию с параметром reload=True, что бы получить актуальный состав холдинга
        
        Returns
        -------
        
        DataFrame с найдеными объектами входящими в список холдингов
        
        """
        
        holdings_filename = 'holdings.pikle'
        hids = pd.DataFrame()
        hids_list = []
        # check file
        if not os.path.exists(holdings_filename):
            reload = True
        else:
            # load pickle
            hids = pd.read_pickle(holdings_filename)
        if len(hids) == 0:
            reload = True
        i = 0
        for _, row in holdings.iterrows():
            hid = row['id']
            if reload or (len(hids) > 0 and len(hids[hids['id'] == str(hid)]) == 0):
                if len(hids) > 0:
                    hids.drop(hids[hids['id'] == str(hid)].index, inplace=True)
                h = self.get_holding(facility_id=facility_id, id=hid)
                hids_list.append(h)
            i += 1
        hids_list.append(hids)
        hids = pd.concat(hids_list, ignore_index=True)
        # save for next time
        hids.to_pickle(holdings_filename)
        return hids
