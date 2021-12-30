import os
import pandas as pd
from ..core import net


class ResponsumCats:
    facility_id = None

    def __new__(cls, facility_id, settings_filename=None, cache_path=None, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            # print("Creating Instance")
            cls.instance = super(ResponsumCats, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, facility_id, settings_filename=None, cache_path=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # load holdings
        self.msapi_network = net.MediascopeApiNetwork(settings_filename, cache_path)
        if facility_id != self.facility_id or not hasattr(self, 'demattr') or not hasattr(self, 'holdings'):
            self.facility_id = facility_id
            self.demattr = self.get_demo()
            self.demattr_exp = self.get_demo(expand=True)
            self.holdings = self.get_holdings(facility_id)

    def get_demo(self, did=None, find_text=None, expand=True, frmt='df'):
        """
        Получить список демографических переменных: всеx, по id или поиском по названию.
        
        Parameters
        ----------

        did : int
            Идентификатор демографической переменной для того, чтобы получить одну переменную. По умолчанию - не задано
            (None).
        
        find_text : str
            Текст для поиска по названию переменной. По умолчанию - не задано (None).
        
        expand : bool
            Отобразить в таблице (DataFrame) доступные варианты для данной переменной - True/False? По умолчанию - True.
        
        frmt: str
            Формат вывода результата:
            - "df" - DataFrame,
            - "json" - JSON.
            По умолчанию - "df".
            
        
        Returns
        -------
        DataFrame

            DataFrame с демографическими переменными.
        """
        data = self.msapi_network.send_request('get', '/demo/variables', use_cache=True)
        res = []

        for item in data:
            is_found = False
            if did is not None and str(item['varId']) == str(did):
                is_found = True
            elif find_text is not None and (
                    item['varName'].lower().find(find_text.lower()) >= 0 or item['title'].lower()
                    .find(find_text.lower()) >= 0):
                is_found = True
            elif did is None and find_text is None:
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
                        if citem.get('catNum') is not None:
                            cobj['catNum'] = citem['catNum']
                        else:
                            cobj['catNum'] = ''
                        if citem.get('title') is not None:
                            cobj['catTitle'] = citem['title']
                        else:
                            cobj['catTitle'] = ''
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
            # df['catNum'] = df[['catNum']].dropna().astype('int')
            return df

    @staticmethod
    def get_demo_dict(df):
        """
        Получить список демографических атрибутов в виде словаря. Используется при формировании задания.
        Данный метод вызывается при инициализации.

        Parameters
        ----------

        df: DataFrame
            DataFrame со списком атрибутов

        Returns
        -------

        dict

            Словарь с демографическими переменными: {attribute_name: {k: {"%key%"}, v: {"%%val"}}}

        """
        d = {}
        for item in df[['varName', 'varId']].to_dict('split')['data']:
            key = item[0]
            val = item[1]
            d[key.lower()] = {'k': key, 'v': val}
        return d

    @staticmethod
    def get_population(names='Russia0+'):
        """
            ! Функция не используется.
            
            Получить список ID численности населения городов для фильтра по географии.
            Список строится на основе демографической переменной "Численность населения города" VarId=350.

            Parameters
            ----------

            names: str
                Cтрока, в которой перечислены географии в текстовом виде (через пробел, если географий несколько):

                * Russia0+ - Все города
                * Russia100- - Города с населением менее 100 тыс. человек
                * Russia100+ - Города с населением 100 тыс. человек и более
                * 700+ - Города с населением 700 тыс. человек и более
                * 400-700 - Города с населением от 400 до 700 тыс. человек
                * 100-400 - Города с населением от 100 до 400 тыс. человек

                Если список не задан или пустой, возвращается численность для Russia0+.

            Returns
            -------

            list
                Список идентификаторов Численности населения городов для заданных географий.

            Examples
            --------

            Получим список идентификаторов для России 100+:

            >>> import sys
            >>> sys.path.append("../../mediascope-api/")
            >>> from mediascope_api.responsum import catalogs as rc
            >>> facility = 'mobile'
            >>> rcats = rc.ResponsumCats(facility)
            >>> populations = rcats.get_population('Russia100+')
            >>> print(populations)
            >>> [1,2,3]
        """

        # ds = self.demattr_exp
        # return ds[ds.varId==350][['catNum', 'catTitle']].rename(columns={'catNum': 'id', 'catTitle': 'citySize'})
        pop_dict = {'russia0+': [1, 2, 3, 4],
                    'russia100-': [4],
                    'russia100+': [1, 2, 3],
                    '700+': [1],
                    '400-700': [2],
                    '100-400': [3],
                    '100-': [2],
                    }
        names = names.replace(',', '').replace(';', '').strip().lower()
        if len(names) == 0:
            names = 'russia100-'
        data = []
        for ut in names.split(' '):
            i = pop_dict.get(ut)
            if i is None:
                print(f"Внимание! : Численность'{ut}' не найдена.")
            else:
                data.extend(i)
        return list(set(data))

    @staticmethod
    def get_usetype(usetype_names='all'):
        """
        Получить список ID типов пользования Интернетом по текстовому описанию.

        Parameters
        ----------

        usetype_names: str

            Cтрока, в которой перечислены типы пользования Интернетом (через пробел, если типов пользования Интернетом несколько).

            Если не задано или строка пустая - возвращает все устройства.

            Допустимые значения:

                * all - Все устройства (usetype=1,2,3,4)
                * desktop - Только Desktop (usetype=1)
                * web - Desktop и Мобильный веб (usetype=1,2)
                * mobile - Мобильные веб, Мобильные приложения (usetype=2,3,4)
                * mobile-web - Мобильный веб (usetype=2)
                * mobile-app - Мобильные приложения (usetype=3,4)
                * mobile-app-online - Мобильные приложения онлайн (usetype=3)
                * mobile-app-offline - Мобильные приложения оффлайн (usetype=4)

        Returns
        -------

        list

            Список идентификаторов типов пользования Интернетом.


        Examples
        --------

        Получим список usetype для Desktop и Mobile-web:

        >>> import sys
        >>> sys.path.append("../../mediascope-api/")
        >>> from mediascope_api.responsum import catalogs as rc
        >>> facility = 'mobile'
        >>> rcats = rc.ResponsumCats
        >>> usetypes = rcats.get_usetype('desktop mobile-web')
        >>> print(usetypes)
        >>> [1,2]

        """
        usetypes_dict = {'all': [1, 2, 3, 4],
                         'desktop': [1],
                         'web': [1, 2],
                         'mobile': [2, 3, 4],
                         'mobile-web': [2],
                         'mobile-app': [3, 4],
                         'mobile-app-online': [3],
                         'mobile-app-offline': [4]}
        unames = usetype_names.replace(',', '').replace(';', '').strip().lower()
        if len(unames) == 0:
            unames = 'all'
        data = []
        for ut in unames.split(' '):
            i = usetypes_dict.get(ut)
            if i is None:
                print(f"Внимание! Тип использования: '{ut}' не найден.")
            else:
                data.extend(i)
        return list(set(data))

    @staticmethod
    def get_age_groups(ages='12+'):
        """
        ! Функция не используется.
        
        Получить список ID Возрастных групп для фильтра.

        Список строится на основе демографической переменной "Возрастные группы" VarId=170.

        Parameters
        ----------

        ages: str
            Cтрока, в которой перечислены возрастные группы (через пробел, если возрастных групп несколько):
                * 12+ - Население 12+ лет
                * 12-17 - Население 12-17 лет
                * 18-24 - Население 18-24 лет
                * 25-34 - Население 25-34 лет
                * 35-44 - Население 35-44 лет
                * 45-54 - Население 45-54 лет
                * 55-64 - Население 55-64 лет
                * 65+ - Население 65+ лет

            Если список не задан или пустой, возвращаются возврастные группы для 12+.

        Returns
        -------

        list
            Список идентификаторов Возрастных групп.

        Examples
        --------

        Получим список идентификаторов для населения 12+:
        
        >>> import sys
        >>> sys.path.append("../../mediascope-api/")
        >>> from mediascope_api.responsum import catalogs as rc
        >>> facility = 'mobile'
        >>> rcats = rc.ResponsumCats
        >>> ageids = rcats.get_age_groups('12+')
        >>> print(ageids)
        >>> [1,2,3]
        """
        pop_dict = {'12+': [1, 2, 3, 4, 5, 6, 7],
                    '12-17': [1],
                    '18-24': [2],
                    '25-34': [3],
                    '35-44': [4],
                    '45-54': [5],
                    '55-64': [6],
                    '65+': [7]
                    }
        ages = ages.replace(',', '').replace(';', '').strip().lower()
        if len(ages) == 0:
            ages = '12+'
        data = []
        for ut in ages.split(' '):
            i = pop_dict.get(ut)
            if i is None:
                print(f"Внимание! : Возрастная группа: '{ut}' не найдена.")
            else:
                data.extend(i)
        return list(set(data))

    def get_holdings(self, facility_id, branch='any', find_text=None, use_cache=True):
        """
        Получить список холдингов: всеx, по id или поиском по названию.
        Если branch или find_text не заданы, то возвращает все доступные холдинги.
        
        Parameters
        ----------
        facility_id : str
            Установка: "desktop", "mobile", "desktop_pre". Обязательный параметр.

        branch: str
            Ветка каталога для поиска:
            - any - Поиск во всех ветках (по умолчанию)
            - holding - Поиск в ветке "Холдинги"
            - agency - Поиск в ветке "Рекламные агентства"
            - network - Поиск в ветке "Рекламные сети"
        
        find_text: str
            Текст для поиска по названию холдинга. По умолчанию - не задано (None).

        use_cache : bool
            Использовать кэширование - Да/Нет (True/False)?

        Returns
        -------
        
        DataFrame с холдингами.
        
        """

        data = self.msapi_network.send_request('get', '/media/holdings?facility_id={}'.format(facility_id),
                                               use_cache=use_cache)
        jdata = []
        for holding in data:
            hid = holding['id']
            title = holding['title']
            if holding['holding']:
                branch_type = 'holding'
            elif holding['adAgency']:
                branch_type = 'agency'
            elif holding['network']:
                branch_type = 'network'
            else:
                branch_type = 'any'

            if len(holding['sites']) > 0:
                for site in holding['sites']:
                    if len(site['sections']) > 0:
                        for section in site['sections']:
                            if len(section['subSections']) > 0:
                                for subsection in section['subSections']:
                                    jdata.append(
                                        {'holding_id': hid, 'holding_title': title,
                                         'site_id': site['id'], 'site_title': site['title'],
                                         'section_id': section['id'], 'section_title': section['title'],
                                         'subsection_id': subsection['id'], 'subsection_title': subsection['title'],
                                         'branch': branch_type
                                         })
                            else:
                                jdata.append(
                                    {'holding_id': hid, 'holding_title': title,
                                     'site_id': site['id'], 'site_title': site['title'],
                                     'section_id': section['id'], 'section_title': section['title'],
                                     'branch': branch_type
                                     })
                    else:
                        jdata.append({'holding_id': hid, 'holding_title': title,
                                      'site_id': site['id'], 'site_title': site['title'],
                                      'branch': branch_type
                                      })
            else:
                jdata.append({'holding_id': hid, 'holding_title': title, 'branch': branch_type})

        df = pd.DataFrame(jdata)
        df['holding_id'] = df['holding_id'].astype(str)
        df['site_id'] = df['site_id'].astype(str)
        df['section_id'] = df['section_id'].astype(str)
        df['subsection_id'] = df['subsection_id'].astype(str)

        # parse result
        if find_text is not None:
            if branch == 'any':
                df = df[df['holding_title'].str.contains(find_text, case=False) |
                        df['site_title'].str.contains(find_text, case=False) |
                        df['section_title'].str.contains(find_text, case=False) |
                        df['subsection_title'].str.contains(find_text, case=False) |
                        df['holding_id'].str.contains(find_text, case=False) |
                        df['site_id'].str.contains(find_text, case=False) |
                        df['section_id'].str.contains(find_text, case=False) |
                        df['subsection_id'].str.contains(find_text, case=False)
                        ]
            else:
                df = df[df['branch'].str.equal(branch) &
                        (df['holding_title'].str.contains(find_text, case=False) |
                         df['site_title'].str.contains(find_text, case=False) |
                         df['section_title'].str.contains(find_text, case=False) |
                         df['subsection_title'].str.contains(find_text, case=False) |
                         df['holding_id'].str.contains(find_text, case=False) |
                         df['site_id'].str.contains(find_text, case=False) |
                         df['section_id'].str.contains(find_text, case=False) |
                         df['subsection_id'].str.contains(find_text, case=False))
                        ]

        return df

    def get_holding(self, facility_id, hid, find_text=None):
        """
        Получить холдинг - получает все сайты, секции, субсекции, входящие в холдинг.
        
        Parameters
        ----------

        facility_id : Установка: "desktop", "mobile", "desktop_pre". Обязательный параметр.
        
        hid : идентификатор холдинга. Обязательный параметр.
        
        
        find_text : Текст для поиска внутри холдинга по названию сайта, секции, субсекции. 
        
        Returns
        -------
        
        DataFrame с найденными объектами.
        
        """
        data = self.msapi_network.send_request('get', '/media/holdings/{}?facility_id={}'.format(hid, facility_id))
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
                                        {'holding_id': hid, 'holding_title': title,
                                         'site_id': site['id'], 'site_title': site['title'],
                                         'section_id': section['id'], 'section_title': section['title'],
                                         'subsection_id': subsection['id'], 'subsection_title': subsection['title']
                                         })
                            else:
                                jdata.append(
                                    {'holding_id': hid, 'holding_title': title,
                                     'site_id': site['id'], 'site_title': site['title'],
                                     'section_id': section['id'], 'section_title': section['title']
                                     })
                    else:
                        jdata.append({'holding_id': hid, 'holding_title': title,
                                      'site_id': site['id'], 'site_title': site['title']
                                      })
            else:
                jdata.append({'holding_id': hid, 'holding_title': title})

            df = pd.DataFrame(jdata)
            df['holding_id'] = df['holding_id'].astype(str)
            df['site_id'] = df['site_id'].astype(str)
            df['section_id'] = df['section_id'].astype(str)
            df['subsection_id'] = df['subsection_id'].astype(str)
        else:
            df = pd.DataFrame(data)
        # parse result
        if find_text is not None:
            df = df[df['holding_title'].str.contains(find_text, case=False) |
                    df['site_title'].str.contains(find_text, case=False) |
                    df['section_title'].str.contains(find_text, case=False) |
                    df['subsection_title'].str.contains(find_text, case=False) |
                    df['site_id'].str.contains(find_text, case=False) |
                    df['section_id'].str.contains(find_text, case=False) |
                    df['subsection_id'].str.contains(find_text, case=False)
                    ]
        return df

    def load_mediatree(self, facility_id, holdings, reload=False):
        """
        Загружает список объектов, входящих в холдинг, из  списка холдингов, переданных в параметре holdings,
        а затем сохраняет его в DataFrame и внешний кэш-файл.
        При следующей загрузке данных, если не задан параметр reload=True и объекты холдинга присутствуют в
        кэш-файле, холдинг загружается из кэша.
        Не рекомендуется передавать большой список холдингов на загрузку, т.к. получение каждого отдельного холдинга
        занимает время.
        
        
        Parameters
        ----------

        facility_id : str
            Установка: "desktop", "mobile", "desktop_pre". Обязательный параметр.
        
        holdings : DataFrame
            Список холдингов, по которым нужно получить их состав.
        
        
        reload : bool
            Флаг перезагрузки: 
                True - загружает информацию по холдингам без использования кэш-файла, т.е. получает информацию с сервера.
                False - использует кэш-файл.
            Рекомендуется периодически вызывать функцию с параметром reload=True, чтобы получить актуальный
            состав холдинга.
        
        Returns
        -------
        
        DataFrame с найденными объектами, входящими в список холдингов.
        
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
                h = self.get_holding(facility_id=facility_id, hid=hid)
                hids_list.append(h)
            i += 1
        hids_list.append(hids)
        hids = pd.concat(hids_list, ignore_index=True)
        # save for next time
        hids.to_pickle(holdings_filename)
        return hids

    def find_media(self, find_text, branch='any', find_in=None, limit=None):
        """
        Поиск по медиа-дереву.

        Parameters
        ----------

        find_text: str

            Поисковое выражение, по которому осуществляется поиск.
            Ищется вхождение выражения в любое из полей, если поля не ограничены списком find_in.

        branch: Ветка каталога для поиска:
            - any - Поиск во всех ветках (по умолчанию)
            - holding - Поиск в ветке "Холдинги"
            - agency - Поиск в ветке "Рекламные агентства"
            - network - Поиск в ветке "Рекламные сети"

        find_in: list
            Список полей каталога, по которым осуществляется поиск:
            - any - все поля (по умолчанию)
            - holding
            - site
            - section
            - subsection

        limit : int
            Максимальное количество записей в результате.
            По умолчанию выводятся все записи.

        Returns
        -------

        data : DataFrame

            DataFrame с найденными выражениями.

        """
        fin_list = ['any', 'holding', 'site', 'project', 'section', 'subsection']
        if find_in is not None:
            # check find_in
            if type(find_in) == str:
                find_in = [str(find_in).strip()]
            if type(find_in) == list:
                is_error = False
                for f in find_in:
                    if str(f).lower() not in fin_list:
                        print(f"Неизвестный атрибут для поиска: {f}.")
                        is_error = True
                if is_error:
                    print(f"Допустимые атрибуты: {(','.join(fin_list))[:-1]}'")
                    return None
        else:
            find_in = fin_list

        df = self.holdings
        if branch == 'holding' or branch == 'agency' or branch == 'network':
            df = df[df['branch'] == branch]

        text = find_text.strip()
        if len(text) > 0:
            if find_in is not None and type(find_in) == list:
                dframes = list()
                for f in find_in:
                    if f == 'any':

                        dframes.append(df[df['holding_title'].str.contains(find_text, case=False) |
                                          df['site_title'].str.contains(find_text, case=False) |
                                          df['section_title'].str.contains(find_text, case=False) |
                                          df['subsection_title'].str.contains(find_text, case=False) |
                                          df['holding_id'].str.contains(find_text, case=False) |
                                          df['site_id'].str.contains(find_text, case=False) |
                                          df['section_id'].str.contains(find_text, case=False) |
                                          df['subsection_id'].str.contains(find_text, case=False)]
                                       )

                    elif f == 'holding':
                        dframes.append(df[df['holding_title'].str.contains(find_text, case=False) |
                                          df['holding_id'].str.contains(find_text, case=False)]
                                       )
                    elif f == 'site' or f == 'project':
                        dframes.append(df[df['site_id'].str.contains(find_text, case=False) |
                                          df['site_title'].str.contains(find_text, case=False)]
                                       )
                    elif f == 'section':
                        dframes.append(df[df['section_id'].str.contains(find_text, case=False) |
                                          df['section_title'].str.contains(find_text, case=False)]
                                       )
                    elif f == 'subsection':
                        dframes.append(df[df['subsection_id'].str.contains(find_text, case=False) |
                                          df['subsection_title'].str.contains(find_text, case=False)]
                                       )
                df = pd.concat(dframes).drop_duplicates()

        if limit is None or limit <= 0:
            return df
        else:
            return df.head(limit)
