import requests
from urllib3.util import Retry
from requests import Session
from requests.adapters import HTTPAdapter
import datetime
import time
import re
import os
from . import errors
from . import utils
from . import cache


class MediascopeApiNetwork:
    DEFAULT_SETTINGS_FILENAME = 'settings.json'

    def __new__(cls, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        return super(MediascopeApiNetwork, cls).__new__(cls, *args, **kwargs) #cls.instance

    def __init__(self, settings_filename: str = None, cache_path: str = None, cache_enabled: bool = True,
                 username: str = None, passw: str = None, root_url: str = None, client_id: str = None,
                 client_secret: str = None, keycloak_url: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if cache_path is not None and cache_enabled:
            cache.cache_path = cache_path
        elif not cache_enabled:
            cache.cache_path = None

        proxy_server = None

        if username is not None and passw is not None and root_url is not None and client_id is not None \
                and client_secret is not None and keycloak_url is not None:
            self.username = username
            self.passw = passw
            self.root_url = root_url
            self.client_id = client_id
            self.client_secret = client_secret
            self.keycloak_url = keycloak_url
        else:
            if settings_filename is None:
                if os.path.exists(self.DEFAULT_SETTINGS_FILENAME):
                    settings_filename = self.DEFAULT_SETTINGS_FILENAME
                else:
                    raise Exception('Не указаны настройки для подключения к Mediascope-API')

            self.username, self.passw, self.root_url, self.client_id, \
                self.client_secret, self.keycloak_url, proxy_server = utils.load_settings(settings_filename)

        if self.username is None or self.passw is None or self.root_url is None and \
                self.client_id is None or self.client_secret is None or self.keycloak_url is None:
            raise Exception('Не указаны настройки для подключения к Mediascope-API')

        self.token = {}

        self.proxies = None
        if proxy_server is not None:
            self.proxies = {"https": proxy_server}
            print(f"Подсоединение через прокси {self.proxies}")
        else:
            self.proxies = {"https": ""}

        # настройка механизма автоматических повторных запросов
        self.session = Session()

        self.retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[502, 503, 504],
            allowed_methods={'POST', 'GET', 'DELETE'},
        )

        self.session.mount('https://', HTTPAdapter(max_retries=self.retries))

    def get_token(self, username: str, passw: str) -> dict:
        """
        Получить токен по имени пользователя и паролю

        Parameters
        ----------

        username : str
            Имя пользователя (login)

        passw : str
            Пароль пользователя

        Returns
        -------

        token : dict
            Токен доступа к Mediascope-API
        """
        my_token_req = self.session.post(
            url=self.keycloak_url,
            data={
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'username': username,
                'password': passw,
                'grant_type': 'password'
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            proxies=self.proxies
        )
        if my_token_req.status_code == 200:
            t = my_token_req.json()
            t['now'] = datetime.datetime.now()
            return t
        elif my_token_req.status_code == 401:
            raise Exception(f'Ошибка авторизации!',
                            f'Не верный логин или пароль. Проверьте параметры указанные в файле: settings.json')
        elif my_token_req.status_code == 403:
            raise Exception(f'Ошибка авторизации!', f'Доступ запрещен.')
        else:
            raise Exception(f'Status code {my_token_req.status_code}', f'response: {my_token_req.text}')

    def refresh_token(self):
        """
        Обновить текущий токен или получить новый с использованием username и passw сохраненных в настройках
        """
        if 'now' in self.token:
            now = time.mktime(datetime.datetime.now().timetuple())
            token_time = time.mktime(self.token['now'].timetuple())
            token_exp = self.token['expires_in']
            if token_time + token_exp - now <= 0:
                self.token = self.get_token(self.username, self.passw)
        else:
            self.token = self.get_token(self.username, self.passw)

    def send_request(self, method: str, endpoint: str, data: dict = None, use_cache: bool = False):
        """
        Отправляет запрос в Mediascope-API

        Parameters
        ----------

        method : str
            HTTP метод:
                - get
                - post

        endpoint : str
            Путь к точке API, к которому идет обращение. Конкатенируется с основным URL
            Пример:
                /task/duplication

        data : dict
            Данные отправляемые в запросе к API

        use_cache : bool
            Флаг кэширования
                - True - использовать кэш. Формирует хэш для запроса, и сохраняет результат в файл.
                        Если следующие запросы совпадут по хэшу с существующим - результат возьмется из сохраненного
                        файла. Запроса к API не будет. Удобно использовать для частых запросов к большим объемам.
                - False - кэш не используется (по умолчанию).

        Returns
        -------

        result : dict
            Результат выполнения запроса
        """
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()

        # Check cache
        cache_query = f'{method}\n{endpoint}\n{data}'
        if use_cache and method in ['post', 'get']:
            cache_data = cache.get_cache(cache_query, self.username)
            if cache_data is not None:
                return cache_data

        # No cache, request service
        url = self.root_url + endpoint
        headers = {'Authorization': f'Bearer {self.token["access_token"]}',
                   'Content-Type': 'application/json; charset=utf-8'
                   }
        req = getattr(self.session, method)(url=url, headers=headers, data=f'{data}'.encode('utf-8'), proxies=self.proxies)

        if req.status_code == 200:
            # try to save in cache for next use
            rj = self._req_to_json(req, endpoint, data)
            if use_cache:
                cache.save_cache(cache_query, rj, self.username)
            return rj
        else:
            self._raise_error(req)
            return None

    def send_request_lo(self, method: str, endpoint: str, data: dict = None,
                        use_cache: bool = False, limit: int = 1000):
        """
        Отправляет запрос в Mediascope-API

        Parameters
        ----------

        method : str
            HTTP метод:
                - get
                - post

        endpoint : str
            Путь к точке API, к которому идет обращение. Конкатенируется с основным URL
            Пример:
                /task/duplication

        data : dict
            Данные отправляемые в запросе к API

        use_cache : bool
            Флаг кэширования
                - True - использовать кэш. Формирует хэш для запроса и сохраняет результат в файл.
                        Если следующие запросы совпадут по хэшу с существующим - результат возьмется из сохраненного
                        файла. Запроса к API не будет. Удобно использовать для частых запросов к большим объемам.
                - False - кэш не используется (по умолчанию).

        limit : int
            Размер порции данных получаемых за один запрос

        Returns
        -------

        result : dict
            Результат выполнения запроса
        """
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []

        # Check cache
        cache_query = f'{method}\n{endpoint}\n{data}'
        if use_cache and method in ['post', 'get']:
            cache_data = cache.get_cache(cache_query, self.username)
            if cache_data is not None:
                return cache_data

        result = {'header': {'total': 0}}
        result_data = []
        offset = 0
        is_reading = True

        while is_reading:
            self.refresh_token()

            # No cache, request service
            if endpoint.rfind('?') >= 0:
                url = self.root_url + endpoint + f'&offset={offset}&limit={limit}'
            else:
                url = self.root_url + endpoint + f'?offset={offset}&limit={limit}'
            headers = {'Authorization': f'Bearer {self.token["access_token"]}',
                       'Content-Type': 'application/json'
                       }
            req = getattr(self.session, method)(url=url, headers=headers, data=f'{data}', proxies=self.proxies)

            if req.status_code == 200:
                # try to save in cache for next use
                rj = self._req_to_json(req, endpoint, data)
                if rj is None or type(rj) != dict:
                    break
                if 'header' not in rj or 'data' not in rj:
                    break

                header = rj['header']
                if 'total' not in header:
                    is_reading = False
                    total = limit
                total = int(header['total'])
                result['header']['total'] = total
                offset += limit
                if offset >= total:
                    is_reading = False

                if type(rj['data']) == list:
                    result_data.extend(rj['data'])
            else:
                self._raise_error(req)
                break

        result['data'] = result_data
        if use_cache:
            cache.save_cache(cache_query, result, self.username)
        return result

    def send_raw_request(self, method: str, endpoint: str, data: dict = None):
        """
        Отправляет запрос в Mediascope-API

        Parameters
        ----------

        method : str
            HTTP метод:
                - get
                - post

        endpoint : str
            Путь к точке API, к которому идет обращение. Конкатенируется с основным URL
            Пример:
                /task/duplication

        data : dict
            Данные отправляемые в запросе к API

        use_cache : bool
            Флаг кэширования
                - True - использовать кэш. Формирует хэш для запроса и сохраняет результат в файл.
                        Если следующие запросы совпадут по хэшу с существующим - результат возьмется из сохраненного
                        файла. Запроса к API не будет. Удобно использовать для частых запросов к большим объемам.
                - False - кэш не используется (по умолчанию).

        limit : int
            Размер порции данных получаемых за один запрос

        Returns
        -------

        result : dict
            Результат выполнения запроса
        """
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()
        url = self.root_url + endpoint
        headers = {'Authorization': f'Bearer {self.token["access_token"]}',
                   'Content-Type': 'application/json'
                   }
        req = getattr(self.session, method)(url=url, headers=headers, data=f'{data}', proxies=self.proxies)
        if req.status_code == 200:
            return req.text
        else:
            self._raise_error(req)
            return None


    def send_crossweb_request(self, method: str, endpoint: str, data: dict = None):
        """
        Отправляет запрос в Mediascope-API для проекта CrossWeb

        method : str
            HTTP метод:
                - get
                - post

        endpoint : str
            Путь к точке API, к которому идет обращение. Конкатенируется с основным URL
            Пример:
                /task/duplication

        data : dict
            Данные отправляемые в запросе к API

        use_cache : bool
            Флаг кэширования
                - True - использовать кэш. Формирует кэш для запроса, и сохраняет результат в файл.
                        Если следующие запросы совпадут по хэшу с существующим - результат возьмется из сохраненного
                        файла. Запроса к API не будет. Удобно использовать для частых запросов к большим объемам.
                - False - кэш не используется (по умолчанию).

        Returns
        -------

        result : str
            Результат выполнения запроса
        """
        # "Задача 2942b28b-d62e-42fb-a013-0ab067d21588 поступила в обработку."
        rx = re.compile(r"Задача (?P<taskid>[0-9a-f-]+) поступила в обработку..*")
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()
        url = self.root_url + endpoint
        headers = {'Authorization': f'Bearer {self.token["access_token"]}',
                   'Content-Type': 'application/json'
                   }
        req = getattr(self.session, method)(url=url, headers=headers, data=f'{data}', proxies=self.proxies)
        if req is not None and req.status_code == 200:
            mh = rx.match(req.text)
            if mh is not None:
                taskid = mh.group(1)
                return {"taskId": taskid, "message": req.text}

        elif req is not None:
            self._raise_error(req)
            return None

    def get_curl_request(self, method: str, endpoint: str, data: dict = None) -> str:
        """
        Формирует запрос к Mediascope-API в CURL формате.
        Можно вставлять в консоль и обратиться к API прямо из консоли.
        При этом используется текущий токен пользователи или получается новый.

        method : str
            HTTP метод:
                - get
                - post

        endpoint : str
            Путь к точке API, к которому идет обращение. Конкатенируется с основным URL
            Пример:
                /task/duplication

        data : dict
            Данные отправляемые в запросе к API

        use_cache : bool
            Флаг кэширования
                - True - использовать кэш. Формирует кэш для запроса, и сохраняет результат в файл.
                        Если следующие запросы совпадут по хэшу с существующим - результат возьмется из сохраненного
                        файла. Запроса к API не будет. Удобно использовать для частых запросов к большим объемам.
                - False - кэш не используется (по умолчанию).

        Returns
        -------

        result : str
            Текст в виде команды CURL. Можно вставлять в консоль и обратиться к API прямо из консоли
        """

        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()
        url = self.root_url + endpoint

        treq = list()
        treq.append(f"curl --location --request {str(method).upper()} '{url}'")
        treq.append(f"--header 'Content-Type: application/json'")
        treq.append(f"--header 'Authorization: Bearer {self.token['access_token']}'")
        if len(data) > 0:
            treq.append(f"--data-raw '{data}'")
        print(" \\\n".join(treq))

    @staticmethod
    def _req_to_json(req, endpoint, data):
        try:
            result = req.json()
            return result
        except:
            log_file_name = 'json-error-' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.txt'
            with open(log_file_name, "w") as log_file:
                log_file.write("JSON decode error")
                log_file.write("\nEndpoint\n")
                log_file.write(endpoint)
                log_file.write("\nRequest body\n")
                log_file.write(str(data))
                log_file.write("\nJSON request answer\n")
                log_file.write(req.text)
            raise Exception('Ошибка', f'Ошибка JSON ответа. Пришлите файл {log_file_name} в Mediascope')

    @staticmethod
    def _raise_error(req):
        if req.status_code == 204:
            raise Exception('Нет данных', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 401:
            raise Exception('Не авторизирован', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 403:
            raise Exception('Доступ запрещен', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 400:
            raise errors.HTTP400Error(req.status_code, req.text)
        elif req.status_code == 429:
            raise Exception('Слишком много запросов', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 404:
            raise errors.HTTP404Error(f'Code: {req.status_code}, Адрес или задача не найдена: "{req.text}"')
        else:
            raise Exception('Ошибка', f'Code: {req.status_code}, Сообщение: "{req.text}"')
