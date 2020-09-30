import requests
import datetime
import time
import json
from . import utils
from . import cache


class MediascopeApiNetwork:
    root_url='https://api.mediascope.net/responsum/api/v1'
    keycloak_url = 'https://auth.mediascope.net/auth/realms/DD/protocol/openid-connect/token'
    client_secret = '05e6f8cf-a16a-483f-8a22-9d6e91d65eff'

    # TODO: Добавить документацию для публичных методов

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(MediascopeApiNetwork, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.username, self.passw = utils.load_settings()
        self.token = {}
        

    def get_token(self, username, passw):
        my_tocken_req = requests.post(
            url=self.keycloak_url,
            # TODO: Вынести client_id и client_secret в
            data={
                'client_id': 'responsum',
                'client_secret': self.client_secret,
                'username': username,
                'password': passw,
                'grant_type': 'password',
                'scope': 'offline_access'},
            headers={'Content-Type': 'application/x-www-form-urlencoded'})
        if my_tocken_req.status_code == 200:
            t = my_tocken_req.json()
            t['now'] = datetime.datetime.now()
            return t
        else:
            raise Exception(f'Status code {my_tocken_req.status_code}', f'response: {my_tocken_req.text}')
        return None


    def refresh_token(self):
        if 'now' in self.token:
            now = time.mktime(datetime.datetime.now().timetuple())
            token_time = time.mktime(self.token['now'].timetuple())
            token_exp = self.token['expires_in']
            # print(now, token_time, token_exp, token_time + token_exp - now)
            if token_time + token_exp - now <= 0:
                self.token = self.get_token(self.username, self.passw)
        else:
            self.token = self.get_token(self.username, self.passw)


    def send_request(self, method, endpoint, data=None, use_cache=False):
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()

        # Check cache
        cache_query = f'{method}\n{endpoint}\n{data}'
        if use_cache and method in ['post', 'get']:
            cache_data = cache.get_cache(cache_query)
            if cache_data is not None:
                return cache_data
        
        # No cache, request service
        url = self.root_url + endpoint
        headers = {'Authorization': 'Bearer {}'.format(self.token['access_token'])}
        req = getattr(requests, method)(url=url, headers=headers, data=f'{data}')
        
        if req.status_code == 200:
            # try to save in cache for next use
            if use_cache:
                cache.save_cache(cache_query, req.json())
            return req.json()
        else:
            self._raise_error(req)
            return None
        


    def send_raw_request(self, method, endpoint, data=None, use_cache=False):
        # Send request
        if method not in ['post', 'get', 'delete']:
            raise ValueError(f'Method "{method}" is not supported')
        if data is None:
            data = []
        self.refresh_token()
        url = self.root_url + endpoint
        headers = {'Authorization': 'Bearer {}'.format(self.token['access_token'])}
        req = getattr(requests, method)(url=url, headers=headers, data=f'{data}')
        if req.status_code == 200:
            return req.text
        else:
            self._raise_error(req)
            return None

    def _raise_error(self, req):
        if req.status_code == 204:
            raise Exception(f'Нет данных', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 401:
            raise Exception(f'Не авторизирован', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 403:
            raise Exception(f'Доступ запрещен', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 400:
            raise Exception(f'Не верный запрос', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        elif req.status_code == 429:
            raise Exception(f'Слишком много запросов', f'Code: {req.status_code}, Сообщение: "{req.text}"')
        else:
            raise Exception(f'Ошибка', f'Code: {req.status_code}, Сообщение: "{req.text}"')
