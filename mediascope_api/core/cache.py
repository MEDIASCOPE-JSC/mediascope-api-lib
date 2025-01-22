"""
Cache module
"""
import os
import hashlib
import json
import pathlib
from datetime import datetime

CACHE_PATH = '../.cache'


def get_hash(query: str) -> str:
    """
        Получить Хэш для запроса

        Returns
        -------

        hash : str
            Md5 хэш
    """
    return hashlib.md5(query.encode('utf-8')).hexdigest()


def get_cache(query: str, login: str = 'default'):
    """
        Получить объект из кэша по его хэшу

        Parameters
        ----------

        query : str
            md5 хэш объекта

        login : str
            Логин пользователя, добавляется в имя файла для обеспечения уникальности кэша

        Returns
        -------

        obj : json
            Хэшрованный объект
    """
    if CACHE_PATH is None:
        return None
    h = get_hash(query)
    h = login + '-' + get_hash(query)
    cache_filename = _get_cache_fname(h)
    if not os.path.exists(cache_filename):
        return None
    if not _check_cache_is_valid(cache_filename):
        return None
    with open(cache_filename, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_cache(query: str, jdata, login: str='default'):
    """
        Сохранить объект в кэш-файл

        Parameters
        ----------

        query : str
            запрос по которому формируются данные - задание для api

        jdata : dict
            данные для кэширования
        login : str
            Логин пользователя, добавляется в имя файла для обеспечения уникальности кэша
    """

    if CACHE_PATH is None:
        return None
    h = login + '-' + get_hash(query)
    cache_file = _get_cache_fname(h)
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(jdata, f)


def _get_cache_fname(h: str) -> str:
    file_path = os.path.join(CACHE_PATH, h + '.cache')
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH, exist_ok=True)
    return file_path


def _check_cache_is_valid(filename: str) -> bool:
    fname = pathlib.Path(filename)
    if fname.exists():
        ctime = datetime.fromtimestamp(fname.stat().st_ctime)
        td = datetime.now() - ctime
        if td.total_seconds() < 86400:
            return True
    return False
