import os
import hashlib
import json

cache_path = '../.cache'


def get_hash(query):
    """
        Получить Хэш для запроса

        Returns
        -------

        hash : str
            Md5 хзш
    """
    return hashlib.md5(query.encode('utf-8')).hexdigest()


def get_cache(query):
    """
        Получить объект из кэша по его хэшу

         Parameters
        ----------

        query : str
            md5 хзш объекта

        Returns
        -------

        obj : json
            Хзшрованный объект
    """
    h = get_hash(query)
    cache_fname = _get_cache_fname(h)
    if not os.path.exists(cache_fname):
        return None
    with open(cache_fname, 'r') as f:
        return json.load(f)


def save_cache(query, jdata):
    """
        Сохранить объект в кэш-файл

         Parameters
        ----------

        query : str
            запрос по которому формируются данные - задание для api

        jdata : json
            данные для кэширования
         объект
    """
    h = get_hash(query)
    cache_file = _get_cache_fname(h)
    with open(cache_file, 'w') as f:
        json.dump(jdata, f)


def _get_cache_fname(h):
    file_path = os.path.join(cache_path, h + '.cache')
    if not os.path.exists(cache_path):
        os.makedirs(cache_path, exist_ok=True)
    return file_path
