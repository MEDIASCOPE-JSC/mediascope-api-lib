import os
import hashlib
import json

# TODO: Добавить документацию для публичных методов

cache_path='../.cache'
    
def get_hash(query):
    return hashlib.md5(query.encode('utf-8')).hexdigest()

def get_cache(query):
    h = get_hash(query)
    cache_fname = get_cache_fname(h)
    print(cache_fname)
    if not os.path.exists(cache_fname):
        return None
    with open(cache_fname, 'r') as f:
        return json.load(f)

def save_cache(query, jdata):
    h = get_hash(query)
    cache_file = get_cache_fname(h)
    with open(cache_file, 'w') as f:
        json.dump(jdata, f)

def get_cache_fname(hash):
    file_path = os.path.join(cache_path, hash + '.cache')
    if not os.path.exists(cache_path):
        os.makedirs(cache_path, exist_ok=True)
    return file_path
        
