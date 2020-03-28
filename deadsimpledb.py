import logging

import os
import pickle
import shutil
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import PIL.Image
import simplejson as json

SUPPORTED_FILE_TYPES = ['png', 'jpg', 'pkl', 'json']

class NpEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)

def key_as_str(key):
    return ",".join([str(k) for k in key])

def str_as_key(s):
    return s.split(",")

def get_filetype(filepath_prefix):
    for ft in SUPPORTED_FILE_TYPES:
        if os.path.exists(filepath_prefix + "." + ft):
            return ft
    return None

class DeadSimpleDB(object):
    def __init__(self, root_path=None, overwrite=False):
        if root_path is None:
            root_path = "deadsimpledb"

        self.root_path = root_path

        if overwrite:
            logging.info("Overwrite enabled...")
            if os.path.exists(self.root_path):
                logging.info(
                    ".... Removing directory {}".format(self.root_path))
                shutil.rmtree(self.root_path)
            else:
                logging.info("No folder exists, not overwriting")

        if not os.path.exists(self.root_path):
            os.makedirs(self.root_path)

        self.data_store: Dict[str, Dict[str, Any]] = {}

    def check_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def update_dict(self, key, value, name='data', stype="json", flush=False, clear_cache=False):
        value_dict = self.get(key, name, stype=stype)
        if value_dict is None:
            value_dict = {}
            self.save(key,name=name,value=value_dict,stype=stype,flush=flush, clear_cache=clear_cache)
        value_dict.update(value)
        if flush:
            self.flush(key, name, clear_cache)

    def append_to_list(self, key, value, name='data', stype="json", flush=False, clear_cache=False):
        value_list = self.get(key, name, stype=stype)
        if value_list is None:
            value_list = []
            self.save(key,name=name,value=value_list,stype=stype,flush=flush, clear_cache=clear_cache)
        value_list.append(value)
        if flush:
            self.flush(key, name, clear_cache)

    def save(self, key, value, name='data', stype="json", flush=False, clear_cache=False):
        lookup_key = key_as_str(key)
        items = self.data_store.get(lookup_key, {})
        items[name] = {
            'key': key,
            'value': value,
            'name': name,
            'stype': stype,
            'dirty': True,
            'last_access': time.time()}
        self.data_store[lookup_key] = items
        if flush:
            self.flush(key, name, clear_cache)

    def get(self, key, name="data", stype=None):
        if type(key) is not list:
            raise Exception("Incorrect key type")
        items = self.data_store.get(key_as_str(key), None)
        if items is not None and name in items:
            entry = items.get(name)
            entry['last_accessed'] = time.time()
            return entry.get('value')
        else:
            data, stype = self.read(key, name, stype)
            if data is None:
                return None
            self.save(key, data, name=name, stype=stype)
            return data

    def delete(self,key, name="data", stype=None):
        items = self.data_store.get(key_as_str(key))
        path = self._get_path_from_key(key)
        filepath_prefix = os.path.join(path, "{}".format(name))
        if stype is None:
            stype = get_filetype(filepath_prefix)
            if stype is None:
                raise Exception("Not found")
        del items[name]
        filepath = os.path.join(path, "{}.{}".format(name, stype.lower()))
        os.remove(filepath)
        if len(items) == 0:
            del self.data_store[key_as_str(key)]
            if len(os.listdir(path))==0:
                os.rmdir(path)
            
            # os.rmdir(path)

    def flush(self, key, name='data', clear_cache=False):
        items =  self.data_store[key_as_str(key)]

        entry = items.get(name)
        if clear_cache:
            del items[name]
            if len(items) == 0:
                del self.data_store[key_as_str(key)]
          
        entry['dirty'] = False
        self.write(key, name=name, value=entry['value'], stype=entry['stype'])

    def _get_path_from_key(self,key):
        path_parts = [str(k) for k in [self.root_path] + key]
        path = os.path.join(*path_parts)
        self.check_path(path)
        return path

    def flush_all(self, dirty_only=True, clear_cache=False):
        for lookup_key, items in self.data_store.items():
            key = str_as_key(lookup_key)
            for name, entry in items.items():
                if entry['dirty'] and dirty_only:
                    self.flush(key, name, clear_cache=clear_cache)

    def write(self, key, value, name='data', stype="json"):
        path = self._get_path_from_key(key)
        filepath = os.path.join(path, "{}.{}".format(name, stype.lower()))
        if stype == "json":
            with open(filepath, 'w') as f:
                json.dump(value, f, ignore_nan=True, cls=NpEncoder)
        elif stype == "pkl":
            with open(filepath, 'wb') as f:
                pickle.dump(value, f)
        elif stype == "png" or stype == "jpg":
            im = PIL.Image.fromarray(value)
            im.save(filepath)
        else:
            raise Exception("File type not supported: {}".format(stype))

    def read(self, key, name="data", stype=None, default_value=None):
        path = self._get_path_from_key(key)
        filepath_prefix = os.path.join(path, "{}".format(name))
        if stype is None:
            stype = get_filetype(filepath_prefix)
            if stype is None:
                return None, None
        else:
            return None, None
        filepath = filepath_prefix + "." + stype.lower()

        if not os.path.isfile(filepath):
            return default_value, stype

        if stype.lower() == "json":
            with open(filepath, 'r') as f:
                value = json.load(f)
        elif stype == "pkl":
            with open(filepath, 'rb') as f:
                value = pickle.load(f)
        else:
            raise Exception("Unsupported format {}".format(stype))
        return value, stype
