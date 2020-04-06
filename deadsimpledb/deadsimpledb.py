import logging

import os
import pickle
import shutil
import time
from typing import Any, Dict, List, Tuple

import numpy as np
import PIL.Image
import simplejson as json
from os.path import isfile, join
import threading
from multiprocessing import Queue
SUPPORTED_FILE_TYPES = ['png', 'jpg', 'pkl', 'json']

class JSONEncoderDefault(json.JSONEncoder):

    def default(self, obj):  # pylint: disable=E0202
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            if obj.size() > 1000:
                return "REDACTED: NUMPY OBJ OF SIZE {} TOO LARGE".format(obj.size())
            else:
                return obj.tolist()
        else:
            try:
                return super(JSONEncoderDefault, self).default(obj)
            except Exception as e:
                return "ENCODE_FAILED:{}_AS_STR:{}".format(type(obj),obj)


class JSONDecoderDefault(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):  # pylint: disable=E0202
        return obj


def format_key(key):
    return tuple([str(k) for k in key])

def get_filetype(filepath_prefix):
    for ft in SUPPORTED_FILE_TYPES:
        if os.path.exists(filepath_prefix + "." + ft):
            return ft
    return None


class DeadSimpleDB:

    def __init__(self, 
        root_path=None, 
        overwrite=False, 
        json_encoder=JSONEncoderDefault, 
        json_decoder=JSONDecoderDefault,
        ignore_cache=False,
        test_mode=False,
        use_write_thread=True):

        self.json_encoder = json_encoder
        self.json_decoder = json_decoder

        if root_path is None:
            root_path = "deadsimpledb"

        self.root_path = root_path
        self.ignore_cache = ignore_cache

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
        self.test_mode = test_mode
        self.running = True

        self.data_store: Dict[str, Dict[str, Any]] = {}
        self.use_write_thread = use_write_thread

        if self.use_write_thread:
            self.write_queue= Queue()

            self.write_lock = threading.Lock()
            self.writer_thread = threading.Thread(target=self._process_write_requests, args=())
            self.writer_thread.daemon = True
            self.writer_thread.start()

    def _process_write_requests(self):
        while self.running:
            with self.write_lock:
                items = {}
                while (self.write_queue.qsize() != 0):
                    key,name,clear_cache = self.write_queue.get()
                    items[(key,name)] = clear_cache

                for key_name,clear_cache in items.items():
                    key,name = key_name
                    self._flush_sync(key,name,clear_cache)
            # time.sleep(0.01)
            
            
    def check_path(self, path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)

    def update_dict(self, key, value, name='data', stype="json", clear_cache=False):
        key = format_key(key)
        value_dict = self.get(key, name, stype=stype)
        if value_dict is None:
            value_dict = {}
            value_dict.update(value)
            self.save(key, name=name, value=value_dict, stype=stype, clear_cache=clear_cache)
        else:
            value_dict.update(value) 
            self._flush(key, name, clear_cache)

    def remove_items_from_dict(self, key, items, name='data', stype="json", clear_cache=False):
        key = format_key(key)

        value_dict = self.get(key, name, stype=stype)
        if value_dict is None:
            return
        for item in items:
            value_dict.pop(item, None)
        self._flush(key, name, clear_cache)

    def append_to_list(self, key, value, name='data', stype="json", clear_cache=False):
        value_list = self.get(key, name, stype=stype)
        if value_list is None:
            value_list = []
            value_list.append(value)
            self.save(key, name=name, value=value_list, stype=stype, clear_cache=clear_cache)
        else:
            value_list.append(value)
            self._flush(key, name, clear_cache)

    def save(self, key, value, name='data', stype="json", clear_cache=False):
        key = format_key(key)
        items = self.data_store.get(key, {})
        items[name] = {
            'key': key,
            'value': value,
            'name': name,
            'stype': stype}
        self.data_store[key] = items
        self._flush(key, name, clear_cache)


    def list(self,key, use_cache=False):
        #TODO: add list cache
        key = format_key(key)
        names = []
        subkeys = []
        path = self._get_path_from_key(key)
        for fname in os.listdir(path):
            if isfile(os.path.join(path, fname)):
                name = os.path.splitext(fname)[0]
                stype = os.path.splitext(fname)[1]
                names.append(name)
            else:
                subkeys.append(fname)
        return names, subkeys

    def list_objects_with_name(self,key,name):
        key = format_key(key)
        names, subkeys = self.list(key)
        objects = []
        for col in subkeys:
            fullkey = key + [col]
            obj = self.get(fullkey,name)
            if obj is not None:
                objects.append((col,obj))
        return objects

    def list_objects(self,key):
        key = format_key(key)

        names, subkeys = self.list(key)
        objects = []
        for name in names:
            obj = self.get(key,name)
            if obj is not None:
                objects.append((name,obj))
        return objects

    def get(self, key, name="data", stype=None, refresh=False):
        key = format_key(key)
        items = self.data_store.get(key, None)
        if not refresh and not self.ignore_cache and items is not None and name in items:
            entry = items.get(name)
            return entry.get('value')
        else:
            data, stype = self._read(key, name, stype)
            if data is None:
                return None
            self.save(key, data, name=name, stype=stype)
            return data

    def delete(self, key, name="data", stype=None):
        #TODO Fix slow deletes
        key = format_key(key)
        self.flush_all()
        items = self.data_store.get(key)
        path = self._get_path_from_key(key)

        #Remove file from disk
        if name is not None:
            items.pop(name,None)
            filepath_prefix = os.path.join(path, "{}".format(name))
            if stype is None:
                stype = get_filetype(filepath_prefix)
                    
                    #raise Exception("Not found")
            if stype is not None:
                filepath = os.path.join(path, "{}.{}".format(name, stype.lower()))
                if os.path.isfile(filepath):
                    os.remove(filepath)
            
        #Remove path
        if items is None or len(items) == 0:
            # Remove Path From memory
            self.data_store.pop(key,None)

            # If path is empty
            if len(os.listdir(path)) == 0:
                try:
                    if not os.path.isfile(path):
                        os.rmdir(path=os.path.join(os.getcwd(),path))
                except Exception as e:
                    print(e)
                if len(key) > 1:
                    self.delete(key[:-1],name=None)

    def _flush(self, key, name='data', clear_cache=False):
        if self.test_mode:
            return 
        if self.use_write_thread:
            self.write_queue.put((key,name,clear_cache))
        else:
            self._flush_sync(key,name,clear_cache)

    def _flush_sync(self, key, name='data', clear_cache=False):
        if self.test_mode:
            return 
        items = self.data_store[key]
        entry = items.get(name)
        entry['dirty'] = False
        value = entry['value']
        if clear_cache:
            entry['value'] = None
        self._write(key, name=name, value=value, stype=entry['stype'])
            

    def _get_path_from_key(self, key):
        path_parts = [str(k) for k in [self.root_path] + list(key)]
        path = os.path.join(*path_parts)
        self.check_path(path)
        return path
        
    def close(self):
        self.running = False
        self.writer_thread.join()

    def flush_all(self):
        if self.test_mode:
            return 
        flushed = False
        while not flushed:
            with self.write_lock:
                if self.write_queue.qsize() == 0:
                    return
                    


    def _write(self, key, value, name='data', stype="json"):
        """
        saves value to file
        TODO: add autohandling of file type
        """
        path = self._get_path_from_key(key)
        filepath = os.path.join(path, "{}.{}".format(name, stype.lower()))
        filepath_tmp = os.path.join(
            path, "{}_tmp.{}".format(name, stype.lower()))
        if stype == "json":
            with open(filepath_tmp, 'w') as f:
                json.dump(value, f, ignore_nan=True, cls=self.json_encoder)
        elif stype == "pkl":
            with open(filepath_tmp, 'wb') as f:
                pickle.dump(value, f)
        elif stype == "png" or stype == "jpg":
            im = PIL.Image.fromarray(value)
            im.save(filepath_tmp)
        else:
            raise Exception("File type not supported: {}".format(stype))
        shutil.copyfile(filepath_tmp, filepath)
        os.remove(filepath_tmp)


    def _read(self, key, name="data", stype=None, default_value=None):
        path = self._get_path_from_key(key)
        filepath_prefix = os.path.join(path, "{}".format(name))
        if stype is None:
            stype = get_filetype(filepath_prefix)
            if stype is None:
                return None, None

        filepath = filepath_prefix + "." + stype.lower()

        if not os.path.isfile(filepath):
            return default_value, stype

        if stype.lower() == "json":
            with open(filepath, 'r') as f:
                value = json.load(f, cls=self.json_decoder)
        elif stype == "pkl":
            with open(filepath, 'rb') as f:
                value = pickle.load(f)
        else:
            raise Exception("Unsupported format {}".format(stype))
        return value, stype
