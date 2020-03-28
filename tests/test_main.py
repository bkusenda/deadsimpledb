import numpy
from deadsimpledb import DeadSimpleDB, key_as_str, str_as_key
import unittest


class TestMain(unittest.TestCase):

    def test_save_flush_get(self):
        dsdb = DeadSimpleDB("testdb")

        key = ["entry",1]
        value = {'value':10}
        dsdb.save(key,value=value)
        dsdb.flush(key)
        value2 = dsdb.get(key)
        assert(value['value'] == value2['value'])


    def test_string_functions(self):
        key = ['123',1, "yes"]
        key_s = key_as_str(key)
        key2 = str_as_key(key_s)
        key_s2 = key_as_str(key2)
        assert(key_s == key_s2)

    def test_multiple_key(self):
        dsdb = DeadSimpleDB("testdb")
        name = "test_multiple"
        clear_cache = False

        for i in range(100):
            key = ["entry",i]
            dsdb.save(key,name=name,value={'value':10 * i})
            dsdb.flush(key,name=name,clear_cache=clear_cache)
            dsdb.delete(key,name=name)
            assert(dsdb.get(key,name=name) is None)

    def test_multiple_name(self):
        dsdb = DeadSimpleDB("testdb")
        key = ["test_multiple",1]
        clear_cache = False

        for i in range(100):
            dsdb.save(key,name=i,value={'value':10 * i})
            dsdb.flush(key,name=i,clear_cache=clear_cache)
            dsdb.delete(key,name=i)
            assert(dsdb.get(key,name=i) is None)


    def test_append_list_pkl(self):
        dsdb = DeadSimpleDB("testdb")

        for i in range(100):
            key = ["entry",i]
            dsdb.append_to_list(key,name="test6",value=numpy.random.rand(3,3),stype='pkl')
        
        dsdb.flush_all()

        for i in range(100):
            key = ["entry",i]
            d = dsdb.get(key,name="test6")
            assert(d is not None)

        for i in range(100):
            key = ["entry",i]
            dsdb.delete(key,name="test6")
            assert(d is not None)

