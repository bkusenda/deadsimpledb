import numpy
from deadsimpledb import DeadSimpleDB, format_key
import unittest
import time

class TestMain(unittest.TestCase):

    def test_save_flush_get(self):
        dsdb = DeadSimpleDB("testdb")

        key = ("entry", 1)
        value = {'value': 10}
        dsdb.save(key, value=value)
        value2 = dsdb.get(key)

        dsdb.delete(key)
        dsdb.close()
        assert(value['value'] == value2['value'])

    def test_multiple_key(self):
        dsdb = DeadSimpleDB("testdb")
        name = "test_multiple"
        clear_cache = False

        for i in range(3):
            print("-------------------")
            key = ("entryx", i)
            dsdb.save(key, name=name, value={'value': 10 * i})
            dsdb.delete(key, name=name)
            assert(dsdb.get(key, name=name) is None)
        dsdb.close()

    def test_multiple_name(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("test_multiple", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
            dsdb.delete(key, name=i)
            assert(dsdb.get(key, name=i) is None)
        dsdb.close()

    def test_multiple_name_no_delete(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("test_multiple", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
        print("Done saving")
        dsdb.close()
        print("Done writing")
        dsdb = DeadSimpleDB("testdb")
        assert(len(dsdb.list(key)[0])==10)
        print("done listing")


    def test_append_list_pkl(self):
        dsdb = DeadSimpleDB("testdb")

        for i in range(10):
            key = ("entry2", i)
            dsdb.append_to_list(key, name="test6",
                                value=numpy.random.rand(3, 3), stype='pkl')

        dsdb.flush_all()

        for i in range(10):
            key = ("entry2", i)
            d = dsdb.get(key, name="test6")
            assert(d is not None)

        for i in range(10):
            key = ("entry2", i)
            dsdb.delete(key, name="test6")
            assert(d is not None)
        dsdb.close()

    def test_index_1(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("hi", 123, "test_multiple", 1)
        # clear_cache = False

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})

        key = ("hi", 1234, "test_multiple", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})

        print(dsdb.list(key))
        for i in range(0, len(key)):
            print(dsdb.list(key[:-(i+1)]))
        dsdb.flush_all()

        dsdb = DeadSimpleDB("testdb")
        item_count = len(dsdb.list(key=("hi", 123, "test_multiple", 1))[0])
        print("item count {}".format(item_count))
        dsdb.close()

        assert(item_count == 10)
