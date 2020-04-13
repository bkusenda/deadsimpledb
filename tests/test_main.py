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
        assert(value['value'] == value2['value'])

    def test_multiple_key(self):
        dsdb = DeadSimpleDB("testdb")
        name = "test_multiple"
        clear_cache = False

        for i in range(3):
            print("-------------------")
            key = ("entryx", i)
            dsdb.save(key, name=name, value={'value': 10 * i})
            assert(dsdb.get(key, name=name) is not None)

    def test_multiple_name(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("test_multiple", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
            del_result = dsdb.get(key, name=i)
            print(del_result)
            assert(del_result is not None)

    def test_multiple_name_no_delete(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("test_multiple2", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
        print("Done saving")
        dsdb.flush_all()
        print("Done writing")
        dsdb = DeadSimpleDB("testdb")
        items= dsdb.list(key)[0]
        print("count {}".format(len(items)))
        print(items)

        assert(len(items)==10)
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


    def test_append_list_csv(self):
        dsdb = DeadSimpleDB("testdb")

        for i in range(10):
            key = ("entrycsv", i)
            dsdb.append_to_list(key, name="test6",
                                value=['abc',1,2,3], stype='csv')

        dsdb.flush_all()

        for i in range(10):
            key = ("entrycsv", i)
            d = dsdb.get(key, name="test6")
            assert(d is not None)

    def test_index_1(self):
        dsdb = DeadSimpleDB("testdb")
        key = ("hi", 123, "test_multiple5", 1)
        # clear_cache = False

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})

        key = ("hi", 1234, "test_multiple5", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})

        print(dsdb.list(key))
        for i in range(0, len(key)):
            print(dsdb.list(key[:-(i+1)]))
        dsdb.flush_all()

        dsdb = DeadSimpleDB("testdb")
        item_count = len(dsdb.list(key=("hi", 123, "test_multiple5", 1))[0])
        print("item count {}".format(item_count))

        assert(item_count == 10)
