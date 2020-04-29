import numpy
from deadsimpledb import DeadSimpleDB, format_key
import unittest
import time

class TestMain(unittest.TestCase):

    def test_save_flush_get(self):
        dsdb = DeadSimpleDB("/tmp/testdb")

        key = ("entry", 1)
        value = {'value': 10}
        dsdb.save(key, value=value)
        value2 = dsdb.get(key)
        assert(value['value'] == value2['value'])

    def test_multiple_key(self):

        size = 100000
        dsdb = DeadSimpleDB("/tmp/testdb",
            check_file_last_updated=False,
            use_write_thread=False,
            read_only=True)
        self.multiwrite('mem_only',dsdb,size)

        dsdb = DeadSimpleDB("/tmp/testdb",
            check_file_last_updated=False,
            use_write_thread=True,
            read_only=False)
        self.multiwrite('with_thread',dsdb,size)


        dsdb = DeadSimpleDB("/tmp/testdb",
            check_file_last_updated=False,
            use_write_thread=False,
            read_only=False)
        self.multiwrite('sync',dsdb,size)

        """
        mem_only
Runtime:1.4291698932647705
Full Write time:1.4292702674865723
Using Write Thread
---
with_thread
Runtime:2.2632408142089844
Full Write time:42.657451152801514
Not using write thread
---
sync
Runtime:26.0583233833313
Full Write time:26.05849027633667

        """






    def multiwrite(self,prefix,dsdb,size):

        name = "test_multiple_big"
        start = time.time()

        for i in range(size):
            key = (prefix,"entryxx", i)
            dsdb.save(key, name=name, value={'value': 10 * i})
            assert(dsdb.get(key, name=name) is not None)
        print("---")
        print(prefix)
        print("Runtime:{}".format(time.time() - start))
        dsdb.close()
        print("Full Write time:{}".format(time.time() - start))



    def test_multiple_name(self):
        dsdb = DeadSimpleDB("/tmp/testdb")
        key = ("test_multiple", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
            del_result = dsdb.get(key, name=i)
            print(del_result)
            assert(del_result is not None)

    def test_multiple_name_no_delete(self):
        dsdb = DeadSimpleDB("/tmp/testdb")
        key = ("test_multiple2", 1)

        for i in range(10):
            dsdb.save(key, name=i, value={'value': 10 * i})
        print("Done saving")
        dsdb.flush_all()
        print("Done writing")
        dsdb = DeadSimpleDB("/tmp/testdb")
        items= dsdb.list(key)[0]
        print("count {}".format(len(items)))
        print(items)

        assert(len(items)==10)
        print("done listing")


    def test_append_list_pkl(self):
        dsdb = DeadSimpleDB("/tmp/testdb")

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
        dsdb = DeadSimpleDB("/tmp/testdb")

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
        dsdb = DeadSimpleDB("/tmp/testdb")
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

        dsdb = DeadSimpleDB("/tmp/testdb")
        item_count = len(dsdb.list(key=("hi", 123, "test_multiple5", 1))[0])
        print("item count {}".format(item_count))

        assert(item_count == 10)
