from deadsimpledb import DeadSimpleDB, key_as_str, str_as_key
import numpy

def test1():
    dsdb = DeadSimpleDB("testdb")

    key = ["entry",1]
    dsdb.save(key,value={'value':10})
    dsdb.flush(key)
    # dsdb.flush_all()

    print(dsdb.get(key))


def test2():
    key = ['123',1, "yes"]
    key_s = key_as_str(key)
    key2 = str_as_key(key_s)
    key_s2 = key_as_str(key2)
    assert(key_s == key_s2)

def test3():
    dsdb = DeadSimpleDB("testdb")

    for i in range(100):
        key = ["entry",i]
        dsdb.save(key,name="test3",value={'value':10 * i})
        dsdb.flush(key,name="test3")

def test4():
    dsdb = DeadSimpleDB("testdb")

    for i in range(100):
        key = ["entry",i]
        dsdb.save(key,name="test4",value={'value':10 * i})
        dsdb.flush(key,name="test4", clear_cache=True)

    for i in range(100):
        key = ["entry",i]
        d = dsdb.get(key)
        print(d)

def test5():
    dsdb = DeadSimpleDB("testdb")

    for i in range(100):
        key = ["entry",i]
        dsdb.save(key,name='test5', value=numpy.random.rand(3,3),stype='pkl')
        dsdb.flush(key,name='test5', clear_cache=True)

    for i in range(100):
        key = ["entry",i]
        d = dsdb.get(key)
        print(d)

def test6():
    dsdb = DeadSimpleDB("testdb")

    for i in range(100):
        key = ["entry",i]
        dsdb.append_to_list(key,name="test6",value=numpy.random.rand(3,3),stype='pkl')
    
    dsdb.flush_all()

    for i in range(100):
        key = ["entry",i]
        d = dsdb.get(key,name="test6")
        print(d)

    for i in range(100):
        key = ["entry",i]
        dsdb.delete(key,name="test6")


test1()
test2()
test3()
test4()
test5()
test6()