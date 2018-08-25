# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

def test1():
    print("test1")

def test2():
    test1()

test2()

request.set_return("success")
