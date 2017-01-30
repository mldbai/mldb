# This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

def test1():
    print "test1"

def test2():
    test1()

test2()

mldb.script.set_return("success")
