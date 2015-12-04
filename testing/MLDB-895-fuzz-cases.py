# This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.



#problem was a modulus by 0
result = mldb.perform('GET', '/v1/query', [['q', 'select CAST (61971-71%0 AS timestamp)']])
mldb.log(result)
result = mldb.perform('GET', '/v1/query', [['q', 'select CAST (61971-7%-0 AS timestamp)']])
mldb.log(result)
result = mldb.perform('GET', '/v1/query', [['q', 'select 1 % 2IN (4)']])
mldb.log(result)
result = mldb.perform('GET', '/v1/query', [['q', 'select 01--1%0e83^ 2 + (9)']])
mldb.log(result)
result = mldb.perform('GET', '/v1/query', [['q', 'select 2IN (-1%1-11,1IN (21%1,11),1%1%1IN (31%1,11),1%1<1%1,11)']])
mldb.log(result)
assert result['statusCode'] >= 400

#problem with unary bitwise operator print()
result = mldb.perform('GET', '/v1/query', [['q', 'select {~9as*}']])
mldb.log(result)

mldb.script.set_return("success")